// Multithreaded batch address activity checker for GoChain.
//
// Reads addresses (one per line) from --in, checks each one for ANY on-chain
// activity via a public GoChain JSON-RPC node and appends active addresses
// to --out (default: result.txt).
//
// Activity checks, in order (short-circuit on first positive signal):
//  1. eth_getTransactionCount(addr, "latest") > 0
//     -> address has SENT at least one tx (native, token, contract call, ...).
//  2. eth_getBalance(addr, "latest") > 0
//     -> address has a non-zero native balance (received native funds).
//  3. eth_getLogs for Transfer events where `to` == addr (optional, --logs).
//     Covers ERC-20, ERC-721 and ERC-1155 incoming transfers, so even a
//     "receive-only" wallet that has since been drained is detected.
//
// If --logs=true and the node rejects a wide block range, we fall back to
// chunked scanning of [0, latest] in --chunk sized windows.
//
// Features:
//   - Worker pool with configurable concurrency.
//   - Global rate limiter (token bucket) to respect node quota.
//   - Retries with exponential backoff + jitter on transient errors.
//   - Short-circuit at every level.
//   - Streaming writer with mutex, so result.txt is always consistent.
//   - Graceful shutdown on Ctrl+C.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type Config struct {
	RPCURL     string
	InputFile  string
	OutputFile string
	Workers    int
	RPS        int
	MaxRetries int
	Timeout    time.Duration
	CheckLogs  bool
	ChunkSize  uint64
}

func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.RPCURL, "rpc", "https://rpc.gochain.io",
		"GoChain JSON-RPC endpoint")
	flag.StringVar(&cfg.InputFile, "in", "addresses.txt",
		"input file: one address per line")
	flag.StringVar(&cfg.OutputFile, "out", "result.txt",
		"output file: addresses with at least one tx will be appended here")
	flag.IntVar(&cfg.Workers, "workers", 20,
		"number of concurrent workers")
	flag.IntVar(&cfg.RPS, "rps", 20,
		"global RPC requests per second cap")
	flag.IntVar(&cfg.MaxRetries, "retries", 5,
		"max retries per request on transient errors")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Second,
		"per-request HTTP timeout")
	flag.BoolVar(&cfg.CheckLogs, "logs", true,
		"also scan eth_getLogs for incoming ERC-20/721/1155 transfers (catches receive-only wallets)")
	flag.Uint64Var(&cfg.ChunkSize, "chunk", 0,
		"block range per eth_getLogs call (0 = try full range first, chunk only on error)")
	flag.Parse()

	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.RPS <= 0 {
		cfg.RPS = 1
	}
	return cfg
}

// ----------------------------------------------------------------------------
// Rate limiter (token bucket, stdlib only)
// ----------------------------------------------------------------------------

type RateLimiter struct {
	tokens chan struct{}
	stop   chan struct{}
}

func NewRateLimiter(rps int) *RateLimiter {
	rl := &RateLimiter{
		tokens: make(chan struct{}, rps),
		stop:   make(chan struct{}),
	}
	for i := 0; i < rps; i++ {
		rl.tokens <- struct{}{}
	}
	interval := time.Second / time.Duration(rps)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-rl.stop:
				return
			case <-t.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	return rl
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rl *RateLimiter) Close() { close(rl.stop) }

// ----------------------------------------------------------------------------
// JSON-RPC client
// ----------------------------------------------------------------------------

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error,omitempty"`
}

type transientError struct{ err error }

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

// rangeTooWideError means the node rejected a logs query because the block
// range / response is too large. We DO NOT retry; we chunk instead.
type rangeTooWideError struct{ err error }

func (e *rangeTooWideError) Error() string { return e.err.Error() }
func (e *rangeTooWideError) Unwrap() error { return e.err }

type Client struct {
	http    *http.Client
	cfg     *Config
	limiter *RateLimiter

	latestOnce sync.Once
	latest     uint64
	latestErr  error
}

func NewClient(cfg *Config, rl *RateLimiter) *Client {
	return &Client{
		http: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		cfg:     cfg,
		limiter: rl,
	}
}

// callOnce performs one JSON-RPC call and decodes the result into out.
func (c *Client) callOnce(ctx context.Context, method string, params []interface{}, out interface{}) error {
	if err := c.limiter.Wait(ctx); err != nil {
		return err
	}

	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.RPCURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return &transientError{err: fmt.Errorf("http: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return &transientError{err: fmt.Errorf("read body: %w", err)}
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return &transientError{
			err: fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(raw), 200)),
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}

	var r rpcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("decode json: %w (body=%s)", err, truncate(string(raw), 200))
	}
	if r.Error != nil {
		msg := strings.ToLower(r.Error.Message)
		switch {
		// "range too wide", "too many results", "query returned more than ..."
		// -- we should chunk, not retry.
		case strings.Contains(msg, "range") ||
			strings.Contains(msg, "too many") ||
			strings.Contains(msg, "exceed") ||
			strings.Contains(msg, "maximum") ||
			strings.Contains(msg, "response size") ||
			strings.Contains(msg, "returned more than"):
			return &rangeTooWideError{err: r.Error}
		// generic "retryable" signals
		case r.Error.Code == -32005 ||
			strings.Contains(msg, "rate") ||
			strings.Contains(msg, "limit") ||
			strings.Contains(msg, "busy") ||
			strings.Contains(msg, "timeout"):
			return &transientError{err: r.Error}
		default:
			return r.Error
		}
	}
	if out != nil {
		if err := json.Unmarshal(r.Result, out); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}
	return nil
}

// call wraps callOnce with retries + exponential backoff + jitter.
// rangeTooWideError is NOT retried -- it is returned to the caller so it can chunk.
func (c *Client) call(ctx context.Context, method string, params []interface{}, out interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		err := c.callOnce(ctx, method, params, out)
		if err == nil {
			return nil
		}
		lastErr = err

		var rte *rangeTooWideError
		if errors.As(err, &rte) {
			return err
		}
		var te *transientError
		if !errors.As(err, &te) {
			return err
		}
		if attempt == c.cfg.MaxRetries {
			break
		}
		base := 300 * time.Millisecond << attempt
		sleep := base + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
	return fmt.Errorf("after %d retries: %w", c.cfg.MaxRetries, lastErr)
}

// ----------------------------------------------------------------------------
// RPC helpers
// ----------------------------------------------------------------------------

func (c *Client) TxCount(ctx context.Context, addr string) (uint64, error) {
	var hex string
	if err := c.call(ctx, "eth_getTransactionCount",
		[]interface{}{addr, "latest"}, &hex); err != nil {
		return 0, err
	}
	return parseHexUint64(hex)
}

func (c *Client) Balance(ctx context.Context, addr string) (*big.Int, error) {
	var hex string
	if err := c.call(ctx, "eth_getBalance",
		[]interface{}{addr, "latest"}, &hex); err != nil {
		return nil, err
	}
	return parseHexBig(hex)
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var hex string
	if err := c.call(ctx, "eth_blockNumber", []interface{}{}, &hex); err != nil {
		return 0, err
	}
	return parseHexUint64(hex)
}

// LatestBlock caches eth_blockNumber across the whole run so we don't hit the
// node for every address.
func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	c.latestOnce.Do(func() {
		c.latest, c.latestErr = c.BlockNumber(ctx)
	})
	return c.latest, c.latestErr
}

// ----------------------------------------------------------------------------
// Logs-based activity check
// ----------------------------------------------------------------------------

const (
	// keccak256("Transfer(address,address,uint256)")
	// Same topic0 for ERC-20 and ERC-721; `to` is topic index 2.
	topicERC20721Transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	// keccak256("TransferSingle(address,address,address,uint256,uint256)")
	// ERC-1155; `to` is topic index 3.
	topicERC1155Single = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
	// keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
	// ERC-1155; `to` is topic index 3.
	topicERC1155Batch = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

// padAddress left-pads a 20-byte address to a 32-byte hex topic value.
func padAddress(addr string) string {
	clean := strings.ToLower(strings.TrimPrefix(addr, "0x"))
	return "0x" + strings.Repeat("0", 64-len(clean)) + clean
}

// buildTopics returns a topics filter where `to` is constrained to addr at
// the given position, and all preceding positions are unconstrained (null).
// topic0 can be a single hash or an OR-array of hashes.
func buildTopics(topic0 interface{}, toPos int, addr string) []interface{} {
	t := make([]interface{}, toPos+1)
	t[0] = topic0
	for i := 1; i < toPos; i++ {
		t[i] = nil
	}
	t[toPos] = padAddress(addr)
	return t
}

// getLogsRange calls eth_getLogs for [fromBlock, toBlock] and returns true if
// at least one log matched.
func (c *Client) getLogsRange(ctx context.Context, topics []interface{}, fromBlock, toBlock string) (bool, error) {
	filter := map[string]interface{}{
		"fromBlock": fromBlock,
		"toBlock":   toBlock,
		"topics":    topics,
	}
	var logs []json.RawMessage
	if err := c.call(ctx, "eth_getLogs", []interface{}{filter}, &logs); err != nil {
		return false, err
	}
	return len(logs) > 0, nil
}

// hasIncomingByTopics scans [0, latest] for Transfer-like events targeting addr.
// If --chunk==0 we first try the full range and only chunk on a range-too-wide
// error; otherwise we go straight to fixed-size chunks.
func (c *Client) hasIncomingByTopics(ctx context.Context, topics []interface{}) (bool, error) {
	latest, err := c.LatestBlock(ctx)
	if err != nil {
		return false, fmt.Errorf("eth_blockNumber: %w", err)
	}

	chunk := c.cfg.ChunkSize
	if chunk == 0 {
		has, err := c.getLogsRange(ctx, topics, "0x0", "latest")
		if err == nil {
			return has, nil
		}
		var rte *rangeTooWideError
		if !errors.As(err, &rte) {
			return false, err
		}
		// Fall back to a safe default chunk.
		chunk = 200_000
	}

	for start := uint64(0); start <= latest; start += chunk {
		end := start + chunk - 1
		if end > latest {
			end = latest
		}
		has, err := c.getLogsRange(ctx, topics,
			"0x"+strconv.FormatUint(start, 16),
			"0x"+strconv.FormatUint(end, 16))
		if err != nil {
			// If THIS chunk is still too wide, halve it on the fly.
			var rte *rangeTooWideError
			if errors.As(err, &rte) && chunk > 1 {
				chunk /= 2
				// Re-run this same window with the smaller chunk.
				for s := start; s <= end; s += chunk {
					e := s + chunk - 1
					if e > end {
						e = end
					}
					h, err := c.getLogsRange(ctx, topics,
						"0x"+strconv.FormatUint(s, 16),
						"0x"+strconv.FormatUint(e, 16))
					if err != nil {
						return false, err
					}
					if h {
						return true, nil
					}
				}
				continue
			}
			return false, err
		}
		if has {
			return true, nil
		}
	}
	return false, nil
}

// hasIncomingTransfers checks ERC-20/721 then ERC-1155 with short-circuit.
func (c *Client) hasIncomingTransfers(ctx context.Context, addr string) (bool, error) {
	// ERC-20 / ERC-721: single topic0, `to` at position 2.
	t1 := buildTopics(topicERC20721Transfer, 2, addr)
	has, err := c.hasIncomingByTopics(ctx, t1)
	if err != nil {
		return false, fmt.Errorf("logs erc20/721: %w", err)
	}
	if has {
		return true, nil
	}
	// ERC-1155: OR of two topic0s, `to` at position 3.
	t2 := buildTopics([]string{topicERC1155Single, topicERC1155Batch}, 3, addr)
	has, err = c.hasIncomingByTopics(ctx, t2)
	if err != nil {
		return false, fmt.Errorf("logs erc1155: %w", err)
	}
	return has, nil
}

// ----------------------------------------------------------------------------
// IsActive
// ----------------------------------------------------------------------------

func (c *Client) IsActive(ctx context.Context, addr string) (bool, error) {
	nonce, err := c.TxCount(ctx, addr)
	if err != nil {
		return false, fmt.Errorf("eth_getTransactionCount: %w", err)
	}
	if nonce > 0 {
		return true, nil
	}
	bal, err := c.Balance(ctx, addr)
	if err != nil {
		return false, fmt.Errorf("eth_getBalance: %w", err)
	}
	if bal.Sign() > 0 {
		return true, nil
	}
	if !c.cfg.CheckLogs {
		return false, nil
	}
	return c.hasIncomingTransfers(ctx, addr)
}

// ----------------------------------------------------------------------------
// Hex helpers
// ----------------------------------------------------------------------------

func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return 0, nil
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return 0, fmt.Errorf("bad hex uint: %q", s)
	}
	if !n.IsUint64() {
		return 0, fmt.Errorf("hex uint overflow: %q", s)
	}
	return n.Uint64(), nil
}

func parseHexBig(s string) (*big.Int, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return big.NewInt(0), nil
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return nil, fmt.Errorf("bad hex big: %q", s)
	}
	return n, nil
}

// ----------------------------------------------------------------------------
// Pipeline
// ----------------------------------------------------------------------------

type job struct{ addr string }
type result struct {
	addr   string
	active bool
	err    error
}

type safeWriter struct {
	mu sync.Mutex
	f  *os.File
	bw *bufio.Writer
}

func newSafeWriter(path string) (*safeWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &safeWriter{f: f, bw: bufio.NewWriter(f)}, nil
}

func (w *safeWriter) Write(addr string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.bw.WriteString(addr + "\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *safeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

func readAddresses(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var addrs []string
	seen := make(map[string]struct{})
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !isEVMAddress(line) {
			log.Printf("[warn] skip invalid address: %q", line)
			continue
		}
		low := strings.ToLower(line)
		if _, ok := seen[low]; ok {
			continue
		}
		seen[low] = struct{}{}
		addrs = append(addrs, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return addrs, nil
}

func isEVMAddress(s string) bool {
	if len(s) != 42 || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, c := range s[2:] {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	cfg := parseFlags()

	addrs, err := readAddresses(cfg.InputFile)
	if err != nil {
		log.Fatalf("read addresses: %v", err)
	}
	if len(addrs) == 0 {
		log.Fatalf("no valid addresses in %s", cfg.InputFile)
	}
	log.Printf("loaded %d unique addresses from %s", len(addrs), cfg.InputFile)
	log.Printf("rpc=%s workers=%d rps=%d retries=%d logs=%t chunk=%d",
		cfg.RPCURL, cfg.Workers, cfg.RPS, cfg.MaxRetries, cfg.CheckLogs, cfg.ChunkSize)

	writer, err := newSafeWriter(cfg.OutputFile)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer writer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	limiter := NewRateLimiter(cfg.RPS)
	defer limiter.Close()

	client := NewClient(cfg, limiter)

	// Warm up latest block once; makes the first logs query predictable.
	if cfg.CheckLogs {
		if n, err := client.LatestBlock(ctx); err != nil {
			log.Fatalf("eth_blockNumber: %v", err)
		} else {
			log.Printf("latest block = %d", n)
		}
	}

	jobs := make(chan job, cfg.Workers*2)
	results := make(chan result, cfg.Workers*2)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				active, err := client.IsActive(ctx, j.addr)
				results <- result{addr: j.addr, active: active, err: err}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, a := range addrs {
			select {
			case <-ctx.Done():
				return
			case jobs <- job{addr: a}:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		total     = len(addrs)
		done      int64
		activeN   int64
		failedN   int64
		startTime = time.Now()
	)
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

loop:
	for {
		select {
		case <-progress.C:
			d := atomic.LoadInt64(&done)
			a := atomic.LoadInt64(&activeN)
			f := atomic.LoadInt64(&failedN)
			log.Printf("progress: %d/%d  active=%d  failed=%d  elapsed=%s",
				d, total, a, f, time.Since(startTime).Round(time.Second))
		case r, ok := <-results:
			if !ok {
				break loop
			}
			atomic.AddInt64(&done, 1)
			switch {
			case r.err != nil:
				atomic.AddInt64(&failedN, 1)
				log.Printf("[err ] %s: %v", r.addr, r.err)
			case r.active:
				atomic.AddInt64(&activeN, 1)
				if werr := writer.Write(r.addr); werr != nil {
					log.Printf("[err ] write %s: %v", r.addr, werr)
				} else {
					log.Printf("[ ok ] %s  active", r.addr)
				}
			default:
				log.Printf("[skip] %s  no activity", r.addr)
			}
		}
	}

	log.Printf("done: total=%d active=%d failed=%d elapsed=%s  -> %s",
		total,
		atomic.LoadInt64(&activeN),
		atomic.LoadInt64(&failedN),
		time.Since(startTime).Round(time.Second),
		cfg.OutputFile,
	)
}

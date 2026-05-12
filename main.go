// Multithreaded batch address activity checker for GoChain.
//
// Reads addresses (one per line) from --in, checks each one for ANY on-chain
// activity via a public GoChain JSON-RPC node and appends active addresses
// to --out (default: result.txt).
//
// "Activity" for a public JSON-RPC node (no indexer) is defined as:
//   - eth_getTransactionCount(addr, "latest") > 0  -> the address has sent
//     at least one tx (native transfer, token transfer, contract call, ...),
//     OR
//   - eth_getBalance(addr, "latest") > 0           -> the address has received
//     native funds at least once.
//
// If either holds, the address is considered active and written to result.txt.
// Short-circuit: we stop checking an address as soon as activity is found.
//
// Features:
//   - Worker pool with configurable concurrency.
//   - Global rate limiter (token bucket) to respect node quota.
//   - Retries with exponential backoff + jitter on transient errors.
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
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second,
		"per-request HTTP timeout")
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

// transientError signals that a request should be retried.
type transientError struct{ err error }

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

type Client struct {
	http    *http.Client
	cfg     *Config
	limiter *RateLimiter
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

// callOnce performs one JSON-RPC call and decodes the result into out (if non-nil).
// Retryable failures are returned wrapped in *transientError.
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
		return err // non-retryable
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

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
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
		// -32005 and friends are "limit exceeded" -> retryable.
		if r.Error.Code == -32005 ||
			strings.Contains(strings.ToLower(r.Error.Message), "rate") ||
			strings.Contains(strings.ToLower(r.Error.Message), "limit") ||
			strings.Contains(strings.ToLower(r.Error.Message), "busy") {
			return &transientError{err: r.Error}
		}
		return r.Error
	}
	if out != nil {
		if err := json.Unmarshal(r.Result, out); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}
	return nil
}

// call wraps callOnce with retries + exponential backoff + jitter.
func (c *Client) call(ctx context.Context, method string, params []interface{}, out interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		err := c.callOnce(ctx, method, params, out)
		if err == nil {
			return nil
		}
		lastErr = err

		var te *transientError
		if !errors.As(err, &te) {
			return err
		}
		if attempt == c.cfg.MaxRetries {
			break
		}
		// 300ms, 600ms, 1.2s, 2.4s, ... + up to 250ms jitter
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

// TxCount returns eth_getTransactionCount(addr, "latest") as uint64.
func (c *Client) TxCount(ctx context.Context, addr string) (uint64, error) {
	var hex string
	if err := c.call(ctx, "eth_getTransactionCount",
		[]interface{}{addr, "latest"}, &hex); err != nil {
		return 0, err
	}
	return parseHexUint64(hex)
}

// Balance returns eth_getBalance(addr, "latest") as *big.Int.
func (c *Client) Balance(ctx context.Context, addr string) (*big.Int, error) {
	var hex string
	if err := c.call(ctx, "eth_getBalance",
		[]interface{}{addr, "latest"}, &hex); err != nil {
		return nil, err
	}
	return parseHexBig(hex)
}

// IsActive returns true if the address sent at least one tx OR has a non-zero balance.
// Short-circuits on the first positive signal.
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
	return bal.Sign() > 0, nil
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
	log.Printf("rpc=%s workers=%d rps=%d retries=%d",
		cfg.RPCURL, cfg.Workers, cfg.RPS, cfg.MaxRetries)

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

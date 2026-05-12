// Multithreaded batch EVM address activity checker.
//
// Reads addresses (one per line) from --in, checks each one for ANY on-chain
// activity (native tx / ERC-20 transfer / ERC-721 transfer) via Etherscan V2
// unified API and appends active addresses to --out (default: result.txt).
//
// Features:
//   - Worker pool with configurable concurrency.
//   - Global rate limiter (token bucket) to respect API quota.
//   - Retries with exponential backoff + jitter on transient errors.
//   - Short-circuit: stops checking an address as soon as activity is found.
//   - Streaming writer with mutex, so result.txt is always consistent.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
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
	APIKey     string
	ChainID    int
	BaseURL    string
	InputFile  string
	OutputFile string
	Workers    int
	RPS        int
	MaxRetries int
	Timeout    time.Duration
}

func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.APIKey, "key", os.Getenv("ETHERSCAN_API_KEY"),
		"Etherscan V2 API key (or env ETHERSCAN_API_KEY)")
	flag.IntVar(&cfg.ChainID, "chain", 1,
		"EVM chain id (1=ETH, 56=BSC, 137=Polygon, 42161=Arbitrum, 10=Optimism, 8453=Base, ...)")
	flag.StringVar(&cfg.BaseURL, "url", "https://api.etherscan.io/v2/api",
		"Etherscan V2 unified API endpoint")
	flag.StringVar(&cfg.InputFile, "in", "addresses.txt",
		"input file: one address per line")
	flag.StringVar(&cfg.OutputFile, "out", "result.txt",
		"output file: addresses with at least one tx will be appended here")
	flag.IntVar(&cfg.Workers, "workers", 10,
		"number of concurrent workers")
	flag.IntVar(&cfg.RPS, "rps", 5,
		"global API requests per second (free Etherscan tier = 5)")
	flag.IntVar(&cfg.MaxRetries, "retries", 5,
		"max retries per request on transient errors")
	flag.DurationVar(&cfg.Timeout, "timeout", 20*time.Second,
		"per-request HTTP timeout")
	flag.Parse()

	if cfg.APIKey == "" {
		log.Fatal("api key is required: pass --key or set ETHERSCAN_API_KEY")
	}
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
	// prefill the bucket
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
				default: // bucket full, drop
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
// Etherscan client
// ----------------------------------------------------------------------------

type etherscanResp struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

// transient signals that a request should be retried.
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

// callOnce performs a single API call. Returns parsed response or an error.
// Errors wrapped in *transientError should be retried by the caller.
func (c *Client) callOnce(ctx context.Context, addr, action string) (*etherscanResp, error) {
	q := url.Values{}
	q.Set("chainid", fmt.Sprintf("%d", c.cfg.ChainID))
	q.Set("module", "account")
	q.Set("action", action)
	q.Set("address", addr)
	q.Set("startblock", "0")
	q.Set("endblock", "99999999")
	q.Set("page", "1")
	q.Set("offset", "1") // we only need to know if >=1 tx exists
	q.Set("sort", "asc")
	q.Set("apikey", c.cfg.APIKey)

	full := c.cfg.BaseURL + "?" + q.Encode()

	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err // non-retryable
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &transientError{err: fmt.Errorf("http: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, &transientError{err: fmt.Errorf("read body: %w", err)}
	}

	// 5xx and 429 are classic retryables.
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, &transientError{
			err: fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 200)),
		}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var r etherscanResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("decode json: %w (body=%s)", err, truncate(string(body), 200))
	}

	// Etherscan rate-limit messages come as HTTP 200 with status="0".
	msg := strings.ToLower(r.Message)
	if r.Status == "0" && (strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "max rate") ||
		strings.Contains(msg, "max calls")) {
		return nil, &transientError{err: fmt.Errorf("api rate limit: %s", r.Message)}
	}
	return &r, nil
}

// call wraps callOnce with retries and exponential backoff + jitter.
func (c *Client) call(ctx context.Context, addr, action string) (*etherscanResp, error) {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		r, err := c.callOnce(ctx, addr, action)
		if err == nil {
			return r, nil
		}
		lastErr = err

		var te *transientError
		if !errors.As(err, &te) {
			return nil, err
		}
		if attempt == c.cfg.MaxRetries {
			break
		}
		// exp backoff: 300ms, 600ms, 1.2s, 2.4s, ... + up to 250ms jitter
		base := 300 * time.Millisecond << attempt
		sleep := base + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
	}
	return nil, fmt.Errorf("after %d retries: %w", c.cfg.MaxRetries, lastErr)
}

// hasActivity returns true if the address has at least one tx of the given kind.
// Etherscan returns status="1" + non-empty array when there are results,
// and status="0" with message "No transactions found" otherwise.
func (c *Client) hasActivity(ctx context.Context, addr, action string) (bool, error) {
	r, err := c.call(ctx, addr, action)
	if err != nil {
		return false, err
	}
	if r.Status == "1" {
		// Result is an array; empty array would be status "0", but be defensive.
		s := strings.TrimSpace(string(r.Result))
		return s != "" && s != "[]" && s != "null", nil
	}
	// status "0" with "No transactions found" is a legitimate negative answer.
	if strings.Contains(strings.ToLower(r.Message), "no transactions") {
		return false, nil
	}
	// Any other non-ok message is an error we should surface.
	return false, fmt.Errorf("api error (action=%s): status=%s msg=%s",
		action, r.Status, r.Message)
}

// IsActive checks native / ERC-20 / ERC-721 activity with short-circuit.
func (c *Client) IsActive(ctx context.Context, addr string) (bool, error) {
	for _, action := range []string{"txlist", "tokentx", "tokennfttx"} {
		active, err := c.hasActivity(ctx, addr, action)
		if err != nil {
			return false, fmt.Errorf("%s: %w", action, err)
		}
		if active {
			return true, nil
		}
	}
	return false, nil
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

// safeWriter serializes writes to result.txt.
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
	return w.bw.Flush() // flush immediately so nothing is lost on crash
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
		// Basic EVM address validation.
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

	writer, err := newSafeWriter(cfg.OutputFile)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer writer.Close()

	// Cancel on Ctrl+C so in-flight requests unwind gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	limiter := NewRateLimiter(cfg.RPS)
	defer limiter.Close()

	client := NewClient(cfg, limiter)

	jobs := make(chan job, cfg.Workers*2)
	results := make(chan result, cfg.Workers*2)

	// Workers.
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				active, err := client.IsActive(ctx, j.addr)
				results <- result{addr: j.addr, active: active, err: err}
			}
		}(i)
	}

	// Feeder.
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

	// Closer for results.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collector + progress.
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

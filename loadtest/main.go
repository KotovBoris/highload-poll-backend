// Command loadtest — нагрузочный тест против поднятого стека (docker-compose).
//
// Сценарий:
//  1. Создать опрос через админку (results) с заданной длительностью.
//  2. Прогреть GET /polls/{id} (получить cookie).
//  3. Нагрузить POST /polls/{id}/vote с заданным числом воркеров и/или
//     целевым RPS в течение окна голосования.
//  4. Дождаться завершения опроса (ends_at + grace), опросить результаты.
//
// Метрики: RPS (факт), latency p50/p90/p99/max, коды ответов, число голосов.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	baseURL     string
	adminURL    string
	concurrency int
	duration    time.Duration
	targetRPS   int // 0 = без ограничения (максимальный темп)
	optionCount int
	uniqueIPs   int // диапазон фейковых X-Forwarded-For
	waitResults bool
	graceWait   time.Duration
	// closeDelay — насколько позже окончания нагрузки закрывается опрос.
	closeDelay time.Duration
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.baseURL, "base-url", "http://localhost:8080", "base URL API-воркера")
	flag.StringVar(&cfg.adminURL, "admin-url", "http://localhost:8081", "base URL сервиса результатов")
	flag.IntVar(&cfg.concurrency, "c", 200, "число параллельных воркеров")
	flag.DurationVar(&cfg.duration, "d", 10*time.Second, "длительность нагрузки")
	flag.IntVar(&cfg.targetRPS, "rps", 0, "целевой RPS (0 = без ограничения)")
	flag.IntVar(&cfg.optionCount, "options", 4, "число вариантов ответа в опросе")
	flag.IntVar(&cfg.uniqueIPs, "unique-ips", 100000, "диапазон уникальных X-Forwarded-For")
	flag.BoolVar(&cfg.waitResults, "wait-results", true, "дождаться результатов после опроса")
	flag.DurationVar(&cfg.graceWait, "grace", 60*time.Second, "доп. ожидание завершения опроса")
	flag.DurationVar(&cfg.closeDelay, "close-delay", 3*time.Second, "через сколько после нагрузки закрывается опрос")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest failed:", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	httpc := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.concurrency * 2,
			MaxIdleConnsPerHost: cfg.concurrency * 2,
			MaxConnsPerHost:     cfg.concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	// 1. Создаём опрос: заканчивается чуть позже окончания нагрузки, чтобы
	// после прогона можно было дождаться закрытия и flush результатов.
	pollDuration := int(cfg.duration.Seconds()) + int(cfg.closeDelay.Seconds())
	pollID, endsAt, err := createPoll(httpc, cfg.adminURL, cfg.optionCount, pollDuration)
	if err != nil {
		return fmt.Errorf("create poll: %w", err)
	}
	fmt.Printf("created poll id=%s ends_at=%s duration=%ds\n", pollID, endsAt.Format(time.RFC3339), pollDuration)

	// 2. Прогрев: получаем cookie.
	_, _, err = warmup(httpc, cfg.baseURL, pollID)
	if err != nil {
		return fmt.Errorf("warmup: %w", err)
	}

	// 3. Нагрузка.
	stats := runLoad(ctx, httpc, cfg, pollID)

	fmt.Println("\n===== НАГРУЗКА =====")
	printStats(stats, cfg)

	// 4. Ждём результаты.
	if cfg.waitResults {
		fmt.Println("\n===== ОЖИДАНИЕ РЕЗУЛЬТАТОВ =====")
		if err := waitForResults(ctx, httpc, cfg, pollID); err != nil {
			fmt.Println("wait results:", err)
		}
	}
	return nil
}

func createPoll(c *http.Client, adminURL string, options, durationSec int) (string, time.Time, error) {
	opts := make([]string, options)
	for i := range opts {
		opts[i] = fmt.Sprintf("Option %d", i+1)
	}
	body, _ := json.Marshal(map[string]any{
		"question":         "Load test question?",
		"options":          opts,
		"duration_seconds": durationSec,
	})
	resp, err := c.Post(adminURL+"/admin/polls", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var poll struct {
		ID     string    `json:"id"`
		EndsAt time.Time `json:"ends_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&poll); err != nil {
		return "", time.Time{}, err
	}
	return poll.ID, poll.EndsAt, nil
}

func warmup(c *http.Client, baseURL, pollID string) (*http.Cookie, int, error) {
	resp, err := c.Get(baseURL + "/polls/" + pollID)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	var cookie *http.Cookie
	for _, ck := range resp.Cookies() {
		if ck.Name == "voter_id" {
			cookie = ck
		}
	}
	return cookie, resp.StatusCode, nil
}

type stats struct {
	total    atomic.Int64
	ok       atomic.Int64
	accepted atomic.Int64
	badReq   atomic.Int64
	closed   atomic.Int64
	errs     atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration
}

func (s *stats) record(d time.Duration, status int) {
	s.total.Add(1)
	switch status {
	case http.StatusAccepted:
		s.accepted.Add(1)
		s.ok.Add(1)
	case http.StatusForbidden:
		s.closed.Add(1)
	case http.StatusBadRequest:
		s.badReq.Add(1)
	case 0:
		s.errs.Add(1)
	default:
		s.errs.Add(1)
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func runLoad(ctx context.Context, c *http.Client, cfg config, pollID string) *stats {
	st := &stats{latencies: make([]time.Duration, 0, 1<<20)}

	voteURL := cfg.baseURL + "/polls/" + pollID + "/vote"
	deadline := time.Now().Add(cfg.duration)

	// Ограничитель RPS (если задан).
	var ticker *time.Ticker
	var paceCh <-chan time.Time
	if cfg.targetRPS > 0 {
		interval := time.Second / time.Duration(cfg.targetRPS)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		paceCh = ticker.C
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			for {
				if time.Now().After(deadline) {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				if paceCh != nil {
					select {
					case <-paceCh:
					case <-ctx.Done():
						return
					}
				}
				fp := rng.Intn(cfg.uniqueIPs)
				optID := rng.Intn(cfg.optionCount) + 1
				start := time.Now()
				status := doVote(c, voteURL, fp, optID)
				st.record(time.Since(start), status)
			}
		}(i)
	}

	// Прогресс.
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		start := time.Now()
		var last int64
		for {
			select {
			case <-t.C:
				cur := st.total.Load()
				fmt.Printf("\r  elapsed=%4.0fs total=%d rps=%.0f acc=%d err=%d",
					time.Since(start).Seconds(), cur, float64(cur-last), st.accepted.Load(), st.errs.Load())
				last = cur
				if time.Now().After(deadline) {
					fmt.Println()
					return
				}
			case <-ctx.Done():
				fmt.Println()
				return
			}
		}
	}()

	wg.Wait()
	<-done
	return st
}

// doVote выполняет один запрос голосования с уникальным fingerprint
// (X-Forwarded-For + User-Agent). Возвращает HTTP-статус (0 = сетевая ошибка).
func doVote(c *http.Client, url string, fpSeed, optID int) int {
	body := []byte(fmt.Sprintf(`{"option_id":%d}`, optID))
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.%d.%d.%d", (fpSeed>>16)&0xff, (fpSeed>>8)&0xff, fpSeed&0xff))
	req.Header.Set("User-Agent", fmt.Sprintf("loadtest/1.0 uid=%d", fpSeed))

	resp, err := c.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func printStats(st *stats, cfg config) {
	total := st.total.Load()
	acc := st.accepted.Load()
	errs := st.errs.Load()
	closed := st.closed.Load()
	bad := st.badReq.Load()

	elapsed := cfg.duration.Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	st.mu.Lock()
	lats := st.latencies
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	n := len(lats)
	pct := func(p float64) time.Duration {
		if n == 0 {
			return 0
		}
		idx := int(p * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return lats[idx]
	}
	p50, p90, p99, max := pct(0.50), pct(0.90), pct(0.99), time.Duration(0)
	if n > 0 {
		max = lats[n-1]
	}
	st.mu.Unlock()

	fmt.Printf("  запросов:      %d\n", total)
	fmt.Printf("  RPS (факт):    %.0f\n", float64(total)/elapsed)
	fmt.Printf("  accepted:      %d (%.2f%%)\n", acc, percent(acc, total))
	fmt.Printf("  poll_closed:   %d (%.2f%%)\n", closed, percent(closed, total))
	fmt.Printf("  bad_request:   %d\n", bad)
	fmt.Printf("  ошибок:        %d (%.2f%%)\n", errs, percent(errs, total))
	fmt.Printf("  latency p50:   %s\n", p50)
	fmt.Printf("  latency p90:   %s\n", p90)
	fmt.Printf("  latency p99:   %s\n", p99)
	fmt.Printf("  latency max:   %s\n", max)
}

func percent(a, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(a) / float64(total) * 100
}

// waitForResults периодически опрашивает админку, пока не появятся результаты.
func waitForResults(ctx context.Context, c *http.Client, cfg config, pollID string) error {
	deadline := time.Now().Add(cfg.graceWait)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		resp, err := c.Get(cfg.adminURL + "/admin/polls/" + pollID + "/results")
		if err != nil {
			continue
		}
		var poll struct {
			Status  string `json:"status"`
			Results *struct {
				Total   int64 `json:"total"`
				Options []struct {
					OptionID int   `json:"option_id"`
					Count    int64 `json:"count"`
				} `json:"options"`
				WorkersSeen int `json:"workers_seen"`
				WorkersDone int `json:"workers_done"`
			} `json:"results"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&poll)
		resp.Body.Close()

		if poll.Status == "completed" && poll.Results != nil {
			fmt.Printf("  status=%s total=%d workers_seen=%d workers_done=%d\n",
				poll.Status, poll.Results.Total, poll.Results.WorkersSeen, poll.Results.WorkersDone)
			for _, o := range poll.Results.Options {
				fmt.Printf("    option %d: %d\n", o.OptionID, o.Count)
			}
			return nil
		}
		fmt.Printf("  ещё не завершён (status=%s)...\n", poll.Status)
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for results (last status=%s)", poll.Status)
		}
	}
}

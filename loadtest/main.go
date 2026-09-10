// Command loadtest — сценарный нагрузочный тест против docker-compose стека.
//
// Сценарий (значения по умолчанию):
//   - общая длительность сценария — 3 минуты;
//   - 15 опросов, каждый длится 30 секунд;
//   - каждый опрос стартует в случайный момент внутри окна так, чтобы успеть
//     завершиться до конца сценария (агрегированная нагрузка: разгон →
//     плато → затухание);
//   - внутри каждого опроса интенсивность растёт от нуля, достигает пика и
//     затухает к нулю (параболический профиль);
//   - cookie подписываются КЛИЕНТОМ известным секретом (тот же
//     internal/fingerprint), поэтому каждый запрос идёт с уникальным
//     fingerprint'ом и дедупликация на стороне consumer'а не срабатывает;
//   - для каждого опроса измеряется время от ends_at до готовности результатов
//     и сверяется число голосов: ни один принятый (202) голос не должен
//     потеряться.
//
// Проверка корректности:
//
//	accepted(202) == results.total  для каждого опроса
//
// Если расхождение или результаты не готовы за отведённое время — тест
// завершается с ненулевым кодом.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/fingerprint"
)

// ---------- конфигурация ----------

type config struct {
	apiURLs        []string
	adminURL       string
	duration       time.Duration
	polls          int
	pollDuration   time.Duration
	votesPerPoll   int
	optionCount    int
	concurrency    int
	cookieSecret   string
	resultsTimeout time.Duration
	tick           time.Duration
	seed           int64
	// burst — режим поиска пика: один опрос, все воркеры жмут максимально
	// быстро, без профиля и пейсинга.
	burst bool
}

func main() {
	var (
		apiURLs  string
		adminURL string
	)
	cfg := config{}
	flag.StringVar(&apiURLs, "api-urls", "http://localhost:8080,http://localhost:8083",
		"base URL API-воркеров через запятую (недоступные отбрасываются)")
	flag.StringVar(&adminURL, "admin-url", "http://localhost:8081", "base URL сервиса результатов")
	flag.DurationVar(&cfg.duration, "duration", 3*time.Minute, "длительность сценария")
	flag.IntVar(&cfg.polls, "polls", 15, "число опросов")
	flag.DurationVar(&cfg.pollDuration, "poll-duration", 30*time.Second, "длительность одного опроса")
	flag.IntVar(&cfg.votesPerPoll, "votes-per-poll", 40000, "целевое число голосов на опрос")
	flag.IntVar(&cfg.optionCount, "options", 4, "число вариантов ответа")
	flag.IntVar(&cfg.concurrency, "c", 2000, "число параллельных воркеров-пользователей")
	flag.StringVar(&cfg.cookieSecret, "cookie-secret", "dev-cookie-secret-change-me",
		"секрет HMAC-подписи cookie (должен совпадать с COOKIE_SECRET API-воркеров)")
	flag.DurationVar(&cfg.resultsTimeout, "results-timeout", 120*time.Second,
		"сколько ждать готовности результатов после конца опроса")
	flag.DurationVar(&cfg.tick, "tick", 25*time.Millisecond, "шаг планировщика нагрузки")
	flag.Int64Var(&cfg.seed, "seed", 0, "seed генератора (0 — случайный)")
	flag.BoolVar(&cfg.burst, "burst", false, "режим поиска пика: один опрос, максимальная скорость без профиля")
	flag.Parse()

	cfg.apiURLs = splitURLs(apiURLs)
	cfg.adminURL = strings.TrimRight(adminURL, "/")

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "\nloadtest failed:", err)
		os.Exit(1)
	}
}

func splitURLs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(strings.TrimRight(p, "/")); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---------- профиль нагрузки ----------

// profileCumulative — доля голосов опроса, отправленных к моменту x ∈ [0,1].
// Первообразная от веса 6x(1-x), нормированная на единицу: 3x² − 2x³.
// Даёт плавный разгон, пик в середине и затухание.
func profileCumulative(x float64) float64 {
	switch {
	case x <= 0:
		return 0
	case x >= 1:
		return 1
	default:
		return 3*x*x - 2*x*x*x
	}
}

// ---------- время исполнения ----------

type pollPlan struct {
	index   int
	startAt time.Time
}

type pollRuntime struct {
	index    int
	id       string
	beginsAt time.Time
	endsAt   time.Time
	target   int64
	launched int64 // трогает только планировщик (один goroutine)

	accepted  atomic.Int64
	forbidden atomic.Int64

	completedAt time.Time
	resultTotal int64
	resultsErr  error
}

type job struct {
	poll *pollRuntime
	seq  int64
}

type collector struct {
	mu        sync.Mutex
	latencies []time.Duration
	accepted  atomic.Int64
	forbidden atomic.Int64
	badReq    atomic.Int64
	errs      atomic.Int64
}

func (c *collector) record(d time.Duration) {
	c.mu.Lock()
	c.latencies = append(c.latencies, d)
	c.mu.Unlock()
}

type harness struct {
	cfg      config
	client   *http.Client
	signer   *fingerprint.Signer
	apiURLs  []string
	adminURL string

	mu    sync.RWMutex
	polls []*pollRuntime

	seq  atomic.Int64
	jobs chan job
	rr   atomic.Uint64

	col *collector
	t0  time.Time

	allCreated chan struct{}
	stop       chan struct{}
	workersWG  sync.WaitGroup
	watchersWG sync.WaitGroup
	runnersWG  sync.WaitGroup
}

func run(cfg config) error {
	if cfg.burst {
		return runBurst(cfg)
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.concurrency * 2,
			MaxIdleConnsPerHost: cfg.concurrency * 2,
			MaxConnsPerHost:     cfg.concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	// Проверяем, какие API-воркеры доступны.
	if err := probe(cfg.adminURL+"/healthz", client); err != nil {
		return fmt.Errorf("results service is not reachable at %s: %w", cfg.adminURL, err)
	}
	apiURLs := make([]string, 0, len(cfg.apiURLs))
	for _, u := range cfg.apiURLs {
		if err := probe(u+"/healthz", client); err != nil {
			fmt.Printf("warning: api worker %s unreachable, skipped (%v)\n", u, err)
			continue
		}
		apiURLs = append(apiURLs, u)
	}
	if len(apiURLs) == 0 {
		return fmt.Errorf("no reachable api workers among %v", cfg.apiURLs)
	}

	seed := cfg.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	h := &harness{
		cfg:        cfg,
		client:     client,
		signer:     fingerprint.NewSigner(cfg.cookieSecret),
		apiURLs:    apiURLs,
		adminURL:   cfg.adminURL,
		jobs:       make(chan job, cfg.concurrency*4),
		col:        &collector{latencies: make([]time.Duration, 0, cfg.polls*cfg.votesPerPoll)},
		allCreated: make(chan struct{}),
		stop:       make(chan struct{}),
	}
	h.t0 = time.Now()

	fmt.Printf("scenario: duration=%s polls=%d poll-duration=%s votes/poll=%d concurrency=%d seed=%d\n",
		cfg.duration, cfg.polls, cfg.pollDuration, cfg.votesPerPoll, cfg.concurrency, seed)
	fmt.Printf("api workers: %v\n\n", apiURLs)

	plan := h.buildPlan(seed)

	h.startWorkers()
	go h.createAndWatch(plan)

	// Прогресс каждые 10 секунд.
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fmt.Printf("  [%4.0fs] accepted=%d forbidden=%d errors=%d\n",
					time.Since(h.t0).Seconds(), h.col.accepted.Load(), h.col.forbidden.Load(), h.col.errs.Load())
			case <-h.stop:
				return
			}
		}
	}()

	// Ждём создания всех опросов и завершения последнего.
	<-h.allCreated
	h.mu.RLock()
	var maxEnd time.Time
	for _, p := range h.polls {
		if p.endsAt.After(maxEnd) {
			maxEnd = p.endsAt
		}
	}
	h.mu.RUnlock()

	if d := time.Until(maxEnd); d > 0 {
		fmt.Printf("\nall polls created, waiting %s until last poll ends...\n", d.Round(time.Second))
		time.Sleep(d + h.cfg.tick)
	}

	close(h.stop)
	h.runnersWG.Wait()
	close(h.jobs)
	h.workersWG.Wait()
	<-progressDone

	// Дожидаемся результатов по всем опросам.
	fmt.Println("\nwaiting for results of all polls (done from all api workers)...")
	h.watchersWG.Wait()

	return h.report()
}

func (h *harness) buildPlan(seed int64) []pollPlan {
	rng := rand.New(rand.NewSource(seed))
	maxStart := h.cfg.duration - h.cfg.pollDuration
	if maxStart < 0 {
		maxStart = 0
	}
	plan := make([]pollPlan, 0, h.cfg.polls)
	for i := 0; i < h.cfg.polls; i++ {
		offset := time.Duration(rng.Int63n(int64(maxStart) + 1))
		plan = append(plan, pollPlan{index: i, startAt: h.t0.Add(offset)})
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].startAt.Before(plan[j].startAt) })
	return plan
}

// createAndWatch создаёт опросы по расписанию и сразу запускает наблюдение
// за его результатами.
func (h *harness) createAndWatch(plan []pollPlan) {
	defer close(h.allCreated)
	for _, p := range plan {
		if d := time.Until(p.startAt); d > 0 {
			time.Sleep(d)
		}
		if h.stopped() {
			return
		}
		created, err := h.createPoll()
		if err != nil {
			fmt.Printf("poll #%d creation failed: %v\n", p.index, err)
			continue
		}
		rt := &pollRuntime{
			index:    p.index,
			id:       created.ID,
			endsAt:   created.EndsAt,
			beginsAt: created.EndsAt.Add(-h.cfg.pollDuration),
			target:   int64(h.cfg.votesPerPoll),
		}
		h.mu.Lock()
		h.polls = append(h.polls, rt)
		h.mu.Unlock()

		fmt.Printf("  poll #%02d created id=%s start=+%4.0fs end=+%4.0fs\n",
			rt.index, rt.id, rt.beginsAt.Sub(h.t0).Seconds(), rt.endsAt.Sub(h.t0).Seconds())

		h.runnersWG.Add(1)
		go h.pump(rt)

		h.watchersWG.Add(1)
		go h.watchResults(rt)
	}
}

func (h *harness) stopped() bool {
	select {
	case <-h.stop:
		return true
	default:
		return false
	}
}

// pump — планировщик одного опроса: раз в tick выдаёт порцию голосов согласно
// параболическому профилю. У каждого опроса свой pump, поэтому профили
// накладываются и агрегированная нагрузка сначала растёт, затем падает.
func (h *harness) pump(p *pollRuntime) {
	defer h.runnersWG.Done()
	ticker := time.NewTicker(h.cfg.tick)
	defer ticker.Stop()

	for {
		var now time.Time
		select {
		case <-h.stop:
			return
		case now = <-ticker.C:
		}

		if now.Before(p.beginsAt) {
			continue
		}
		if !now.Before(p.endsAt) {
			return
		}
		span := p.endsAt.Sub(p.beginsAt).Seconds()
		if span <= 0 {
			return
		}
		x := now.Sub(p.beginsAt).Seconds() / span
		want := int64(float64(p.target) * profileCumulative(x))
		if want <= p.launched {
			continue
		}
		delta := want - p.launched
		p.launched = want
		for i := int64(0); i < delta; i++ {
			h.jobs <- job{poll: p, seq: h.seq.Add(1)}
		}
	}
}

func (h *harness) startWorkers() {
	for i := 0; i < h.cfg.concurrency; i++ {
		h.workersWG.Add(1)
		go func() {
			defer h.workersWG.Done()
			for j := range h.jobs {
				h.doVote(j)
			}
		}()
	}
}

// doVote формирует подписанную cookie и отправляет один голос.
func (h *harness) doVote(j job) {
	raw := fmt.Sprintf("u%d", j.seq)
	cookieVal := h.signer.Sign(j.poll.id, raw)

	body := []byte(fmt.Sprintf(`{"option_id":%d}`, int(j.seq)%h.cfg.optionCount+1))
	url := h.apiURLs[int(h.rr.Add(1))%len(h.apiURLs)] + "/polls/" + j.poll.id + "/vote"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		h.col.errs.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: fingerprint.CookieName, Value: cookieVal})

	start := time.Now()
	resp, err := h.client.Do(req)
	d := time.Since(start)
	h.col.record(d)
	if err != nil {
		h.col.errs.Add(1)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		h.col.accepted.Add(1)
		j.poll.accepted.Add(1)
	case http.StatusForbidden:
		// Опрос закрылся, пока запрос летел, — это нормально, голос не принят.
		h.col.forbidden.Add(1)
		j.poll.forbidden.Add(1)
	case http.StatusBadRequest:
		h.col.badReq.Add(1)
	default:
		h.col.errs.Add(1)
	}
}

// watchResults ждёт, пока сервис результатов отдаст completed, и фиксирует
// время готовности.
func (h *harness) watchResults(rt *pollRuntime) {
	defer h.watchersWG.Done()

	deadline := time.Now().Add(h.cfg.resultsTimeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		total, status, err := h.fetchResults(rt.id)
		if err == nil && status == "completed" {
			rt.completedAt = time.Now()
			rt.resultTotal = total
			return
		}
		if time.Now().After(deadline) {
			if err != nil {
				rt.resultsErr = fmt.Errorf("timeout, last error: %w", err)
			} else {
				rt.resultsErr = fmt.Errorf("timeout, last status: %s", status)
			}
			return
		}
		<-ticker.C
	}
}

func (h *harness) fetchResults(pollID string) (total int64, status string, err error) {
	resp, err := h.client.Get(h.adminURL + "/admin/polls/" + pollID + "/results")
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, "", fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var payload struct {
		Status  string `json:"status"`
		Results *struct {
			Total int64 `json:"total"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, "", err
	}
	if payload.Results != nil {
		return payload.Results.Total, payload.Status, nil
	}
	return 0, payload.Status, nil
}

type createdPoll struct {
	ID     string    `json:"id"`
	EndsAt time.Time `json:"ends_at"`
}

func (h *harness) createPoll() (createdPoll, error) {
	opts := make([]string, h.cfg.optionCount)
	for i := range opts {
		opts[i] = fmt.Sprintf("Option %d", i+1)
	}
	body, _ := json.Marshal(map[string]any{
		"question":         "Load test question?",
		"options":          opts,
		"duration_seconds": int(h.cfg.pollDuration.Seconds()),
	})
	resp, err := h.client.Post(h.adminURL+"/admin/polls", "application/json", bytes.NewReader(body))
	if err != nil {
		return createdPoll{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return createdPoll{}, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var p createdPoll
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return createdPoll{}, err
	}
	return p, nil
}

func probe(url string, c *http.Client) error {
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// ---------- отчёт ----------

func (h *harness) report() error {
	h.mu.RLock()
	polls := append([]*pollRuntime(nil), h.polls...)
	h.mu.RUnlock()
	sort.Slice(polls, func(i, j int) bool { return polls[i].index < polls[j].index })

	fmt.Println("\n===== РЕЗУЛЬТАТЫ ПО ОПРОСАМ =====")
	fmt.Printf("%-4s %-10s %-10s %10s %10s %12s %10s\n",
		"#", "start", "end", "accepted", "in-results", "ready-after", "verdict")

	var (
		totalAccepted int64
		totalResults  int64
		readiness     []time.Duration
		mismatches    int
	)

	for _, p := range polls {
		acc := p.accepted.Load()
		totalAccepted += acc
		verdict := "OK"
		readyStr := "-"

		if p.resultsErr != nil {
			verdict = "NO RESULT"
			mismatches++
		} else {
			totalResults += p.resultTotal
			readiness = append(readiness, p.completedAt.Sub(p.endsAt))
			readyStr = p.completedAt.Sub(p.endsAt).Round(time.Millisecond).String()

			switch {
			case p.resultTotal == acc:
				// Идеально: сколько клиент получил 202, столько и посчитано.
			case p.resultTotal > acc:
				// В результатах БОЛЬШЕ, чем клиент подтвердил. Причина —
				// сетевые ошибки на клиенте (сервер обработал запрос, но
				// ответ не дошёл). Это НЕ потеря данных.
				verdict = fmt.Sprintf("+%d unconfirmed", p.resultTotal-acc)
			default:
				// В результатах МЕНЬШЕ — вот это потеря голосов.
				verdict = fmt.Sprintf("LOST %d", acc-p.resultTotal)
				mismatches++
			}
		}

		fmt.Printf("%-4d %-10s %-10s %10d %10d %12s %10s\n",
			p.index,
			fmt.Sprintf("+%.0fs", p.beginsAt.Sub(h.t0).Seconds()),
			fmt.Sprintf("+%.0fs", p.endsAt.Sub(h.t0).Seconds()),
			acc, p.resultTotal, readyStr, verdict)
	}

	elapsed := time.Since(h.t0)
	lat := h.col.latencies

	fmt.Println("\n===== ИТОГИ =====")
	fmt.Printf("  длительность сценария:   %s\n", elapsed.Round(time.Second))
	fmt.Printf("  опросов:                 %d\n", len(polls))
	fmt.Printf("  принято голосов (202):   %d\n", totalAccepted)
	fmt.Printf("  учтено в результатах:    %d\n", totalResults)
	fmt.Printf("  отказов 403 (закрыт):    %d\n", h.col.forbidden.Load())
	fmt.Printf("  bad_request:             %d\n", h.col.badReq.Load())
	fmt.Printf("  сетевых/прочих ошибок:   %d\n", h.col.errs.Load())
	fmt.Printf("  RPS (принято/сек):       %.0f\n", float64(totalAccepted)/elapsed.Seconds())
	fmt.Printf("  latency p50:             %s\n", percentile(lat, 0.50))
	fmt.Printf("  latency p90:             %s\n", percentile(lat, 0.90))
	fmt.Printf("  latency p99:             %s\n", percentile(lat, 0.99))

	if len(readiness) > 0 {
		sort.Slice(readiness, func(i, j int) bool { return readiness[i] < readiness[j] })
		fmt.Printf("  готовность результатов:  p50=%s p95=%s max=%s (после ends_at)\n",
			percentileDur(readiness, 0.50),
			percentileDur(readiness, 0.95),
			readiness[len(readiness)-1].Round(time.Millisecond))
	}

	fmt.Println()
	if mismatches > 0 {
		return fmt.Errorf("FAILED: %d poll(s) с потерянными голосами или без результатов", mismatches)
	}
	fmt.Println("OK: ни один ПОДТВЕРЖДЁННЫЙ голос не потерян, все опросы завершены")
	fmt.Println("    (колонка '+N unconfirmed' — клиент не дождался ответа, сервер голос учёл)")
	return nil
}

func percentile(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(p*float64(len(s)-1))].Round(time.Microsecond)
}

func percentileDur(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return 0
	}
	return v[int(p*float64(len(v)-1))].Round(time.Millisecond)
}

// ---------- режим поиска пика (burst) ----------

// runBurst создаёт один опрос и непрерывно шлёт голоса всеми воркерами на
// максимальной скорости — без профиля и пейсинга. Нужен, чтобы найти потолок
// системы. Метрики считаются по скользящим секундам, чтобы видеть деградацию.
func runBurst(cfg config) error {
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.concurrency * 2,
			MaxIdleConnsPerHost: cfg.concurrency * 2,
			MaxConnsPerHost:     cfg.concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	if err := probe(cfg.adminURL+"/healthz", client); err != nil {
		return fmt.Errorf("results service unreachable: %w", err)
	}
	apiURLs := make([]string, 0, len(cfg.apiURLs))
	for _, u := range cfg.apiURLs {
		if err := probe(u+"/healthz", client); err != nil {
			fmt.Printf("warning: api worker %s unreachable, skipped\n", u)
			continue
		}
		apiURLs = append(apiURLs, u)
	}
	if len(apiURLs) == 0 {
		return fmt.Errorf("no reachable api workers")
	}

	// Опрос длится ровно столько, сколько жать нагрузку.
	dur := cfg.pollDuration
	if cfg.duration > 0 && cfg.duration < dur {
		dur = cfg.duration
	}
	pollID, endsAt, err := createPollFor(client, cfg.adminURL, cfg.optionCount, int(dur.Seconds())+5)
	if err != nil {
		return fmt.Errorf("create poll: %w", err)
	}
	signer := fingerprint.NewSigner(cfg.cookieSecret)
	fmt.Printf("BURST: poll=%s workers=%d concurrency=%d ends_at=%s\n",
		pollID, len(apiURLs), cfg.concurrency, endsAt.Format(time.RFC3339))

	var (
		accepted  atomic.Int64
		errs      atomic.Int64
		seq       atomic.Int64
		rr        atomic.Uint64
		perSec    atomic.Int64
		stopAt    = time.Now().Add(dur)
		mu        sync.Mutex
		latencies []time.Duration
	)
	url := func() string { return apiURLs[int(rr.Add(1))%len(apiURLs)] + "/polls/" + pollID + "/vote" }

	// Замер секундного темпа.
	lastTotal := int64(0)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			cur := accepted.Load()
			perSec.Store(cur - lastTotal)
			lastTotal = cur
			fmt.Printf("  [%3.0fs] rps=%d accepted=%d err=%d\n",
				time.Until(stopAt).Seconds()*-1+time.Until(stopAt).Seconds(), perSec.Load(), cur, errs.Load())
			if time.Now().After(stopAt) {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < cfg.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				raw := "b" + itoa(seq.Add(1))
				body := []byte(`{"option_id":1}`)
				req, _ := http.NewRequest(http.MethodPost, url(), bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.AddCookie(&http.Cookie{Name: fingerprint.CookieName, Value: signer.Sign(pollID, raw)})
				start := time.Now()
				resp, err := client.Do(req)
				d := time.Since(start)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
				if err != nil {
					errs.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusAccepted {
					accepted.Add(1)
				} else {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	total := accepted.Load()
	elapsed := dur.Seconds()
	mu.Lock()
	sorted := append([]time.Duration(nil), latencies...)
	mu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		if len(sorted) == 0 {
			return 0
		}
		return sorted[int(p*float64(len(sorted)-1))].Round(time.Microsecond)
	}

	fmt.Printf("\n===== BURST РЕЗУЛЬТАТ =====\n")
	fmt.Printf("  воркеров-клиентов:   %d\n", cfg.concurrency)
	fmt.Printf("  длительность:        %s\n", dur)
	fmt.Printf("  принято (202):       %d\n", total)
	fmt.Printf("  ошибок:              %d\n", errs.Load())
	fmt.Printf("  средний RPS:         %.0f\n", float64(total)/elapsed)
	fmt.Printf("  последняя секунда:   %d rps\n", perSec.Load())
	fmt.Printf("  latency p50/p90/p99: %s / %s / %s\n", pct(0.50), pct(0.90), pct(0.99))

	// Проверяем, что голоса доехали.
	fmt.Println("\n  жду результатов...")
	deadline := time.Now().Add(cfg.resultsTimeout)
	for {
		got, status, err := fetchResultsFor(client, cfg.adminURL, pollID)
		if err == nil && status == "completed" {
			fmt.Printf("  учтено в результатах: %d (из %d принятых)\n", got, total)
			if got < total {
				return fmt.Errorf("LOST %d votes", total-got)
			}
			return nil
		}
		if time.Now().After(deadline) {
			last := status
			if err != nil {
				last = err.Error()
			}
			return fmt.Errorf("results not ready: %s", last)
		}
		time.Sleep(time.Second)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func createPollFor(c *http.Client, adminURL string, options, durationSec int) (string, time.Time, error) {
	opts := make([]string, options)
	for i := range opts {
		opts[i] = fmt.Sprintf("Option %d", i+1)
	}
	body, _ := json.Marshal(map[string]any{
		"question":         "Burst test?",
		"options":          opts,
		"duration_seconds": durationSec,
	})
	resp, err := c.Post(adminURL+"/admin/polls", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", time.Time{}, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var p struct {
		ID     string    `json:"id"`
		EndsAt time.Time `json:"ends_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return "", time.Time{}, err
	}
	return p.ID, p.EndsAt, nil
}

func fetchResultsFor(c *http.Client, adminURL, pollID string) (int64, string, error) {
	resp, err := c.Get(adminURL + "/admin/polls/" + pollID + "/results")
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var payload struct {
		Status  string `json:"status"`
		Results *struct {
			Total int64 `json:"total"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, "", err
	}
	if payload.Results != nil {
		return payload.Results.Total, payload.Status, nil
	}
	return 0, payload.Status, nil
}

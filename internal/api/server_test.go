package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
)

// fakeProducer — фейковый Producer: запоминает все отправленные батчи.
type fakeProducer struct {
	mu      sync.Mutex
	batches []model.VoteBatch
	failN   int // сколько первых вызовов вернуть с ошибкой
}

func (f *fakeProducer) Produce(_ context.Context, b model.VoteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return context.DeadlineExceeded
	}
	f.batches = append(f.batches, b)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func (f *fakeProducer) snapshot() []model.VoteBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.VoteBatch, len(f.batches))
	copy(out, f.batches)
	return out
}

func (f *fakeProducer) waitFor(t *testing.T, n int, timeout time.Duration) []model.VoteBatch {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b := f.snapshot()
		if len(b) >= n {
			return b
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d batches, got %d", n, len(f.snapshot()))
	return nil
}

// fakeResults — фейковый ResultsClient.
type fakeResults struct {
	mu       sync.Mutex
	polls    map[string]model.Poll
	getErr   error
	getCalls int
}

func newFakeResults() *fakeResults {
	return &fakeResults{polls: make(map[string]model.Poll)}
}

func (f *fakeResults) GetPoll(_ context.Context, id string) (model.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getErr != nil {
		return model.Poll{}, f.getErr
	}
	p, ok := f.polls[id]
	if !ok {
		return model.Poll{}, resultsclient.ErrNotFound{PollID: id}
	}
	return p, nil
}

func (f *fakeResults) FlushResults(_ context.Context, _ string, _ model.Results) (bool, error) {
	return true, nil
}

// testHarness собирает сервер + фейки и управляемые часы.
type testHarness struct {
	ts       *httptest.Server
	producer *fakeProducer
	results  *fakeResults
	server   *Server
	clock    *testClock
	cancel   context.CancelFunc
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func newHarness(t *testing.T, cfg Config) *testHarness {
	t.Helper()
	producer := &fakeProducer{}
	results := newFakeResults()
	clock := &testClock{t: time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)}

	ctx, cancel := context.WithCancel(context.Background())
	sender := NewSender(producer, 512, 2, nil)
	sender.Start(ctx)

	if cfg.WorkerID == "" {
		cfg.WorkerID = "w1"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10000
	}
	server := NewServer(results, sender, cfg, nil)
	server.SetNow(clock.now)

	ts := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		ts.Close()
		cancel()
		sender.Wait()
	})

	return &testHarness{
		ts:       ts,
		producer: producer,
		results:  results,
		server:   server,
		clock:    clock,
		cancel:   cancel,
	}
}

func (h *testHarness) addPoll(id string, endsAt time.Time, options ...int) {
	poll := model.Poll{
		ID:        id,
		Question:  "q",
		Status:    model.PollStatusActive,
		CreatedAt: h.clock.now(),
		EndsAt:    endsAt,
	}
	if len(options) == 0 {
		options = []int{1, 2}
	}
	for _, oid := range options {
		poll.Options = append(poll.Options, model.Option{ID: oid, Text: "opt"})
	}
	h.results.polls[id] = poll
}

func postVote(t *testing.T, url string, optionID int, cookie *http.Cookie) (*http.Response, []byte) {
	t.Helper()
	body, _ := json.Marshal(model.VoteRequest{OptionID: optionID})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func TestGetPoll_IssuesCookie(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	resp, err := http.Get(h.ts.URL + "/polls/p1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	cookies := resp.Cookies()
	if len(cookies) != 1 || cookies[0].Name != "voter_id" || cookies[0].Value == "" {
		t.Fatalf("expected voter_id cookie, got %+v", cookies)
	}
	if !cookies[0].HttpOnly {
		t.Error("cookie should be HttpOnly")
	}
	// Результаты не должны отдаваться.
	var poll model.Poll
	_ = json.NewDecoder(resp.Body).Decode(&poll)
	if poll.Results != nil {
		t.Errorf("results must not be exposed: %+v", poll.Results)
	}
}

func TestGetPoll_DoesNotOverwriteExistingCookie(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	req, _ := http.NewRequest(http.MethodGet, h.ts.URL+"/polls/p1", nil)
	req.AddCookie(&http.Cookie{Name: "voter_id", Value: "existing-uuid"})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if cs := resp.Cookies(); len(cs) != 0 {
		t.Errorf("must not reissue cookie, got %+v", cs)
	}
}

func TestGetPoll_NotFound(t *testing.T) {
	h := newHarness(t, Config{})
	resp, err := http.Get(h.ts.URL + "/polls/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestGetPoll_ResultsUnavailable(t *testing.T) {
	h := newHarness(t, Config{})
	h.results.getErr = context.DeadlineExceeded
	resp, err := http.Get(h.ts.URL + "/polls/p1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestVote_Accepted(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	resp, body := postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u1"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, body)
	}
	var vr model.VoteResponse
	_ = json.Unmarshal(body, &vr)
	if vr.Status != "accepted" {
		t.Errorf("status = %q, want accepted", vr.Status)
	}
}

func TestVote_InvalidOption(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(time.Minute), 1, 2)

	resp, body := postVote(t, h.ts.URL+"/polls/p1/vote", 99, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, body)
	}
}

func TestVote_PollClosed(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(-time.Second)) // уже закончился

	resp, body := postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u1"})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, body)
	}
	// Должно уйти "done"-сообщение (закрытие опроса).
	batches := h.producer.waitFor(t, 1, 2*time.Second)
	if !batches[0].Done {
		t.Errorf("expected done batch, got %+v", batches[0])
	}
}

func TestVote_NotFound(t *testing.T) {
	h := newHarness(t, Config{})
	resp, body := postVote(t, h.ts.URL+"/polls/missing/vote", 1, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, body)
	}
}

func TestVote_Batching(t *testing.T) {
	h := newHarness(t, Config{BatchSize: 3})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	// Два голоса по уникальным fingerprint'ам.
	postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u1"})
	postVote(t, h.ts.URL+"/polls/p1/vote", 2, &http.Cookie{Name: "voter_id", Value: "u2"})
	// Батч ещё не должен уйти.
	time.Sleep(50 * time.Millisecond)
	if n := len(h.producer.snapshot()); n != 0 {
		t.Fatalf("expected no batches yet, got %d", n)
	}
	// Третий голос добивает батч.
	postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u3"})

	batches := h.producer.waitFor(t, 1, 2*time.Second)
	b := batches[0]
	if b.Done {
		t.Error("regular batch must not be done")
	}
	if b.PollID != "p1" || b.WorkerID != "w1" {
		t.Errorf("unexpected meta: %+v", b)
	}
	if len(b.Votes) != 3 {
		t.Fatalf("votes = %d, want 3", len(b.Votes))
	}
	fps := map[string]int{}
	for _, v := range b.Votes {
		fps[v.Fingerprint]++
	}
	if len(fps) != 3 {
		t.Errorf("expected 3 distinct fingerprints, got %v", fps)
	}
}

func TestVote_FingerprintFallback(t *testing.T) {
	h := newHarness(t, Config{BatchSize: 2})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	// Без cookie — должен использоваться hash(IP+UA). Одинаковые IP+UA => один
	// fingerprint, разные => разные.
	req1, _ := http.NewRequest(http.MethodPost, h.ts.URL+"/polls/p1/vote", bytes.NewReader([]byte(`{"option_id":1}`)))
	req1.Header.Set("X-Forwarded-For", "10.0.0.1")
	req1.Header.Set("User-Agent", "test-agent")
	resp, _ := http.DefaultClient.Do(req1)
	resp.Body.Close()

	req2, _ := http.NewRequest(http.MethodPost, h.ts.URL+"/polls/p1/vote", bytes.NewReader([]byte(`{"option_id":2}`)))
	req2.Header.Set("X-Forwarded-For", "10.0.0.2")
	req2.Header.Set("User-Agent", "test-agent")
	resp2, _ := http.DefaultClient.Do(req2)
	resp2.Body.Close()

	batches := h.producer.waitFor(t, 1, 2*time.Second)
	b := batches[0]
	if len(b.Votes) != 2 {
		t.Fatalf("votes = %d, want 2", len(b.Votes))
	}
	if b.Votes[0].Fingerprint == b.Votes[1].Fingerprint {
		t.Errorf("different IPs must yield different fingerprints: %q", b.Votes[0].Fingerprint)
	}
}

func TestClosePoll_SendsRemainderAndDoneOnce(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(time.Minute))

	// Два голоса остаются в буфере (batch size большой).
	postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u1"})
	postVote(t, h.ts.URL+"/polls/p1/vote", 2, &http.Cookie{Name: "voter_id", Value: "u2"})

	h.server.closePoll("p1")

	batches := h.producer.waitFor(t, 2, 2*time.Second)
	// Первый — остаток голосов, второй — done.
	if len(batches[0].Votes) != 2 || batches[0].Done {
		t.Errorf("first batch should be remainder, got %+v", batches[0])
	}
	if !batches[1].Done || len(batches[1].Votes) != 0 {
		t.Errorf("second batch should be done-only, got %+v", batches[1])
	}

	// Повторное закрытие не должно слать ещё один done.
	h.server.closePoll("p1")
	time.Sleep(50 * time.Millisecond)
	if n := len(h.producer.snapshot()); n != 2 {
		t.Fatalf("expected exactly 2 batches after repeated close, got %d", n)
	}
}

func TestSweep_ClosesExpiredPolls(t *testing.T) {
	h := newHarness(t, Config{})
	h.addPoll("p1", h.clock.now().Add(30*time.Second))

	// Голос, чтобы буфер знал об опросе.
	postVote(t, h.ts.URL+"/polls/p1/vote", 1, &http.Cookie{Name: "voter_id", Value: "u1"})

	// Время ещё не пришло — ничего не закрывается.
	h.server.sweep()
	time.Sleep(30 * time.Millisecond)
	if n := len(h.producer.snapshot()); n != 0 {
		t.Fatalf("no close expected yet, got %d batches", n)
	}

	// Переводим часы за ends_at и снова sweep — опрос должен закрыться.
	h.clock.set(h.clock.now().Add(time.Minute))
	h.server.sweep()
	batches := h.producer.waitFor(t, 2, 2*time.Second) // remainder + done
	db := batches[len(batches)-1]
	if !db.Done {
		t.Errorf("expected done after sweep, got %+v", db)
	}
}

func TestSender_RetriesUntilSuccess(t *testing.T) {
	producer := &fakeProducer{failN: 3}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewSender(producer, 8, 1, nil)
	s.Start(ctx)

	s.Enqueue(model.VoteBatch{PollID: "p1", WorkerID: "w1", Votes: []model.Vote{{Fingerprint: "f", OptionID: 1}}})
	b := producer.waitFor(t, 1, 5*time.Second)
	if len(b) != 1 {
		t.Fatalf("expected 1 batch after retries, got %d", len(b))
	}
	if _, failed := s.Stats(); failed == 0 {
		t.Error("expected failed counter > 0")
	}
}

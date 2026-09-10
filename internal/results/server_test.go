package results

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
)

// fakeStorage — in-memory реализация Storage для интеграционных тестов.
// Фиксирует вызовы, чтобы тесты могли их проверять.
type fakeStorage struct {
	mu      sync.Mutex
	polls   map[string]model.Poll
	pingErr error

	createCalls int
	getCalls    int
	flushCalls  int
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{polls: make(map[string]model.Poll)}
}

func (f *fakeStorage) CreatePoll(_ context.Context, poll model.Poll) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.polls[poll.ID] = poll
	return nil
}

func (f *fakeStorage) GetPoll(_ context.Context, id string) (model.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	p, ok := f.polls[id]
	if !ok {
		return model.Poll{}, ErrNotFound
	}
	return p, nil
}

func (f *fakeStorage) FlushResults(_ context.Context, id string, res model.Results) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushCalls++
	p, ok := f.polls[id]
	if !ok {
		return false, ErrNotFound
	}
	if p.Status == model.PollStatusCompleted {
		return false, nil
	}
	p.Status = model.PollStatusCompleted
	results := res
	p.Results = &results
	f.polls[id] = p
	return true, nil
}

func (f *fakeStorage) Ping(_ context.Context) error {
	return f.pingErr
}

// newTestServer поднимает HTTP-сервер с фейковым хранилищем.
func newTestServer(t *testing.T) (*httptest.Server, *fakeStorage) {
	t.Helper()
	fs := newFakeStorage()
	srv := NewServer(fs)
	// Фиксируем время для детерминированных ends_at.
	fixed := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	srv.now = func() time.Time { return fixed }
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, fs
}

func doJSON(t *testing.T, method, url string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, buf.Bytes()
}

func TestCreatePoll_Success(t *testing.T) {
	ts, fs := newTestServer(t)

	resp, body := doJSON(t, http.MethodPost, ts.URL+"/admin/polls", model.CreatePollRequest{
		Question:        "Favorite color?",
		Options:         []string{"Red", "Green", "Blue"},
		DurationSeconds: 60,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", resp.StatusCode, body)
	}

	var poll model.Poll
	if err := json.Unmarshal(body, &poll); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if poll.ID == "" {
		t.Error("expected non-empty poll id")
	}
	if poll.Status != model.PollStatusActive {
		t.Errorf("status = %q, want active", poll.Status)
	}
	if len(poll.Options) != 3 {
		t.Fatalf("options len = %d, want 3", len(poll.Options))
	}
	// id опций должны быть 1..N.
	for i, o := range poll.Options {
		if o.ID != i+1 {
			t.Errorf("option[%d].id = %d, want %d", i, o.ID, i+1)
		}
	}
	wantEnds := time.Date(2026, 9, 10, 8, 1, 0, 0, time.UTC)
	if !poll.EndsAt.Equal(wantEnds) {
		t.Errorf("ends_at = %v, want %v", poll.EndsAt, wantEnds)
	}
	if fs.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", fs.createCalls)
	}
}

func TestCreatePoll_Validation(t *testing.T) {
	ts, fs := newTestServer(t)
	future := time.Date(2026, 9, 10, 8, 5, 0, 0, time.UTC)
	past := time.Date(2026, 9, 10, 7, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		req  model.CreatePollRequest
	}{
		{"empty question", model.CreatePollRequest{Options: []string{"a", "b"}, DurationSeconds: 10}},
		{"less than 2 options", model.CreatePollRequest{Question: "q", Options: []string{"a"}, DurationSeconds: 10}},
		{"empty option", model.CreatePollRequest{Question: "q", Options: []string{"a", ""}, DurationSeconds: 10}},
		{"no ends specified", model.CreatePollRequest{Question: "q", Options: []string{"a", "b"}}},
		{"ends in past", model.CreatePollRequest{Question: "q", Options: []string{"a", "b"}, EndsAt: &past}},
		{"zero duration", model.CreatePollRequest{Question: "q", Options: []string{"a", "b"}, DurationSeconds: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := doJSON(t, http.MethodPost, ts.URL+"/admin/polls", tc.req)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, body)
			}
			var e model.ErrorResponse
			if err := json.Unmarshal(body, &e); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}
			if e.Error != "bad_request" {
				t.Errorf("error = %q, want bad_request", e.Error)
			}
		})
	}

	// ends_at в будущем — валидно.
	resp, _ := doJSON(t, http.MethodPost, ts.URL+"/admin/polls", model.CreatePollRequest{
		Question: "q", Options: []string{"a", "b"}, EndsAt: &future,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("future ends_at: status = %d, want 201", resp.StatusCode)
	}
	if fs.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (only the valid request)", fs.createCalls)
	}
}

func TestInternalGetPoll_NoResults(t *testing.T) {
	ts, fs := newTestServer(t)
	ctx := context.Background()
	_ = ctx
	// Кладём опрос с результатами напрямую в фейк.
	poll := model.Poll{
		ID: "p1", Question: "q",
		Options:   []model.Option{{ID: 1, Text: "a"}, {ID: 2, Text: "b"}},
		Status:    model.PollStatusCompleted,
		CreatedAt: time.Now(),
		EndsAt:    time.Now(),
		Results:   &model.Results{Total: 10},
	}
	fs.polls["p1"] = poll

	resp, body := doJSON(t, http.MethodGet, ts.URL+"/internal/polls/p1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var got model.Poll
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Results != nil {
		t.Errorf("internal endpoint must not expose results, got %+v", got.Results)
	}
	if got.ID != "p1" || got.EndsAt.IsZero() {
		t.Errorf("unexpected poll: %+v", got)
	}
}

func TestInternalGetPoll_NotFound(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := doJSON(t, http.MethodGet, ts.URL+"/internal/polls/none", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, body)
	}
}

func TestFlushResults_Idempotent(t *testing.T) {
	ts, fs := newTestServer(t)
	fs.polls["p1"] = model.Poll{
		ID: "p1", Question: "q",
		Options:   []model.Option{{ID: 1, Text: "a"}},
		Status:    model.PollStatusActive,
		CreatedAt: time.Now(), EndsAt: time.Now(),
	}

	flush := model.FlushRequest{Results: model.Results{
		Total:   3,
		Options: []model.OptionResult{{OptionID: 1, Count: 3}},
	}}

	// Первый flush — записано.
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/internal/polls/p1/results", flush)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first flush status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var r1 flushResponse
	_ = json.Unmarshal(body, &r1)
	if !r1.Written {
		t.Errorf("first flush written = false, want true")
	}

	// Повторный flush — идемпотентный no-op (409).
	resp, body = doJSON(t, http.MethodPost, ts.URL+"/internal/polls/p1/results", flush)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second flush status = %d, want 409; body=%s", resp.StatusCode, body)
	}
	var r2 flushResponse
	_ = json.Unmarshal(body, &r2)
	if r2.Written {
		t.Errorf("second flush written = true, want false")
	}
	if fs.flushCalls != 2 {
		t.Errorf("flushCalls = %d, want 2", fs.flushCalls)
	}
}

func TestFlushResults_NotFound(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/internal/polls/none/results",
		model.FlushRequest{Results: model.Results{Total: 1}})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, body)
	}
}

func TestAdminResults_Flow(t *testing.T) {
	ts, fs := newTestServer(t)

	// Создаём опрос.
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/admin/polls", model.CreatePollRequest{
		Question: "q", Options: []string{"a", "b"}, DurationSeconds: 60,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", resp.StatusCode, body)
	}
	var poll model.Poll
	_ = json.Unmarshal(body, &poll)

	// До завершения — 200 без results.
	resp, body = doJSON(t, http.MethodGet, ts.URL+"/admin/polls/"+poll.ID+"/results", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("results status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var before model.Poll
	_ = json.Unmarshal(body, &before)
	if before.Results != nil {
		t.Errorf("results before flush = %+v, want nil", before.Results)
	}

	// Flush.
	_, _ = doJSON(t, http.MethodPost, ts.URL+"/internal/polls/"+poll.ID+"/results",
		model.FlushRequest{Results: model.Results{Total: 7, Options: []model.OptionResult{{OptionID: 1, Count: 7}}}})

	// После — results на месте.
	resp, body = doJSON(t, http.MethodGet, ts.URL+"/admin/polls/"+poll.ID+"/results", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("results status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var after model.Poll
	_ = json.Unmarshal(body, &after)
	if after.Results == nil || after.Results.Total != 7 {
		t.Errorf("results after flush = %+v, want total 7", after.Results)
	}
	if after.Status != model.PollStatusCompleted {
		t.Errorf("status = %q, want completed", after.Status)
	}
	if fs.getCalls == 0 {
		t.Error("expected storage.GetPoll to be called")
	}
}

func TestHealthz(t *testing.T) {
	ts, fs := newTestServer(t)

	resp, body := doJSON(t, http.MethodGet, ts.URL+"/healthz", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}

	fs.pingErr = context.DeadlineExceeded
	resp, body = doJSON(t, http.MethodGet, ts.URL+"/healthz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", resp.StatusCode, body)
	}
}

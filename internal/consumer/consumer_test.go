package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/kafka"
	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
)

// ---- фейки ----

// fakeReader — фейковый Reader: запоминает коммиты, отдаёт сообщения из канала.
type fakeReader struct {
	mu      sync.Mutex
	commits []kafka.Message
	closed  bool
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, msgs...)
	return nil
}

func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeReader) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.commits))
	for i, m := range f.commits {
		out[i] = m.Offset
	}
	return out
}

// fakeResults — фейковый ResultsClient.
type fakeResults struct {
	mu       sync.Mutex
	polls    map[string]model.Poll
	flushes  []flushCall
	flushErr error
	// alreadyWritten — если true, FlushResults отвечает written=false (409).
	alreadyWritten bool
	flushErrN      int // сколько первых вызовов вернуть ошибку
}

type flushCall struct {
	pollID  string
	results model.Results
}

func newFakeResults() *fakeResults {
	return &fakeResults{polls: make(map[string]model.Poll)}
}

func (f *fakeResults) GetPoll(_ context.Context, id string) (model.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.polls[id]
	if !ok {
		return model.Poll{}, resultsclient.ErrNotFound{PollID: id}
	}
	return p, nil
}

func (f *fakeResults) FlushResults(_ context.Context, id string, r model.Results) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flushErrN > 0 {
		f.flushErrN--
		return false, fmt.Errorf("results unavailable")
	}
	if f.flushErr != nil {
		return false, f.flushErr
	}
	f.flushes = append(f.flushes, flushCall{pollID: id, results: r})
	if f.alreadyWritten {
		return false, nil
	}
	return true, nil
}

func (f *fakeResults) flushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.flushes)
}

func (f *fakeResults) lastFlush() (flushCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.flushes) == 0 {
		return flushCall{}, false
	}
	return f.flushes[len(f.flushes)-1], true
}

// testClock — управляемые часы.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ---- helpers ----

type harness struct {
	cons    *Consumer
	reader  *fakeReader
	results *fakeResults
	clock   *testClock
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	clock := &testClock{t: time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)}
	reader := &fakeReader{}
	results := newFakeResults()
	cons := New(reader, results, cfg, nil)
	cons.SetNow(clock.now)
	return &harness{cons: cons, reader: reader, results: results, clock: clock}
}

func (h *harness) addPoll(id string, endsAt time.Time) {
	h.results.polls[id] = model.Poll{
		ID: id, Question: "q", Status: model.PollStatusActive,
		CreatedAt: h.clock.now(), EndsAt: endsAt,
		Options: []model.Option{{ID: 1, Text: "a"}, {ID: 2, Text: "b"}},
	}
}

// msg строит Kafka-сообщение с батчем голосов.
func msg(partition int, offset int64, b model.VoteBatch) kafka.Message {
	val, _ := json.Marshal(b)
	return kafka.Message{
		Topic: "votes", Partition: partition, Offset: offset,
		Key: []byte(b.PollID), Value: val,
	}
}

func votes(pollID, workerID string, pairs ...string) model.VoteBatch {
	b := model.VoteBatch{PollID: pollID, WorkerID: workerID}
	for i := 0; i+1 < len(pairs); i += 2 {
		var oid int
		_, _ = fmt.Sscanf(pairs[i+1], "%d", &oid)
		b.Votes = append(b.Votes, model.Vote{Fingerprint: pairs[i], OptionID: oid})
	}
	return b
}

// ---- тесты ----

func TestDedup_SameFingerprintCountedOnce(t *testing.T) {
	h := newHarness(t, Config{CloseGrace: 0, CloseHardTmo: time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	ctx := context.Background()

	// Три голоса: два одинаковых fingerprint (дубликат) + один новый.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1", "fp1", "1", "fp2", "2")))

	snap, ok := h.cons.Snapshot("p1")
	if !ok {
		t.Fatal("poll state not found")
	}
	if snap.Total != 2 {
		t.Errorf("total = %d, want 2 (duplicate dropped)", snap.Total)
	}
	if snap.Unique != 2 {
		t.Errorf("unique = %d, want 2", snap.Unique)
	}
}

func TestFlush_OnDoneQuorum(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: 10 * time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	ctx := context.Background()

	// Голоса от двух воркеров.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, votes("p1", "w2", "fp2", "2")))
	// "done" от обоих.
	h.cons.handleMessage(ctx, msg(0, 2, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))
	h.cons.handleMessage(ctx, msg(0, 3, model.VoteBatch{PollID: "p1", WorkerID: "w2", Done: true}))

	// До ends_at+grace — flush не должен происходить, даже с кворумом.
	h.cons.checkAllReady(ctx)
	if h.results.flushCount() != 0 {
		t.Fatalf("flush before ends_at, count=%d", h.results.flushCount())
	}

	// Переводим время за ends_at.
	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	if h.results.flushCount() != 1 {
		t.Fatalf("flush count = %d, want 1", h.results.flushCount())
	}
	fc, _ := h.results.lastFlush()
	if fc.pollID != "p1" {
		t.Errorf("flushed poll = %q, want p1", fc.pollID)
	}
	if fc.results.Total != 2 {
		t.Errorf("total = %d, want 2", fc.results.Total)
	}
	if fc.results.WorkersSeen != 2 || fc.results.WorkersDone != 2 {
		t.Errorf("workers seen/done = %d/%d, want 2/2", fc.results.WorkersSeen, fc.results.WorkersDone)
	}

	// Коммит должен покрыть все 4 сообщения.
	offs := h.reader.committedOffsets()
	if len(offs) != 4 {
		t.Errorf("committed %d messages, want 4 (%v)", len(offs), offs)
	}
}

func TestFlush_NotBeforeAllDone(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: 10 * time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	ctx := context.Background()

	// Голоса от 3 воркеров.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, votes("p1", "w2", "fp2", "1")))
	h.cons.handleMessage(ctx, msg(0, 2, votes("p1", "w3", "fp3", "2")))
	// "done" только от одного (33% < 90%).
	h.cons.handleMessage(ctx, msg(0, 3, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))

	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	if h.results.flushCount() != 0 {
		t.Fatalf("flush happened without quorum, count=%d", h.results.flushCount())
	}

	// Ещё один done → уже 2 из 3 = 66% — всё ещё мало.
	h.cons.handleMessage(ctx, msg(0, 4, model.VoteBatch{PollID: "p1", WorkerID: "w2", Done: true}))
	h.cons.checkAllReady(ctx)
	if h.results.flushCount() != 0 {
		t.Fatalf("flush happened at 66%%, count=%d", h.results.flushCount())
	}

	// Третий done → 100% ≥ 90% → flush.
	h.cons.handleMessage(ctx, msg(0, 5, model.VoteBatch{PollID: "p1", WorkerID: "w3", Done: true}))
	h.cons.checkAllReady(ctx)
	if h.results.flushCount() != 1 {
		t.Fatalf("flush count = %d, want 1 after full quorum", h.results.flushCount())
	}
}

func TestFlush_OnStale(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, StaleMessages: 3, CloseGrace: 0, CloseHardTmo: time.Hour})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	h.addPoll("p2", h.clock.now().Add(time.Hour))
	ctx := context.Background()

	// Один голос по p1, никто не прислал done.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.clock.advance(90 * time.Second)

	// Три сообщения по p2 → p1 «остывает».
	for i := 0; i < 3; i++ {
		h.cons.handleMessage(ctx, msg(0, int64(i+1), votes("p2", "w9", fmt.Sprintf("x%d", i), "1")))
	}
	// Условие «остывания» проверяется периодическим проходом (checkLoop).
	h.cons.checkAllReady(ctx)

	if h.results.flushCount() != 1 {
		t.Fatalf("flush count = %d, want 1 (stale)", h.results.flushCount())
	}
	fc, _ := h.results.lastFlush()
	if fc.pollID != "p1" {
		t.Errorf("flushed %q, want p1", fc.pollID)
	}
}

func TestFlush_OnHardTimeout(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: 30 * time.Second})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	ctx := context.Background()

	// Голос от воркера, который "упал" и не прислал done.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))

	// Прошло much more чем hard timeout.
	h.clock.advance(5 * time.Minute)
	h.cons.checkAllReady(ctx)

	if h.results.flushCount() != 1 {
		t.Fatalf("flush count = %d, want 1 (hard timeout)", h.results.flushCount())
	}
}

func TestFlush_IdempotentOn409(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	h.results.alreadyWritten = true // сервис отвечает 409
	ctx := context.Background()

	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))
	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	// 409 — тоже успех: коммитим offset.
	offs := h.reader.committedOffsets()
	if len(offs) != 2 {
		t.Fatalf("committed %d, want 2 (%v)", len(offs), offs)
	}
	// После коммита состояние опроса очищается из памяти.
	if _, ok := h.cons.Snapshot("p1"); ok {
		t.Error("poll state should be cleaned up after commit")
	}
}

func TestFlush_NoCommitOnError(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	h.results.flushErr = fmt.Errorf("results down") // постоянная ошибка
	ctx := context.Background()

	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))
	h.clock.advance(90 * time.Second)

	// checkPollReady сделает retry с backoff — ограничим контекст.
	ctxTmo, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	h.cons.checkAllReady(ctxTmo)

	if offs := h.reader.committedOffsets(); len(offs) != 0 {
		t.Fatalf("must not commit after flush error, committed %v", offs)
	}
	snap, _ := h.cons.Snapshot("p1")
	if snap.Committed {
		t.Error("poll must not be marked committed after error")
	}
	if snap.Flushed {
		t.Error("flushed flag must be rolled back after error")
	}
}

func TestContiguousCommit_MultiplePollsPerPartition(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, StaleMessages: 1000, CloseGrace: 0, CloseHardTmo: time.Hour})
	// p1 завершится, p2 — нет.
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	h.addPoll("p2", h.clock.now().Add(time.Hour))
	ctx := context.Background()

	// offset 0: p1 голос; offset 1: p2 голос; offset 2: p1 done.
	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, votes("p2", "w1", "fp2", "1")))
	h.cons.handleMessage(ctx, msg(0, 2, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))

	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	// p1 готов, но p2 (offset 1) не завершён, а offset 0 закоммитить нельзя
	// в отрыве от 1... на самом деле offset 0 < 1 и p1 завершён, поэтому
	// коммит префикса возможен только если голова очереди (offset 0)
	// принадлежит завершённому опросу.
	//
	// Проверяем ключевое свойство: offset 1 (p2 не завершён) НЕ закоммичен.
	offs := h.reader.committedOffsets()
	for _, o := range offs {
		if o > 0 {
			t.Errorf("committed offset %d, but p2 (offset 1) is not finished", o)
		}
	}

	// Завершаем p2 — теперь должен закоммититься весь префикс.
	h.clock.advance(20 * time.Hour) // p2 endsAt + grace пройден, hard тоже
	h.cons.checkAllReady(ctx)

	offs = h.reader.committedOffsets()
	seen := map[int64]bool{}
	for _, o := range offs {
		seen[o] = true
	}
	if !seen[0] || !seen[1] || !seen[2] {
		t.Errorf("expected all offsets 0,1,2 committed, got %v", offs)
	}
}

func TestMalformedMessage_DoesNotBlockCommit(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: time.Minute})
	h.addPoll("p1", h.clock.now().Add(time.Minute))
	ctx := context.Background()

	// offset 0 — битый JSON; offset 1 — нормальный голос; offset 2 — done.
	bad := kafka.Message{Topic: "votes", Partition: 0, Offset: 0, Value: []byte("{not json")}
	h.cons.handleMessage(ctx, bad)
	h.cons.handleMessage(ctx, msg(0, 1, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 2, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))

	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	offs := h.reader.committedOffsets()
	seen := map[int64]bool{}
	for _, o := range offs {
		seen[o] = true
	}
	if !seen[0] {
		t.Error("malformed message (offset 0) must be committed/skipped")
	}
}

func TestEndsAtLoadedFromResults(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: time.Second, CloseHardTmo: time.Hour})
	endsAt := h.clock.now().Add(45 * time.Second)
	h.addPoll("p1", endsAt)
	ctx := context.Background()

	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))

	// До ends_at — не готов.
	h.cons.checkAllReady(ctx)
	if h.results.flushCount() != 0 {
		t.Fatal("must not flush before ends_at")
	}

	// После ends_at + grace + done — готов.
	h.clock.advance(50 * time.Second)
	h.cons.handleMessage(ctx, msg(0, 1, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))
	h.cons.checkAllReady(ctx)

	if h.results.flushCount() != 1 {
		t.Fatalf("flush count = %d, want 1", h.results.flushCount())
	}
}

func TestRun_CommitsAndCleansUp(t *testing.T) {
	h := newHarness(t, Config{DonePercent: 90, CloseGrace: 0, CloseHardTmo: time.Minute})
	h.addPoll("p1", h.clock.now().Add(-time.Second)) // уже завершён
	ctx := context.Background()

	h.cons.handleMessage(ctx, msg(0, 0, votes("p1", "w1", "fp1", "1")))
	h.cons.handleMessage(ctx, msg(0, 1, model.VoteBatch{PollID: "p1", WorkerID: "w1", Done: true}))
	h.clock.advance(90 * time.Second)
	h.cons.checkAllReady(ctx)

	if _, ok := h.cons.Snapshot("p1"); ok {
		t.Error("poll state should be cleaned up after commit")
	}
}

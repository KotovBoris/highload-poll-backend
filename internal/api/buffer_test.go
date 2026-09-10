package api

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// recordingSender собирает всё, что буфер отправил, в порядке отправки.
type recordingSender struct {
	mu      sync.Mutex
	batches [][]model.Vote
	doneSeq int // порядковый номер отправки "done" (0 = ещё не было)
	seq     int
}

func (r *recordingSender) send(_ string, votes []model.Vote, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	if done {
		r.doneSeq = r.seq
		return
	}
	cp := make([]model.Vote, len(votes))
	copy(cp, votes)
	r.batches = append(r.batches, cp)
}

func (r *recordingSender) totalVotes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, b := range r.batches {
		n += len(b)
	}
	return n
}

func (r *recordingSender) donePos() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.doneSeq
}

func (r *recordingSender) lastSeq() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// TestVoteBuffer_ConcurrentAddAndClose_NoLoss — регрессия на гонку, при которой
// голос, сформировавший полный батч, мог отправиться ПОСЛЕ "done" и потеряться.
// Проверяем: суммарно отправлено ровно столько голосов, сколько принято, а
// "done" всегда идёт последним.
func TestVoteBuffer_ConcurrentAddAndClose_NoLoss(t *testing.T) {
	const (
		batchSize = 8
		adders    = 8
		perAdder  = 500
	)

	rec := &recordingSender{}
	buf := newVoteBuffer(batchSize, rec.send)

	var accepted atomic.Int64

	// Гарантируем, что часть голосов принята ДО гонки: иначе Close может
	// выиграть раньше первого Add, и тест выродится (все голоса отклонены).
	const warmup = batchSize * 2
	for i := 0; i < warmup; i++ {
		if !buf.Add("p1", model.Vote{Fingerprint: "warmup", OptionID: 1}) {
			t.Fatal("warmup add rejected")
		}
	}
	accepted.Add(warmup)

	start := make(chan struct{})

	var wg sync.WaitGroup
	for a := 0; a < adders; a++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			<-start
			for i := 0; i < perAdder; i++ {
				if buf.Add("p1", model.Vote{Fingerprint: "f", OptionID: 1}) {
					accepted.Add(1)
				}
			}
		}(a)
	}

	// Одновременно закрываем опрос — это и есть гонка.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		buf.Close("p1", time.Now())
	}()

	close(start)
	wg.Wait()

	// Даём drainer'у доразобрать очередь.
	deadline := time.Now().Add(2 * time.Second)
	for rec.donePos() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// После "done" в очереди уже ничего быть не должно.
	time.Sleep(50 * time.Millisecond)

	if got, want := int64(rec.totalVotes()), accepted.Load(); got != want {
		t.Fatalf("sent %d votes, but %d were accepted (LOST %d)", got, want, want-got)
	}
	if rec.donePos() == 0 {
		t.Fatal("done was never sent")
	}
	if rec.donePos() != rec.lastSeq() {
		t.Fatalf("done was not last: done=%d, total=%d (batch sent after done)",
			rec.donePos(), rec.lastSeq())
	}
}

// TestVoteBuffer_AddAfterCloseRejected — после Close новые голоса не принимаются.
func TestVoteBuffer_AddAfterCloseRejected(t *testing.T) {
	rec := &recordingSender{}
	buf := newVoteBuffer(100, rec.send)

	if !buf.Add("p1", model.Vote{Fingerprint: "a", OptionID: 1}) {
		t.Fatal("first vote must be accepted")
	}
	if buf.Close("p1", time.Now()) {
		t.Fatal("first Close must report not-already-closed")
	}
	if buf.Add("p1", model.Vote{Fingerprint: "b", OptionID: 1}) {
		t.Fatal("vote after close must be rejected")
	}
	if !buf.Close("p1", time.Now()) {
		t.Fatal("second Close must report already-closed (idempotent)")
	}
}

// TestVoteBuffer_RemainderBeforeDone — остаток идёт перед "done".
func TestVoteBuffer_RemainderBeforeDone(t *testing.T) {
	rec := &recordingSender{}
	buf := newVoteBuffer(100, rec.send)

	buf.Add("p1", model.Vote{Fingerprint: "a", OptionID: 1})
	buf.Add("p1", model.Vote{Fingerprint: "b", OptionID: 2})
	buf.Close("p1", time.Now())

	deadline := time.Now().Add(2 * time.Second)
	for rec.donePos() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rec.totalVotes() != 2 {
		t.Fatalf("remainder = %d, want 2", rec.totalVotes())
	}
	if rec.donePos() != rec.lastSeq() {
		t.Fatalf("done not last: done=%d total=%d", rec.donePos(), rec.lastSeq())
	}
}

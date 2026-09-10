package api

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/kafka"
	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// Sender — асинхронный отправитель батчей в Kafka.
//
// Зритель получает ответ ДО записи в Kafka: батч кладётся в очередь, а
// фоновые воркеры отправляют его с бесконечным retry (backoff). Так сбой
// Kafka не влияет на latency hot path.
//
// Очередь шардируется по poll_id (FNV-хеш → номер воркера), поэтому все батчи
// одного опроса обрабатываются ОДНИМ воркером и уходят в Kafka в порядке
// постановки. Это критично: "done" не должен обогнать остаток голосов, иначе
// consumer завершит опрос, недосчитав голоса. Параллелизм между разными
// опросами при этом сохраняется.
type Sender struct {
	producer kafka.Producer
	shards   []chan model.VoteBatch
	workers  int
	logger   *slog.Logger

	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex

	produced atomic.Int64
	failed   atomic.Int64
}

// NewSender создаёт отправитель с указанным числом воркеров-шардов.
func NewSender(producer kafka.Producer, queueSize, workers int, logger *slog.Logger) *Sender {
	if workers <= 0 {
		workers = 4
	}
	if queueSize <= 0 {
		queueSize = 4096
	}
	if logger == nil {
		logger = slog.Default()
	}
	// queueSize делится между шардами, минимум 1 на шард.
	perShard := queueSize / workers
	if perShard < 1 {
		perShard = 1
	}
	shards := make([]chan model.VoteBatch, workers)
	for i := range shards {
		shards[i] = make(chan model.VoteBatch, perShard)
	}
	return &Sender{
		producer: producer,
		shards:   shards,
		workers:  workers,
		logger:   logger,
	}
}

// shardFor выбирает шард по poll_id: батчи одного опроса всегда попадают в
// один шард и, значит, отправляются последовательно.
func (s *Sender) shardFor(pollID string) chan model.VoteBatch {
	h := fnv.New32a()
	_, _ = h.Write([]byte(pollID))
	return s.shards[int(h.Sum32())%s.workers]
}

// Start запускает фоновые воркеры, привязанные к контексту.
func (s *Sender) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	for i := range s.shards {
		ch := s.shards[i]
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.run(ctx, ch)
		}()
	}
}

// run обрабатывает один шард, пока не отменён контекст. Перед выходом
// best-effort досылает то, что осталось в очереди (одна попытка без retry).
func (s *Sender) run(ctx context.Context, ch chan model.VoteBatch) {
	for {
		select {
		case <-ctx.Done():
			s.drain(ch)
			return
		case b := <-ch:
			s.produceWithRetry(ctx, b)
		}
	}
}

// produceWithRetry пишет батч, повторяя при ошибках до отмены контекста.
func (s *Sender) produceWithRetry(ctx context.Context, b model.VoteBatch) {
	backoff := 50 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		err := s.producer.Produce(ctx, b)
		if err == nil {
			s.produced.Add(1)
			return
		}
		s.failed.Add(1)
		s.logger.Warn("kafka produce failed, retrying",
			"poll_id", b.PollID, "worker_id", b.WorkerID,
			"done", b.Done, "votes", len(b.Votes), "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// drain пытается разослать оставшиеся в шарде батчи без retry.
func (s *Sender) drain(ch chan model.VoteBatch) {
	for {
		select {
		case b := <-ch:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := s.producer.Produce(ctx, b)
			cancel()
			if err == nil {
				s.produced.Add(1)
			} else {
				s.failed.Add(1)
				s.logger.Error("dropped batch on shutdown", "poll_id", b.PollID, "err", err)
			}
		default:
			return
		}
	}
}

// Enqueue ставит батч в шард соответствующего опроса. Блокируется, если шард
// полон: так мы не теряем "done"-сообщения и сохраняем порядок по опросу.
func (s *Sender) Enqueue(b model.VoteBatch) {
	s.shardFor(b.PollID) <- b
}

// EnqueueCtx как Enqueue, но с учётом контекста.
func (s *Sender) EnqueueCtx(ctx context.Context, b model.VoteBatch) error {
	select {
	case s.shardFor(b.PollID) <- b:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait дожидается завершения фоновых воркеров.
func (s *Sender) Wait() { s.wg.Wait() }

// Stats возвращает счётчики успешных/неуспешных записей (для тестов/мониторинга).
func (s *Sender) Stats() (produced, failed int64) {
	return s.produced.Load(), s.failed.Load()
}

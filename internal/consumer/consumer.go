// Package consumer реализует слой асинхронной дедупликации и агрегации голосов:
// читает батчи из Kafka, дедуплицирует по fingerprint in-memory, считает голоса,
// дожидается завершения опроса («done» от всех известных воркеров) и идемпотентно
// сбрасывает итоги в сервис результатов. Offset коммитится только после успешного
// flush и только непрерывным префиксом по партиции.
//
// См. specs/consumer.md — источник истины по контрактам.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/kafka"
	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
)

// Config — параметры consumer'а.
type Config struct {
	DonePercent   int           // порог Y: кворум "done", %
	StaleMessages int           // порог X: остывание по другим опросам
	CloseGrace    time.Duration // минимальное ожидание после ends_at
	CloseHardTmo  time.Duration // принудительный flush без кворума
}

// pendingMsg — сообщение, ожидающее возможности коммита, в порядке чтения.
type pendingMsg struct {
	partition int
	offset    int64
	pollID    string
	msg       kafka.Message
}

// pollState — агрегированное состояние опроса в памяти.
type pollState struct {
	pollID      string
	dedup       map[string]struct{}
	counts      map[int]int64
	workersSeen map[string]struct{}
	workersDone map[string]struct{}
	endsAt      time.Time
	endsAtKnown bool
	firstSeen   time.Time
	lastSeen    time.Time
	staleCount  int
	flushed     bool
	// committed — все сообщения этого опроса закоммичены.
	committed bool
}

// Consumer — основной обработчик.
type Consumer struct {
	reader kafka.Reader
	client resultsclient.Client
	cfg    Config
	logger *slog.Logger

	now func() time.Time

	mu    sync.Mutex
	polls map[string]*pollState
	// pending[partition] — очередь сообщений в порядке offset'ов.
	pending map[int][]pendingMsg
	closed  bool
}

// New создаёт consumer.
func New(reader kafka.Reader, client resultsclient.Client, cfg Config, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.DonePercent <= 0 {
		cfg.DonePercent = 90
	}
	if cfg.StaleMessages <= 0 {
		cfg.StaleMessages = 1000
	}
	if cfg.CloseGrace <= 0 {
		cfg.CloseGrace = 30 * time.Second
	}
	if cfg.CloseHardTmo <= 0 {
		cfg.CloseHardTmo = 5 * time.Minute
	}
	return &Consumer{
		reader:  reader,
		client:  client,
		cfg:     cfg,
		logger:  logger,
		now:     time.Now,
		polls:   make(map[string]*pollState),
		pending: make(map[int][]pendingMsg),
	}
}

// SetNow подменяет часы (для тестов).
func (c *Consumer) SetNow(now func() time.Time) { c.now = now }

// Run читает и обрабатывает сообщения до отмены контекста.
func (c *Consumer) Run(ctx context.Context, checkInterval time.Duration) error {
	if checkInterval <= 0 {
		checkInterval = time.Second
	}
	go c.checkLoop(ctx, checkInterval)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Warn("fetch message failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		c.handleMessage(ctx, msg)
	}
}

// checkLoop периодически проверяет условия завершения опросов (важно, когда
// новых сообщений нет, но ends_at уже прошёл).
func (c *Consumer) checkLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkAllReady(ctx)
		}
	}
}

// handleMessage обрабатывает одно Kafka-сообщение.
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) {
	var batch model.VoteBatch
	if err := json.Unmarshal(msg.Value, &batch); err != nil {
		c.logger.Error("bad kafka message, skipping", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		// Битое сообщение не должно блокировать коммит префикса: помечаем
		// как «готовое к коммиту» через пустой pollID.
		c.markCommittable(msg.Partition, msg.Offset, "")
		c.tryCommit(ctx, msg.Partition)
		return
	}

	now := c.now()

	c.mu.Lock()
	st := c.polls[batch.PollID]
	if st == nil {
		st = &pollState{
			pollID:      batch.PollID,
			dedup:       make(map[string]struct{}),
			counts:      make(map[int]int64),
			workersSeen: make(map[string]struct{}),
			workersDone: make(map[string]struct{}),
			firstSeen:   now,
			lastSeen:    now,
		}
		c.polls[batch.PollID] = st
	}
	// Регистрируем воркера и обрабатываем голоса.
	if batch.WorkerID != "" {
		st.workersSeen[batch.WorkerID] = struct{}{}
	}
	for _, v := range batch.Votes {
		if _, dup := st.dedup[v.Fingerprint]; dup {
			continue
		}
		st.dedup[v.Fingerprint] = struct{}{}
		st.counts[v.OptionID]++
	}
	if batch.Done && batch.WorkerID != "" {
		st.workersDone[batch.WorkerID] = struct{}{}
	}
	st.lastSeen = now
	// «Остывание»: считаем, сколько сообщений по другим опросам прошло подряд.
	for id, other := range c.polls {
		if id == batch.PollID {
			continue
		}
		if !other.flushed {
			other.staleCount++
		}
	}
	// Регистрируем сообщение как ожидающее коммита.
	c.pending[msg.Partition] = append(c.pending[msg.Partition], pendingMsg{
		partition: msg.Partition,
		offset:    msg.Offset,
		pollID:    batch.PollID,
		msg:       msg,
	})
	c.mu.Unlock()

	// Обеспечиваем известность ends_at (вне мьютекса — сетевой вызов).
	c.ensureEndsAt(ctx, batch.PollID)

	c.checkPollReady(ctx, batch.PollID)
	c.tryCommit(ctx, msg.Partition)
}

// ensureEndsAt однократно подтягивает ends_at из сервиса результатов.
func (c *Consumer) ensureEndsAt(ctx context.Context, pollID string) {
	c.mu.Lock()
	st := c.polls[pollID]
	if st == nil || st.endsAtKnown {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	poll, err := c.client.GetPoll(ctx, pollID)
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()
	st = c.polls[pollID]
	if st == nil {
		return
	}
	if err != nil {
		if _, nf := err.(resultsclient.ErrNotFound); !nf {
			// Сервис недоступен — позже повторим; но подстрахуемся дефолтом,
			// чтобы опрос не завис навсегда.
			if st.endsAt.IsZero() {
				st.endsAt = now.Add(c.cfg.CloseHardTmo)
			}
			return
		}
		// Опрос не найден — завершим по жёсткому таймауту.
		st.endsAt = now.Add(c.cfg.CloseHardTmo)
		st.endsAtKnown = true
		return
	}
	st.endsAt = poll.EndsAt
	st.endsAtKnown = true
}

// checkAllReady проверяет все опросы на готовность к flush.
func (c *Consumer) checkAllReady(ctx context.Context) {
	c.mu.Lock()
	ids := make([]string, 0, len(c.polls))
	for id, st := range c.polls {
		if !st.flushed {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()

	partitions := map[int]struct{}{}
	for _, id := range ids {
		c.ensureEndsAt(ctx, id)
		if c.checkPollReady(ctx, id) {
			c.mu.Lock()
			for p := range c.pending {
				partitions[p] = struct{}{}
			}
			c.mu.Unlock()
		}
	}
	for p := range partitions {
		c.tryCommit(ctx, p)
	}
}

// checkPollReady проверяет условия завершения конкретного опроса и, если готов,
// выполняет flush. Возвращает true, если опрос был завершён этим вызовом.
func (c *Consumer) checkPollReady(ctx context.Context, pollID string) bool {
	c.mu.Lock()
	st := c.polls[pollID]
	if st == nil || st.flushed || st.endsAt.IsZero() {
		c.mu.Unlock()
		return false
	}
	now := c.now()

	// До ends_at + grace ничего не делаем.
	if now.Before(st.endsAt.Add(c.cfg.CloseGrace)) {
		c.mu.Unlock()
		return false
	}

	hardDeadline := st.endsAt.Add(c.cfg.CloseHardTmo)
	hard := !now.Before(hardDeadline)

	// Кворум "done".
	quorumReady := false
	if len(st.workersSeen) > 0 {
		need := (len(st.workersSeen)*c.cfg.DonePercent + 99) / 100
		if need == 0 {
			need = 1
		}
		quorumReady = len(st.workersDone) >= need
	}
	// Остывание.
	staleReady := st.staleCount >= c.cfg.StaleMessages

	if !hard && !quorumReady && !staleReady {
		c.mu.Unlock()
		return false
	}

	results := buildResults(st)
	// Помечаем flushed заранее, чтобы параллельный вызов не сделал двойной flush.
	st.flushed = true
	c.mu.Unlock()

	reason := "quorum"
	if hard {
		reason = "hard_timeout"
	} else if staleReady && !quorumReady {
		reason = "stale"
	}

	if err := c.flush(ctx, pollID, results); err != nil {
		// Flush не удался — откатываем флаг, повторим позже.
		c.mu.Lock()
		if st2 := c.polls[pollID]; st2 != nil {
			st2.flushed = false
		}
		c.mu.Unlock()
		c.logger.Error("flush failed, will retry", "poll_id", pollID, "err", err)
		return false
	}

	c.mu.Lock()
	if st2 := c.polls[pollID]; st2 != nil {
		st2.committed = true
	}
	c.mu.Unlock()

	c.logger.Info("poll completed",
		"poll_id", pollID, "total", results.Total,
		"workers_seen", results.WorkersSeen, "workers_done", results.WorkersDone,
		"reason", reason)
	return true
}

// flush отправляет результаты в сервис результатов с retry.
func (c *Consumer) flush(ctx context.Context, pollID string, results model.Results) error {
	backoff := 100 * time.Millisecond
	const maxBackoff = 3 * time.Second
	for attempt := 1; ; attempt++ {
		written, err := c.client.FlushResults(ctx, pollID, results)
		if err == nil {
			if !written {
				// 409: уже завершён ранее (replay) — тоже успех.
				c.logger.Info("poll already completed (idempotent no-op)", "poll_id", pollID)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.logger.Warn("flush attempt failed", "poll_id", pollID, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// markCommittable помечает сообщение готовым к коммиту (pollID="" — битое
// сообщение, коммитим сразу). Вызывается под мьютексом самостоятельно.
func (c *Consumer) markCommittable(partition int, offset int64, pollID string) {
	c.mu.Lock()
	c.pending[partition] = append(c.pending[partition], pendingMsg{
		partition: partition,
		offset:    offset,
		pollID:    pollID,
	})
	c.mu.Unlock()
}

// tryCommit коммитит непрерывный префикс очереди партиции: пока голова очереди
// принадлежит завершённому опросу (или является битым сообщением), снимаем её.
func (c *Consumer) tryCommit(ctx context.Context, partition int) {
	c.mu.Lock()
	q := c.pending[partition]
	cut := 0
	for cut < len(q) {
		pm := q[cut]
		if pm.pollID == "" {
			cut++
			continue
		}
		st := c.polls[pm.pollID]
		if st != nil && st.committed {
			cut++
			continue
		}
		break
	}
	if cut == 0 {
		c.mu.Unlock()
		return
	}
	toCommit := make([]kafka.Message, 0, cut)
	for _, pm := range q[:cut] {
		toCommit = append(toCommit, pm.msg)
	}
	c.pending[partition] = q[cut:]
	c.mu.Unlock()

	if err := c.reader.CommitMessages(ctx, toCommit...); err != nil {
		c.logger.Warn("commit failed", "partition", partition, "err", err)
		return
	}
	// После коммита чистим состояние завершённых опросов, у которых больше нет
	// pending-сообщений в этой партиции.
	c.cleanupCommitted(partition)
}

// cleanupCommitted удаляет из памяти опросы, закоммиченные и не имеющие
// оставшихся pending-сообщений.
func (c *Consumer) cleanupCommitted(partition int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	inUse := map[string]struct{}{}
	for _, q := range c.pending {
		for _, pm := range q {
			if pm.pollID != "" {
				inUse[pm.pollID] = struct{}{}
			}
		}
	}
	for id, st := range c.polls {
		if st.committed {
			if _, ok := inUse[id]; !ok {
				delete(c.polls, id)
			}
		}
	}
}

// buildResults собирает итоговые результаты из состояния опроса.
// Вызывается под удерживаемым мьютексом.
func buildResults(st *pollState) model.Results {
	optionIDs := make([]int, 0, len(st.counts))
	for id := range st.counts {
		optionIDs = append(optionIDs, id)
	}
	sort.Ints(optionIDs)

	res := model.Results{
		Options:     make([]model.OptionResult, 0, len(optionIDs)),
		WorkersSeen: len(st.workersSeen),
		WorkersDone: len(st.workersDone),
	}
	for _, id := range optionIDs {
		res.Options = append(res.Options, model.OptionResult{OptionID: id, Count: st.counts[id]})
		res.Total += st.counts[id]
	}
	return res
}

// StateSnapshot — снимок состояния опроса (для healthz/тестов).
type StateSnapshot struct {
	PollID      string
	Total       int64
	Unique      int
	WorkersSeen int
	WorkersDone int
	Flushed     bool
	Committed   bool
	StaleCount  int
}

// Snapshot возвращает снимок состояния опроса или false, если его нет.
func (c *Consumer) Snapshot(pollID string) (StateSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.polls[pollID]
	if st == nil {
		return StateSnapshot{}, false
	}
	var total int64
	for _, n := range st.counts {
		total += n
	}
	return StateSnapshot{
		PollID:      pollID,
		Total:       total,
		Unique:      len(st.dedup),
		WorkersSeen: len(st.workersSeen),
		WorkersDone: len(st.workersDone),
		Flushed:     st.flushed,
		Committed:   st.committed,
		StaleCount:  st.staleCount,
	}, true
}

// Polls возвращает список активных (незакоммиченных) опросов.
func (c *Consumer) Polls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.polls))
	for id := range c.polls {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ErrClosed возвращается, когда consumer уже остановлен.
var ErrClosed = errors.New("consumer closed")

// Close освобождает ресурсы читателя.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.reader.Close()
}

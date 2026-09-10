package api

import (
	"context"
	"sync"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
)

// pollCache — кэш метаданных опросов с TTL и схлопыванием одновременных
// промахов (single-flight), чтобы не устраивать «thundering herd» на сервис
// результатов при пиковом RPS.
type pollCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	client  resultsclient.Client
	now     func() time.Time
}

type cacheEntry struct {
	poll      model.Poll
	err       error
	expiresAt time.Time
	inflight  bool
	ready     chan struct{}
}

func newPollCache(client resultsclient.Client, ttl time.Duration, now func() time.Time) *pollCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &pollCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
		client:  client,
		now:     now,
	}
}

// get возвращает метаданные опроса, по возможности из кэша.
func (c *pollCache) get(ctx context.Context, id string) (model.Poll, error) {
	for {
		c.mu.Lock()
		e := c.entries[id]
		if e != nil && !e.inflight && c.now().Before(e.expiresAt) {
			poll, err := e.poll, e.err
			c.mu.Unlock()
			return poll, err
		}
		if e != nil && e.inflight {
			ch := e.ready
			c.mu.Unlock()
			select {
			case <-ch:
				continue // результат уже записан — читаем на следующей итерации
			case <-ctx.Done():
				return model.Poll{}, ctx.Err()
			}
		}
		// Промах: становимся лидером и идём за данными.
		e = &cacheEntry{inflight: true, ready: make(chan struct{})}
		c.entries[id] = e
		c.mu.Unlock()

		poll, err := c.client.GetPoll(ctx, id)

		c.mu.Lock()
		e.poll = poll
		e.err = err
		if err == nil {
			e.expiresAt = c.now().Add(c.ttl)
		} else {
			// Ошибку кэшируем ненадолго, чтобы параллельные ожидающие не
			// начинали повторный шторм запросов.
			negative := c.ttl
			if negative > time.Second {
				negative = time.Second
			}
			e.expiresAt = c.now().Add(negative)
		}
		e.inflight = false
		close(e.ready)
		c.mu.Unlock()
		return poll, err
	}
}

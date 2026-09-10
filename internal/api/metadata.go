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
//
// RWMutex, а не Mutex: подавляющее большинство обращений — чтения готового
// значения (каждый голос и каждая загрузка страницы), запись происходит лишь
// при промахе. На десятках тысяч RPS это снимает contention между читателями.
type pollCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	client  resultsclient.Client
	now     func() time.Time
}

type cacheEntry struct {
	// poll неизменяем после заполнения — поэтому отдаём указатель без копий.
	poll *model.Poll
	// optionSet — предвычисленное множество допустимых option_id. Проверка
	// голоса становится O(1) вместо линейного сканирования слайса опций на
	// каждом запросе (опций немного, но запросов — десятки тысяч в секунду).
	optionSet map[int]struct{}
	err       error
	expiresAt time.Time
	inflight  bool
	ready     chan struct{}
}

// cachedPoll — то, что отдаётся наружу: метаданные и готовое множество опций.
type cachedPoll struct {
	Poll      *model.Poll
	OptionSet map[int]struct{}
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
//
// Возвращается указатель на неизменяемую структуру: копировать model.Poll
// (вместе со слайсом опций) на каждый запрос незачем. Вызывающий код не должен
// менять полученное значение.
func (c *pollCache) get(ctx context.Context, id string) (cachedPoll, error) {
	for {
		// Быстрый путь: готовое значение, RLock — читатели не мешают друг другу.
		c.mu.RLock()
		e := c.entries[id]
		if e != nil && !e.inflight && c.now().Before(e.expiresAt) {
			out := cachedPoll{Poll: e.poll, OptionSet: e.optionSet}
			err := e.err
			c.mu.RUnlock()
			return out, err
		}
		if e != nil && e.inflight {
			ch := e.ready
			c.mu.RUnlock()
			select {
			case <-ch:
				continue // результат уже записан — читаем на следующей итерации
			case <-ctx.Done():
				return cachedPoll{}, ctx.Err()
			}
		}
		c.mu.RUnlock()

		// Промах: становимся лидером и идём за данными.
		c.mu.Lock()
		// Перепроверяем под полной блокировкой: пока мы её ждали, лидер мог
		// уже заполнить запись (двойная проверка — иначе устроим лишний поход
		// в сервис результатов).
		if e = c.entries[id]; e != nil {
			if !e.inflight && c.now().Before(e.expiresAt) {
				out := cachedPoll{Poll: e.poll, OptionSet: e.optionSet}
				err := e.err
				c.mu.Unlock()
				return out, err
			}
			if e.inflight {
				ch := e.ready
				c.mu.Unlock()
				select {
				case <-ch:
					continue
				case <-ctx.Done():
					return cachedPoll{}, ctx.Err()
				}
			}
		}
		e = &cacheEntry{inflight: true, ready: make(chan struct{})}
		c.entries[id] = e
		c.mu.Unlock()

		poll, err := c.client.GetPoll(ctx, id)

		c.mu.Lock()
		if err == nil {
			e.poll = &poll
			e.optionSet = make(map[int]struct{}, len(poll.Options))
			for _, o := range poll.Options {
				e.optionSet[o.ID] = struct{}{}
			}
			e.expiresAt = c.now().Add(c.ttl)
		} else {
			e.err = err
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
		out := cachedPoll{Poll: e.poll, OptionSet: e.optionSet}
		resultErr := e.err
		c.mu.Unlock()
		return out, resultErr
	}
}

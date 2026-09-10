package api

import (
	"sync"
	"time"
)

// issueKey — ключ лимита: конкретный опрос + хеш пары (IP, User-Agent).
// UA хешируем, чтобы ключ был фиксированной длины.
type issueKey string

// issueLimiter — in-memory ограничитель выдачи cookie: для одной пары
// (poll_id, IP+UA) cookie выдаётся только один раз.
//
// Зачем: HMAC-подпись закрывает ПОДДЕЛКУ идентификатора, но не мешает набрать
// настоящих подписанных cookie пачкой, многократно дёргая GET /polls/{id}.
// Лимитер делает выдачу одноразовой на пару (опрос, клиент).
//
// Ограничения (осознанные):
//   - Лимит локальный для процесса. При N API-воркерах за round-robin
//     фактический лимит = N cookie на пару (каждый воркер ведёт свой учёт).
//     В проде это решается маршрутизацией на балансировщике по ключу
//     (consistent hashing по poll_id|IP|UA) — см. docs/architecture.
//   - Ротация IP/UA обходит лимит; от этого защищает только edge-слой.
//   - Записи живут до ends_at опроса, поэтому память самоочищается по TTL
//     и дополнительно ограничена maxEntries (FIFO-вытеснение).
type issueLimiter struct {
	mu      sync.Mutex
	entries map[issueKey]time.Time // ключ → момент истечения записи
	order   []issueKey             // порядок вставки для FIFO-вытеснения
	max     int
	now     func() time.Time
}

// newIssueLimiter создаёт лимитер. max <= 0 означает «без ограничения объёма»
// (записи всё равно удаляются по истечении).
func newIssueLimiter(max int, now func() time.Time) *issueLimiter {
	if now == nil {
		now = time.Now
	}
	return &issueLimiter{
		entries: make(map[issueKey]time.Time),
		max:     max,
		now:     now,
	}
}

// Allow возвращает true и регистрирует выдачу, если для ключа ещё не выдавали
// cookie (или прежняя запись истекла). expiresAt — до какого момента помнить
// запись (обычно ends_at опроса + запас).
func (l *issueLimiter) Allow(key issueKey, expiresAt time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	if exp, ok := l.entries[key]; ok && now.Before(exp) {
		return false // уже выдавали — повторная выдача запрещена
	}

	l.entries[key] = expiresAt
	l.order = append(l.order, key)
	l.evictLocked()
	return true
}

// evictLocked удаляет истёкшие записи и, при превышении max, вытесняет самые
// старые по порядку вставки. Вызывается под удерживаемым мьютексом.
func (l *issueLimiter) evictLocked() {
	now := l.now()

	// Сначала — истёкшие.
	if len(l.order) > 64 {
		kept := l.order[:0]
		for _, k := range l.order {
			if exp, ok := l.entries[k]; ok && now.Before(exp) {
				kept = append(kept, k)
				continue
			}
			delete(l.entries, k)
		}
		l.order = kept
	}

	// Затем — вытеснение по объёму.
	if l.max > 0 {
		for len(l.entries) > l.max && len(l.order) > 0 {
			oldest := l.order[0]
			l.order = l.order[1:]
			delete(l.entries, oldest)
		}
	}
}

// size возвращает текущее число записей (для тестов/метрик).
func (l *issueLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

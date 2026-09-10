package api

import (
	"sync"
	"time"
)

// issueKey — ключ лимита: конкретный опрос + IP клиента.
// User-Agent намеренно НЕ входит в ключ: это клиентский заголовок, его ротация
// бесплатна, и включение UA в ключ защиты обесценило бы лимит.
type issueKey string

// issueLimiter — in-memory ограничитель ВЫДАЧИ cookie: для одного IP в рамках
// опроса можно выдать не более maxCookies cookie.
//
// Зачем: HMAC-подпись закрывает ПОДДЕЛКУ идентификатора, но не мешает набрать
// настоящих подписанных cookie пачкой, многократно дёргая GET /polls/{id}.
// Лимит делает выдачу ограниченной на IP.
//
// Почему счётчик, а не «одна на IP»: за одним IP (особенно CGNAT мобильных
// операторов) легитимно находятся сотни устройств. Жёсткое «1 на IP» отсекало
// бы их всех, поэтому разрешаем до maxCookies выдач — этого хватает реальному
// NAT, но одиночный атакующий с одного IP упирается в тот же предел вместо
// миллионов голосов.
//
// Ограничения (осознанные):
//   - Лимит локальный для процесса: при N API-воркерах за round-robin
//     фактический лимит равен N × maxCookies. Это известная особенность текущей
//     реализации (воркеры stateless, общий стор лимита усложнил бы hot path).
//     Как обеспечить точный лимит при нескольких воркерах — открытый вопрос,
//     требующий отдельной проработки.
//   - Ротация IP обходит лимит; от этого защищает только edge-слой.
//   - Записи живут до ends_at опроса, поэтому память самоочищается по TTL и
//     дополнительно ограничена maxEntries (FIFO-вытеснение).
type issueLimiter struct {
	mu      sync.Mutex
	entries map[issueKey]*issueEntry
	order   []issueKey // порядок вставки для FIFO-вытеснения
	max     int
	now     func() time.Time
}

type issueEntry struct {
	count     int
	expiresAt time.Time
}

// newIssueLimiter создаёт лимитер. max <= 0 означает «без ограничения объёма»
// записей (они всё равно удаляются по истечении).
func newIssueLimiter(max int, now func() time.Time) *issueLimiter {
	if now == nil {
		now = time.Now
	}
	return &issueLimiter{
		entries: make(map[issueKey]*issueEntry),
		max:     max,
		now:     now,
	}
}

// Allow пытается зарегистрировать выдачу cookie для ключа.
// Возвращает true, если выдача разрешена (и учитывает её), и false, если
// исчерпан лимит limit выдач для этого ключа.
// expiresAt — до какого момента (обычно ends_at опроса + запас) помнить запись.
func (l *issueLimiter) Allow(key issueKey, limit int, expiresAt time.Time) bool {
	if limit <= 0 {
		return true // лимит отключён
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	e, ok := l.entries[key]
	if !ok || now.After(e.expiresAt) {
		// Новая или истёкшая запись — начинаем счёт заново.
		l.entries[key] = &issueEntry{count: 1, expiresAt: expiresAt}
		l.order = append(l.order, key)
		l.evictLocked()
		return true
	}
	if e.count >= limit {
		// Продлеваем срок жизни записи, чтобы серия запросов не «сбросила»
		// счётчик по истечении посреди опроса.
		e.expiresAt = expiresAt
		return false
	}
	e.count++
	e.expiresAt = expiresAt
	return true
}

// evictLocked удаляет истёкшие записи и, при превышении max, вытесняет самые
// старые по порядку вставки. Вызывается под удерживаемым мьютексом.
func (l *issueLimiter) evictLocked() {
	now := l.now()

	if len(l.order) > 64 {
		kept := l.order[:0]
		for _, k := range l.order {
			if e, ok := l.entries[k]; ok && now.Before(e.expiresAt) {
				kept = append(kept, k)
				continue
			}
			delete(l.entries, k)
		}
		l.order = kept
	}

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

// count возвращает число учтённых выдач для ключа (для тестов).
func (l *issueLimiter) count(key issueKey) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[key]; ok {
		return e.count
	}
	return 0
}

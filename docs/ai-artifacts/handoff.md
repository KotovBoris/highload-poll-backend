# Контекст для следующего агента

## Что это за проект

Тестовое задание на Go-бэкендера: сервис для проведения минутных опросов на
ТВ-каналах. ~100M зрителей, ~1 минута голосования, пиковый RPS ~3.3M
(оптимистичная оценка с запасом).

**Репозиторий:** https://github.com/KotovBoris/highload-poll-backend
**Локально:** `/Users/b.kotov/personal/highload-poll-backend`
**gh:** `/opt/homebrew/bin/gh` (не в PATH, вызывать по полному пути)
**Аккаунт GitHub:** KotovBoris (SSH)

## Текущее состояние

Архитектура спроектирована и зафиксирована в документах. **Кода ещё нет.**
Нужно переходить к реализации.

## Документы архитектуры (читать в порядке)

1. [`docs/architecture/01-load-estimation.md`](../architecture/01-load-estimation.md) — оценка нагрузки (оптимистичная: p=5%, k=4, M=10, ~3.3M RPS)
2. [`docs/architecture/02-product-assumptions.md`](../architecture/02-product-assumptions.md) — продуктовые допущения (результаты не публичные, не real-time, асинхронная дедупликация, durability не критична)
3. [`docs/architecture/03-deduplication.md`](../architecture/03-deduplication.md) — fingerprint: cookie primary + IP+UA fallback
4. [`docs/architecture/04-architecture.md`](../architecture/04-architecture.md) — **итоговая архитектура + история решений (главный документ)**
5. [`docs/architecture/05-layer-load.md`](../architecture/05-layer-load.md) — RPS/MB/RAM/CPU по слоям

## Архитектура (кратко)

```
Зритель → API-воркеры (Go) → Kafka → Consumer'ы (Go) → Сервис результатов (Go + PostgreSQL)
```

### Компоненты

| Компонент | Технология | Роль |
|---|---|---|
| API-воркеры | Go (net/http) | Приём голосов, выдача cookie, буфер 10K, produce в Kafka |
| Kafka | Kafka (KRaft) | Буфер пика, durability, key=poll_id |
| Consumer'ы | Go | Локальная дедупликация in-memory, подсчёт, flush результатов |
| Сервис результатов | Go + PostgreSQL | Метаданные опросов, итоговые результаты, админка |

### Ключевые решения

1. **Батчинг:** API-воркеры накапливают 10K голосов на опрос, затем batch produce в Kafka.
2. **Kafka-сообщение = батч голосов** (10K, ~370KB), poll_id один раз. Не одно сообщение = один голос.
3. **Дедупликация асинхронная** — в consumer'е, не на hot path.
4. **Локальная дедупликация в consumer'е** (in-memory map), без Redis. key=poll_id → одна партиция → один consumer.
5. **Consumer не коммитит offset до завершения опроса.** При падении — полный replay, идемпотентный flush.
6. **Идемпотентный flush:** `WHERE status != 'completed'` — двойного счёта нет.
7. **Fingerprint:** cookie (как есть, UUID) primary + hash(IP+UA) fallback.
8. **Завершение опроса:** worker_id в каждом сообщении → consumer собирает множество worker_id → после ends_at ждёт "done" от каждого → порог коммита: ≥Y% "done" ИЛИ X сообщений по другим poll_id.
9. **При недоступности Kafka:** API-воркер retry до посинения (уже ответил зрителю OK). Минус: можем потерять голоса.
10. **Kafka consumer настройки:** `max.poll.interval.ms` = 600000 (10 мин), `session.timeout.ms` = 60000.

### API (предварительно)

- `GET /polls/{id}` — страница опроса, выдача cookie
- `POST /polls/{id}/vote` — принять голос
- `POST /admin/polls` — создать опрос (админка)
- `GET /admin/polls/{id}/results` — посмотреть результаты (админка)

### Kafka

- Топик: `votes`
- key: `poll_id`
- Одно сообщение = батч голосов (JSON): `{"poll_id":"...","worker_id":"...","votes":[{"fingerprint":"...","option_id":N},...]}`
- "done" сообщение: `{"poll_id":"...","worker_id":"...","done":true}`
- Компрессия: LZ4
- Retention: 5 мин

### PostgreSQL

- Таблица `polls`: id, question, options (JSON), status, results (JSON), created_at, ends_at
- Идемпотентный flush: `UPDATE polls SET status='completed', results=$1 WHERE id=$2 AND status!='completed'`

## Что нужно сделать (реализация)

1. **Структура Go-проекта** (monorepo или модули):
   - `cmd/api/` — API-воркеры
   - `cmd/consumer/` — Consumer'ы
   - `cmd/results/` — Сервис результатов (админка)
   - `internal/` — общие пакеты

2. **Docker Compose** для локального запуска: Kafka (KRaft) + PostgreSQL + все сервисы.

3. **API-воркер:**
   - HTTP-сервер (net/http или chi/gin)
   - Выдача cookie (UUID)
   - In-memory буфер 10K голосов на poll_id
   - Kafka producer (segmentio/kafka-go или sarama)
   - После ends_at: 403 + дослать "done"

4. **Consumer:**
   - Kafka consumer (consumer group)
   - In-memory map[fingerprint]bool для дедупликации
   - In-memory счётчики
   - Сбор worker_id, ожидание "done"
   - Идемпотентный flush в сервис результатов
   - Commit offset после OK от сервиса

5. **Сервис результатов:**
   - HTTP-сервер (админка)
   - PostgreSQL (создание опросов, сохранение результатов)
   - `GET /polls/{id}` для consumer'а (получить ends_at)

6. **Тестирование:**
   - Нагрузочный тест (хотя бы базовый)
   - Unit-тесты ключевой логики

7. **README:** как запустить локально и протестировать.

## Лог работ

Ведётся в [`docs/ai-artifacts/work-log.md`](work-log.md) — добавляй туда шаги.

## Важные замечания

- Пользователь хочет видеть **обоснование архитектуры** в репозитории (уже есть в `04-architecture.md`).
- Все артефакты работы с ИИ — в `docs/ai-artifacts/`.
- Пользователь шарит за Go, можно не объяснять базовые вещи.
- Коммиты — на английском, с префиксом `docs(arch):`, `feat:`, `fix:` и т.д.
- Пользователь предпочитает обсуждать архитектурные решения **до** их записи в документ.

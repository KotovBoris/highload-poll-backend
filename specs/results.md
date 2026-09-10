# Спека: Сервис результатов (results)

> Источник истины по контрактам. Интеграционные тесты и реализация должны
> соответствовать этой спеке. Базовый документ —
> [`docs/architecture/04-architecture.md`](../docs/architecture/04-architecture.md).

## Назначение

Сервис результатов — «медленный» CRUD-слой: хранит метаданные опросов и итоговые
результаты в PostgreSQL, обслуживает админку и внутренние вызовы от API-воркера и
consumer'а.

Нагрузка (см. [`05-layer-load.md`](../docs/architecture/05-layer-load.md)):
flush результатов ~0.17 RPS, админка — редка. Это самый ненагруженный слой.

## Транспорт

- HTTP/1.1, `net/http`, JSON (`Content-Type: application/json; charset=utf-8`).
- Порт по умолчанию: **8081** (`HTTP_ADDR`, например `:8081`).

## Модель данных

Таблица `polls`:

| Колонка | Тип | Описание |
|---|---|---|
| `id` | `text PRIMARY KEY` | ID опроса (генерируется сервисом, UUIDv4) |
| `question` | `text NOT NULL` | Вопрос |
| `options` | `jsonb NOT NULL` | Массив `[{"id":1,"text":"..."}]` |
| `status` | `text NOT NULL` | `active` \| `completed` |
| `results` | `jsonb NULL` | Итоговые результаты (после flush) |
| `created_at` | `timestamptz NOT NULL` | Момент создания |
| `ends_at` | `timestamptz NOT NULL` | Момент окончания приёма голосов |

Индексы: PK по `id`; индекс по `status` (для диагностики).

## Публичные (админские) ручки

### `POST /admin/polls` — создать опрос

Тело запроса ([`model.CreatePollRequest`](../internal/model/model.go:1)):

```json
{
  "question": "Какой цвет вам нравится?",
  "options": ["Красный", "Зелёный", "Синий"],
  "duration_seconds": 60
}
```

- `options` — минимум 2 варианта; каждый — непустая строка. Дубликаты текстов
  допустимы (не проверяем).
- Варианты нумеруются автоинкрементно начиная с **1** (id 1..N).
- Длительность задаётся одним из способов:
  - `duration_seconds > 0` → `ends_at = now + duration_seconds`;
  - либо `ends_at` (RFC3339, абсолютное время) → используется как есть;
  - если не задано ни то, ни другое → `400 bad_request`.
  - если заданы оба → приоритет у `ends_at`.
- `ends_at` обязан быть в будущем, иначе `400 bad_request`.

Ответ: `201 Created`

```json
{
  "id": "3f2b...",
  "question": "Какой цвет вам нравится?",
  "options": [{"id": 1, "text": "Красный"}, {"id": 2, "text": "Зелёный"}, {"id": 3, "text": "Синий"}],
  "status": "active",
  "created_at": "2026-09-10T08:00:00Z",
  "ends_at": "2026-09-10T08:01:00Z"
}
```

Ошибки: `400 bad_request` (валидация, битый JSON, отсутствующие поля).

### `GET /admin/polls/{id}/results` — результаты опроса

Ответ: `200 OK`

```json
{
  "id": "3f2b...",
  "question": "...",
  "options": [{"id": 1, "text": "Красный"}],
  "status": "completed",
  "created_at": "...",
  "ends_at": "...",
  "results": {
    "total": 123456,
    "options": [{"option_id": 1, "count": 70000}, {"option_id": 2, "count": 53456}],
    "workers_seen": 5,
    "workers_done": 5
  }
}
```

- Если опрос ещё не завершён (`status = active`), `results` отсутствует
  (`omitempty`), но `200 OK` (админка сама решает, что показывать).
- Если опрос не найден → `404 not_found`.

## Внутренние ручки (для API-воркера и consumer'а)

### `GET /internal/polls/{id}` — метаданные опроса

Возвращает ту же структуру, что и админская ручка без результатов. Используется:

- API-воркером — чтобы узнать `ends_at`, `options` (валидация `option_id`),
  кэшируется на стороне воркера;
- consumer'ом — чтобы узнать `ends_at` для логики завершения опроса.

Ответ: `200 OK` с `model.Poll`. `404 not_found`, если опроса нет.

### `POST /internal/polls/{id}/results` — идемпотентный flush

Тело ([`model.FlushRequest`](../internal/model/model.go:1)):

```json
{ "results": { "total": 123456, "options": [{"option_id": 1, "count": 70000}], "workers_seen": 5, "workers_done": 5 } }
```

Поведение — **идемпотентное** (см. архитектуру, п.6):

```sql
UPDATE polls SET status = 'completed', results = $1
WHERE id = $2 AND status != 'completed';
```

- Если строка обновлена (опрос был `active`) → `200 OK`, тело `{"written": true}`.
- Если опрос уже `completed` → `409 Conflict`, тело `{"written": false}`.
  Это не ошибка: повторный flush после replay безопасен.
- Если опроса нет → `404 not_found`.

| Код | Тело | Смысл |
|---|---|---|
| 200 | `{"written": true}` | Результаты записаны этим вызовом |
| 409 | `{"written": false}` | Опрос уже завершён, запись не произведена (идемпотентный no-op) |
| 404 | `{"error":"not_found"}` | Опроса нет |

## Служебные ручки

### `GET /healthz`

- `200 OK`, тело `{"status":"ok"}`, если процесс жив.
- Проверка `SELECT 1` к БД: `200` при успехе, `503 unavailable` при недоступности БД.

## Формат ошибок

Единый для всех ручек ([`model.ErrorResponse`](../internal/model/model.go:1)):

```json
{ "error": "bad_request", "message": "options must contain at least 2 items" }
```

Коды: `bad_request`, `not_found`, `internal_error`, `unavailable`.

## Конфигурация (env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HTTP_ADDR` | `:8081` | Адрес HTTP-сервера |
| `DATABASE_URL` | — (обязательна) | DSN PostgreSQL, например `postgres://poll:poll@postgres:5432/poll?sslmode=disable` |
| `DB_MAX_CONNS` | `10` | Размер пула соединений |
| `SHUTDOWN_TIMEOUT` | `10s` | Таймаут graceful shutdown |

## Требования к корректности и производительности

- **Идемпотентность flush** — ключевое требование: повторный вызов с теми же
  данными не должен менять результат/двойной счёт.
- **Атомарность** — flush это один `UPDATE ... WHERE status != 'completed'`;
  опираемся на атомарность СУБД, без read-modify-write в приложении.
- **Latency** админских ручек и flush — не критична; целевой p99 < 100 ms.
- **Валидация** — весь вход валидируется до обращения к БД, ошибки — `400`.

## Зависимости и точки мокирования (для интеграционных тестов)

Реализация зависит от интерфейса хранилища:

```go
type Storage interface {
    CreatePoll(ctx context.Context, req model.CreatePollRequest, now time.Time) (model.Poll, error)
    GetPoll(ctx context.Context, id string) (model.Poll, error)
    FlushResults(ctx context.Context, id string, res model.Results) (written bool, err error)
    Ping(ctx context.Context) error
}
```

- В интеграционных тестах HTTP-хендлеры поднимаются на `httptest.Server` с
  **фейковым Storage** (in-memory), который фиксирует вызовы.
- Реальная реализация Storage — поверх `pgx/v5` (`internal/results/postgres.go`).

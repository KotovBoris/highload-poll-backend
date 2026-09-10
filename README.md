# highload-poll-backend

Бэкенд-сервис для проведения минутных опросов на ТВ-каналах.

## Постановка задачи

Мы — национальная компания по проведению опросов. Запускаем минутные ролики на
крупнейших ТВ-каналах, где показываем опрос (1 вопрос любого типа: multiple
choice, A/B и т.д.) и просим зрителей пройти по QR-коду / ссылке и проголосовать.

- Опросы **не требуют регистрации**.
- Предусмотрена **базовая дедупликация** голосов (достаточная против обычных,
  не технически подкованных пользователей; не претендует на защиту от
  целенаправленного обхода).
- Каждый ролик смотрит ~100 млн человек, длится около минуты, в течение которой
  зрители могут голосовать (пиковая нагрузка на голосование).

### Требования к сервису

- Анонимно проголосовать.
- Создать опрос (админка).
- Посмотреть обезличенные результаты опроса (админка).

## Архитектура

Проектная документация — в [`docs/architecture`](docs/architecture):

1. [`01-load-estimation.md`](docs/architecture/01-load-estimation.md) — оценка пиковой нагрузки
2. [`02-product-assumptions.md`](docs/architecture/02-product-assumptions.md) — продуктовые допущения
3. [`03-deduplication.md`](docs/architecture/03-deduplication.md) — стратегия fingerprint
4. [`04-architecture.md`](docs/architecture/04-architecture.md) — итоговая архитектура
5. [`05-layer-load.md`](docs/architecture/05-layer-load.md) — нагрузка по слоям

```
Зритель → API-воркеры (Go) → Kafka → Consumer'ы (Go) → Сервис результатов (Go + PostgreSQL)
```

| Компонент | Роль | Порт |
|---|---|---|
| **API-воркер** | Приём голосов, выдача cookie, буфер 10K, produce в Kafka | 8080 |
| **Kafka** | Буфер пика, durability, key = poll_id | 9092 |
| **Consumer** | Дедупликация in-memory, подсчёт, идемпотентный flush | 8082 (healthz) |
| **Results** | Метаданные опросов, результаты, админка (PostgreSQL) | 8081 |

### Ключевые решения

- **Батчинг:** API-воркер копит 10K голосов на опрос и шлёт одно Kafka-сообщение
  (~370KB). `key = poll_id` → одна партиция на опрос.
- **Асинхронная дедупликация:** дедуп по fingerprint — в consumer'е, не на hot path.
- **Локальная дедупликация:** `map[fingerprint]struct{}` в памяти consumer'а,
  без Redis (одна партиция → один consumer).
- **Offset не коммитится до завершения опроса**, flush идемпотентен
  (`WHERE status != 'completed'`) → при падении полный replay без двойного счёта.
- **Завершение опроса:** API-воркер шлёт «done» после `ends_at`; consumer ждёт
  «done» от известных воркеров, с порогом по кворуму/остыванию и жёстким таймаутом.
- **Fingerprint:** cookie `voter_id` (UUID) как primary, `sha256(IP|User-Agent)` —
  fallback.
- **Контигуальный коммит offset:** по партиции коммитится только непрерывный
  префикс завершённых опросов — гарантия отсутствия потери данных.

Спецификации контрактов: [`specs/api.md`](specs/api.md),
[`specs/consumer.md`](specs/consumer.md), [`specs/results.md`](specs/results.md).

## Стек

- **Go 1.26** — все сервисы (stdlib `net/http`, без веб-фреймворков)
- **Kafka (KRaft)** — `segmentio/kafka-go`
- **PostgreSQL 16** — `pgx/v5`
- **Docker Compose** — локальный запуск

## API

### Публичные (API-воркер, :8080)

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/polls/{id}` | Метаданные опроса, выдача cookie `voter_id` |
| `POST` | `/polls/{id}/vote` | Голос `{"option_id": N}` → `202`; `400`/`403`/`404` |
| `GET` | `/healthz` | Healthcheck |

### Админка (Results, :8081)

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/admin/polls` | Создать опрос |
| `GET` | `/admin/polls/{id}/results` | Результаты опроса |
| `GET` | `/healthz` | Healthcheck |

### Внутренние (Results, :8081)

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/internal/polls/{id}` | Метаданные + `ends_at` (для API и consumer) |
| `POST` | `/internal/polls/{id}/results` | Идемпотентный flush: `200 {"written":true}` / `409` |

## Запуск локально

Требуется Docker (Compose).

```bash
make up        # собрать и поднять весь стек
make ps        # статус контейнеров
make logs      # логи
make down      # остановить
make clean     # остановить и удалить тома
```

Проверка вручную:

```bash
# 1. Создать опрос на 60 секунд
curl -s -X POST http://localhost:8081/admin/polls \
  -H 'Content-Type: application/json' \
  -d '{"question":"Какой цвет?","options":["Красный","Зелёный"],"duration_seconds":60}' | jq .

# 2. Получить страницу опроса (выдаст cookie voter_id)
POLL_ID=<id из шага 1>
curl -s -c cookies.txt http://localhost:8080/polls/$POLL_ID | jq .

# 3. Проголосовать
curl -s -b cookies.txt -X POST http://localhost:8080/polls/$POLL_ID/vote \
  -H 'Content-Type: application/json' -d '{"option_id":1}' | jq .

# 4. Через ~60 сек + grace посмотреть результаты
curl -s http://localhost:8081/admin/polls/$POLL_ID/results | jq .
```

## Тестирование

### Интеграционные тесты (моки, без внешних сервисов)

Тесты поднимают HTTP-сервер в памяти и подменяют зависимости фейками, которые
фиксируют вызовы. Внешние сервисы (Kafka, PostgreSQL) не нужны.

```bash
make test              # все тесты с -race
make test-integration  # только интеграционные (api, results, consumer)
```

Покрытие по сервисам:

- **results** — создание/валидация опросов, идемпотентный flush (200/409),
  скрытие результатов во внутреннем API, healthz.
- **api** — выдача cookie, fingerprint (cookie/fallback), `202/400/403/404`,
  бэтчинг по 10K, «done» после закрытия ровно один раз, retry продюсера.
- **consumer** — дедупликация, кворум «done», остывание, жёсткий таймаут,
  идемпотентный flush при 409, отсутствие коммита при ошибке, контигуальный
  коммит нескольких опросов в одной партиции, битые сообщения.

### E2E в Docker

```bash
make up
KEEP=1 ./scripts/e2e.sh   # поднимет (если нужно), проведёт опрос и проверит всё
```

Скрипт проверяет полный сценарий: создание опроса → выдача cookie → 50 голосов →
отбрасывание дубликата → `400` на неверную опцию → завершение и flush →
`403` после закрытия.

### Нагрузочный тест

Нагрузчик написан на Go (не требует установки k6) и запускается против
поднятого стека:

```bash
make up
make loadtest
# или с параметрами:
go run ./loadtest -c 300 -d 15s -unique-ips 3000000 -options 4 -grace 45s
```

Флаги: `-c` (параллелизм), `-d` (длительность), `-rps` (ограничение RPS, 0 = max),
`-unique-ips` (пул уникальных fingerprint'ов), `-options`, `-grace`.

**Результат на MacBook (8 CPU, стек в Docker):**

| Метрика | Значение |
|---|---|
| Пропускная способность | ~12–16 тыс. RPS |
| Ошибки | 0% |
| Latency p50 / p90 / p99 | ~11 ms / ~18 ms / ~40 ms |
| Распределение голосов | равномерное (43747/44093/43921/44000) |
| Дедупликация | подтверждена (уникальные голоса = учтённые) |

Целевые 3.3M RPS достигаются горизонтальным масштабированием API-воркеров за
балансировщиком (stateless) и партиционированием Kafka — см.
[`05-layer-load.md`](docs/architecture/05-layer-load.md).

## Структура репозитория

```
cmd/
  api/         API-воркер
  consumer/    consumer голосов
  results/     сервис результатов
internal/
  api/         буфер, кэш метаданных (single-flight), sender, HTTP-хендлеры
  consumer/    дедупликация, завершение, контигуальный коммит
  results/     storage-интерфейс, HTTP-хендлеры, PostgreSQL, миграции
  kafka/       обёртки Producer/Reader над segmentio/kafka-go
  resultsclient/  HTTP-клиент к внутреннему API results
  fingerprint/ стратегия fingerprint
  model/       доменные структуры и форматы обмена
  config/      чтение env
  httpx/       JSON-хелперы и единый формат ошибок
  uuid/        генератор UUIDv4
specs/         спецификации контрактов сервисов
migrations/    SQL-миграции
loadtest/      нагрузочный тест (Go)
scripts/       e2e.sh
docs/          архитектура и артефакты работы с ИИ
```

## Конфигурация

Все сервисы читают env-переменные (см. [`.env.example`](.env.example)).
Основные:

| Переменная | По умолчанию | Сервис |
|---|---|---|
| `HTTP_ADDR` | `:8080` / `:8081` / `:8082` | api / results / consumer |
| `DATABASE_URL` | — | results |
| `KAFKA_BROKERS` | `kafka:9092` | api, consumer |
| `KAFKA_TOPIC` | `votes` | api, consumer |
| `VOTE_BATCH_SIZE` | `10000` | api |
| `POLL_CACHE_TTL` | `1m` | api |
| `COMMIT_DONE_PERCENT` | `90` | consumer |
| `CLOSE_GRACE` | `30s` / `5s` в compose | consumer |

## Артефакты работы с ИИ

Все артефакты работы с ИИ — в каталоге
[`docs/ai-artifacts`](docs/ai-artifacts) в соответствии с требованиями задания.

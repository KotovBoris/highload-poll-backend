#!/usr/bin/env bash
# ===== E2E-проверка стека в docker-compose =====
#
# Поднимает стек (если не поднят), проводит голосование через реальные сервисы,
# дожидается завершения опроса и проверяет, что результаты сохранены.
#
# Использование:
#   ./scripts/e2e.sh            # поднять и проверить
#   KEEP=1 ./scripts/e2e.sh     # не гасить стек после проверки

set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8081}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

log() { printf '\033[36m[e2e]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[e2e FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || fail "docker не найден"
command -v jq >/dev/null || fail "jq не найден (brew install jq)"

log "Поднимаю стек..."
docker compose up -d --build

log "Жду готовности API ($API_URL/healthz) и results ($ADMIN_URL/healthz)..."
for i in $(seq 1 120); do
  if curl -sf "$API_URL/healthz" >/dev/null 2>&1 && curl -sf "$ADMIN_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  [ "$i" = 120 ] && fail "сервисы не поднялись за 120 секунд"
  sleep 1
done
log "Сервисы готовы."

# --- 1. Создание опроса (60 сек) ---
log "Создаю опрос..."
CREATE=$(curl -sf -X POST "$ADMIN_URL/admin/polls" \
  -H 'Content-Type: application/json' \
  -d '{"question":"E2E test?","options":["A","B","C"],"duration_seconds":30}')
POLL_ID=$(echo "$CREATE" | jq -r '.id')
[ -n "$POLL_ID" ] && [ "$POLL_ID" != "null" ] || fail "не удалось создать опрос: $CREATE"
log "Опрос создан: $POLL_ID"

# --- 2. Получение cookie (страница опроса) ---
log "GET /polls/{id} — получаю cookie..."
POLL_JSON=$(curl -sf "$API_URL/polls/$POLL_ID" -c /tmp/e2e_cookie.txt)
echo "$POLL_JSON" | jq -e '.options | length == 3' >/dev/null || fail "ожидалось 3 опции"
grep -q voter_id /tmp/e2e_cookie.txt || fail "cookie voter_id не выдана"

# cookie должна быть подписанной: "<raw>.<hex-hmac>"
COOKIE_VAL=$(awk '/voter_id/{print $7}' /tmp/e2e_cookie.txt)
echo "$COOKIE_VAL" | grep -qE '^[0-9a-f-]+\.[0-9a-f]{64}$' \
  || fail "cookie не подписана (ожидался формат <uuid>.<hmac>): $COOKIE_VAL"
log "Cookie получена и подписана."

# --- 2b. Повторный GET без cookie => 429 (лимит выдачи) ---
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/polls/$POLL_ID")
[ "$CODE" = "429" ] || fail "повторная выдача cookie: ожидался 429, получен $CODE"
log "Лимит выдачи cookie работает (429)."

# --- 3. Голосование разными fingerprint'ами ---
log "Голосую (уникальные IP + cookie)..."
for i in $(seq 1 50); do
  curl -sf -o /dev/null -X POST "$API_URL/polls/$POLL_ID/vote" \
    -H 'Content-Type: application/json' \
    -H "X-Forwarded-For: 10.1.$((i / 256)).$((i % 256))" \
    -H "User-Agent: e2e-agent-$i" \
    -d "{\"option_id\":$(( (i % 3) + 1 ))}" || fail "голос $i не принят"
done
log "Отправлено 50 голосов."

# --- 3b. Повторный голос с ТЕМ ЖЕ fingerprint должен быть принят (async dedup) ---
curl -sf -o /dev/null -X POST "$API_URL/polls/$POLL_ID/vote" \
  -H 'Content-Type: application/json' \
  -H "X-Forwarded-For: 10.1.0.1" \
  -H "User-Agent: e2e-agent-1" \
  -d '{"option_id":1}'
log "Отправлен дубликат (будет отброшен consumer'ом)."

# --- 4. Невалидная опция => 400 ---
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API_URL/polls/$POLL_ID/vote" \
  -H 'Content-Type: application/json' -d '{"option_id":999}')
[ "$CODE" = "400" ] || fail "невалидная опция: ожидался 400, получен $CODE"
log "Валидация option_id работает (400)."

# --- 5. Ждём ends_at + закрытие + flush ---
log "Жду завершения опроса и flush результатов..."
RESULT=""
for i in $(seq 1 120); do
  RESULT=$(curl -sf "$ADMIN_URL/admin/polls/$POLL_ID/results" || true)
  STATUS=$(echo "$RESULT" | jq -r '.status' 2>/dev/null || echo "")
  if [ "$STATUS" = "completed" ]; then break; fi
  sleep 1
done
[ "$(echo "$RESULT" | jq -r '.status')" = "completed" ] || fail "опрос не завершился: $RESULT"

TOTAL=$(echo "$RESULT" | jq -r '.results.total')
log "Опрос завершён. Всего голосов (после дедупликации): $TOTAL"

# 50 уникальных + 1 дубликат (отброшен) => ровно 50.
[ "$TOTAL" = "50" ] || fail "ожидалось 50 голосов после дедупликации, получено $TOTAL"

# --- 6. Голос после закрытия => 403 ---
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API_URL/polls/$POLL_ID/vote" \
  -H 'Content-Type: application/json' -H "X-Forwarded-For: 10.9.9.9" -d '{"option_id":1}')
[ "$CODE" = "403" ] || fail "после закрытия: ожидался 403, получен $CODE"
log "Голосование после закрытия отклоняется (403)."

echo
log "Все проверки пройдены. Результаты:"
echo "$RESULT" | jq .

if [ "${KEEP:-0}" != "1" ]; then
  log "Останавливаю стек..."
  docker compose down
fi
log "OK"

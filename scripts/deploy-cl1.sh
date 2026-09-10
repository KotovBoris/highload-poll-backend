#!/usr/bin/env bash
# Сборка linux/amd64, доставка на стенд cl1 и перезапуск стека.
# Использование: ./scripts/deploy-cl1.sh [host]
set -euo pipefail

HOST="${1:-cl1}"
REMOTE_DIR="highload-poll-backend"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

log() { printf '\033[36m[deploy]\033[0m %s\n' "$*"; }

log "сборка linux/amd64..."
mkdir -p /tmp/transfer/bin
for svc in api consumer results; do
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "/tmp/transfer/bin/$svc" "./cmd/$svc"
done
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/transfer/bin/loadtest ./loadtest

log "доставка на $HOST..."
rsync -az /tmp/transfer/bin/ "$HOST:~/$REMOTE_DIR/bin/"
rsync -az docker-compose.server.yml Dockerfile.server "$HOST:~/$REMOTE_DIR/"

log "пересборка образов и перезапуск..."
ssh "$HOST" "cd ~/$REMOTE_DIR && for svc in api consumer results; do sudo docker build -q --build-arg SERVICE=\$svc -f Dockerfile.server -t hp-\$svc . >/dev/null; done && sudo docker compose -f docker-compose.server.yml -p hp up -d --force-recreate >/dev/null && sleep 10 && sudo docker ps --filter 'name=hp-' --format '{{.Names}} {{.Status}}'"

log "готово"

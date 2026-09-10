#!/usr/bin/env bash
# Запускается НА стенде внутри каталога проекта.
# Параметры: RUNS SECS CONC CLIENTS
set -uo pipefail

RUNS="${1:-3}"
SECS="${2:-12}"
CONC="${3:-400}"
CLIENTS="${4:-2}"
BIN="/home/centos/highload-poll-backend/bin"

for run in $(seq 1 "$RUNS"); do
  for n in $(seq 1 "$CLIENTS"); do
    sudo docker rm -f "lt$n" >/dev/null 2>&1
    sudo docker run --rm -d --name "lt$n" --network hp_default \
      -v "$BIN":/b alpine:3.20 /b/loadtest \
      -burst -poll-duration "${SECS}s" -c "$CONC" \
      -api-urls http://hp-api-1:8080,http://hp-api2-1:8080 \
      -admin-url http://hp-results-1:8081 \
      -cookie-secret dev-cookie-secret-change-me \
      -results-timeout 60s >/dev/null 2>&1
  done

  sleep $((SECS + 6))

  total=0
  for n in $(seq 1 "$CLIENTS"); do
    rps=$(sudo docker logs "lt$n" 2>&1 | grep 'средний RPS' | awk '{print $3}')
    rps=${rps:-0}
    echo "  client-$n: $rps rps"
    total=$(awk -v t="$total" -v r="$rps" 'BEGIN{printf "%.0f", t+r}')
    sudo docker rm -f "lt$n" >/dev/null 2>&1
  done
  echo "  ИТОГО: $total rps (clients=$CLIENTS, c=$CONC)"
  echo "---"
done

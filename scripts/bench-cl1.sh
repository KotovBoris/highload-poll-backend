#!/usr/bin/env bash
# Нагрузочный замер на стенде cl1.
#
# Важно: ОДИН нагрузчик упирается в себя (жрёт ~8 ядер при c=600) и даёт
# ~60K RPS — это потолок клиента, а не сервера. Чтобы измерить реальную
# пропускную способность, запускаем N независимых клиентов параллельно и
# суммируем их RPS (с двумя клиентами по c=400 получено ~73K суммарно).
#
# Использование: ./scripts/bench-cl1.sh [host] [runs] [seconds] [per-client-conc] [clients]
set -euo pipefail

HOST="${1:-cl1}"
RUNS="${2:-3}"
SECS="${3:-12}"
CONC="${4:-400}"
CLIENTS="${5:-2}"

for i in $(seq 1 "$RUNS"); do
  ssh "$HOST" "
    for n in \$(seq 1 $CLIENTS); do
      sudo docker run --rm -d --name lt\$n --network hp_default \
        -v ~/highload-poll-backend/bin:/b alpine:3.20 /b/loadtest \
        -burst -poll-duration ${SECS}s -c ${CONC} \
        -api-urls http://hp-api-1:8080,http://hp-api2-1:8080 \
        -admin-url http://hp-results-1:8081 \
        -cookie-secret dev-cookie-secret-change-me \
        -results-timeout 60s >/dev/null 2>&1
    done
    sleep $(( SECS + 8 ))
    total=0
    for n in \$(seq 1 $CLIENTS); do
      rps=\$(sudo docker logs lt\$n 2>&1 | grep 'средний RPS' | awk '{print \$3}')
      echo \"  client-\$n: \${rps} rps\"
      total=\$(awk -v t=\$total -v r=\${rps:-0} 'BEGIN{print t+r}')
      sudo docker rm -f lt\$n >/dev/null 2>&1
    done
    printf '  ИТОГО (сумма клиентов): %.0f rps\n' \$total
  "
  echo "---"
done

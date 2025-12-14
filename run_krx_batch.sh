#!/usr/bin/env bash
set -euo pipefail

DAYS="${1:-${DAYS:-3}}"
DAYS="${DAYS//$'\r'/}"

IS_EB=0
if [[ -f "/opt/elasticbeanstalk/deployment/env" && -d "/var/app/current" ]]; then
  IS_EB=1
fi

if [[ "$IS_EB" -eq 1 ]]; then
  APP_DIR="/var/app/current"
  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a
  PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1)"
  LOG="/var/log/web.stdout.log"
else
  APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PY="$(command -v python || command -v python3)"
  LOG="${APP_DIR}/web.stdout.local.log"
fi

cd "$APP_DIR"
echo "[$(date)] [Runner] KRX_BATCH start DAYS=$DAYS" >> "$LOG"
echo "[$(date)] [Runner] using PY=${PY}" >> "$LOG"

# ✅ KST 기준으로 (D-2, D-1, D0) 날짜 3개 생성 (오래된 것부터)
DATES=$("$PY" - <<PY
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
except Exception:
    today = datetime.now().date()

days = int("${DAYS}")
for i in range(days-1, -1, -1):   # 2,1,0
    print((today - timedelta(days=i)).isoformat())
PY
)

for date_str in $DATES; do
  echo "=======================================================" >> "$LOG"
  echo "📅 날짜: ${date_str} 데이터 수집 시작" >> "$LOG"
  echo "=======================================================" >> "$LOG"

  echo "   [1/3] 종목 리스트 수집 중..." >> "$LOG"
  # ✅ 여기: 너 기존에 쓰던 1번 collector 실행 줄 그대로 두기
  "$PY" iceage/src/collectors/krx_listing_collector.py "$date_str" >> "$LOG" 2>&1 || true

  echo "   [2/3] 지수(Index) 수집 중..." >> "$LOG"
  # ✅ 여기: 너 기존 2번 줄
  "$PY" iceage/src/collectors/krx_index_collector.py "$date_str" >> "$LOG" 2>&1 || true

  echo "   [3/3] 일별 시세(Prices) 수집 중..." >> "$LOG"
  # ✅ 여기: 너 기존 3번 줄 (파일명이 krx_prices_collector.py면 그걸로!)
  "$PY" iceage/src/collectors/krx_price_collector.py "$date_str" >> "$LOG" 2>&1 || true

  echo "   ✅ ${date_str} 완료. API 보호를 위해 3초 대기..." >> "$LOG"
  sleep 3
done

echo "🎉 모든 KRX 배치 작업 완료!" >> "$LOG"
echo "[$(date)] [Runner] KRX_BATCH done" >> "$LOG"

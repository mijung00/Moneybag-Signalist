#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

LOG_FILE="${ROOT_DIR}/web.stdout.local.log"
mkdir -p "$(dirname "$LOG_FILE")"
exec >>"$LOG_FILE" 2>&1

echo "[`date`] [Runner] KRX_BATCH start DAYS=${DAYS:-3}"

# 1) AWS(EB) 환경이면 EB env 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a
fi

# 2) 로컬이면 .env 로드(※ .env는 KEY=VALUE 형태로 “공백 없이” 또는 "따옴표" 필요)
if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  . "${ROOT_DIR}/.env"
  set +a
fi

# 3) 윈도우 콘솔(cp949)에서도 이모지/한글 출력 때문에 죽지 않게
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

# 4) 파이썬 선택(로컬 venv 우선)
if [ -n "${VIRTUAL_ENV:-}" ]; then
  if [ -x "${VIRTUAL_ENV}/Scripts/python.exe" ]; then
    PY="${VIRTUAL_ENV}/Scripts/python.exe"
  else
    PY="${VIRTUAL_ENV}/bin/python"
  fi
else
  PY="$(command -v python3 || command -v python)"
fi

echo "[`date`] [Runner] using PY=${PY}"

# 5) 날짜 3개(D, D-1, D-2) 생성(캘린더 기준)
DAYS="${DAYS:-3}"
DATE_LIST="$("$PY" - <<'PY'
from datetime import date, timedelta
import os
days=int(os.getenv("DAYS","3"))
today=date.today()
for i in range(days):
    print((today - timedelta(days=i)).isoformat())
PY
)"

for d in $DATE_LIST; do
  echo "======================================================="
  echo "📅 날짜: $d 데이터 수집 시작"
  echo "======================================================="

  echo "   [1/3] 종목 리스트 수집 중..."
  "$PY" -m iceage.src.collectors.krx_listing_collector "$d" || echo "[WARN] listing failed: $d"

  echo "   [2/3] 지수(Index) 수집 중..."
  "$PY" -m iceage.src.collectors.krx_index_collector "$d" || echo "[WARN] index failed: $d"

  echo "   [3/3] 일별 시세(Prices) 수집 중..."
  "$PY" -m iceage.src.collectors.krx_daily_price_collector "$d" || echo "[WARN] price failed: $d"

  echo "   ✅ $d 완료. API 보호를 위해 3초 대기..."
  sleep 3
done

echo "🎉 모든 KRX 배치 작업 완료!"
echo "[`date`] [Runner] KRX_BATCH done"

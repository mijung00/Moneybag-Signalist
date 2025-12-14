#!/usr/bin/env bash
set -euo pipefail

# EB면 /var/app/current, 로컬이면 스크립트 폴더
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
  LOG="/var/log/web.stdout.log"
else
  APP_DIR="$ROOT_DIR"
  LOG="$APP_DIR/web.stdout.local.log"
fi

cd "$APP_DIR"
mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
touch "$LOG" 2>/dev/null || true
exec >>"$LOG" 2>&1

echo "[$(date)] [Runner] KRX_BATCH start DAYS=${DAYS:-3}"

# 1) env 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f "$APP_DIR/.env" ]; then
  set -a; . "$APP_DIR/.env"; set +a
fi

# 2) Secrets Manager bootstrap
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-2}"

fetch_secret () {
  local sid="$1"
  aws secretsmanager get-secret-value --secret-id "$sid" --query SecretString --output text 2>/dev/null || true
}

extract_json_value () {
  # $1=key, $2=secret_string
  python3 - <<'PY' "$1" "$2"
import json, sys
k = sys.argv[1]
s = sys.argv[2]
try:
    if s.strip().startswith("{"):
        d = json.loads(s)
        v = d.get(k) or d.get("value") or ""
        print(v if v else s)
    else:
        print(s)
except Exception:
    print(s)
PY
}

ensure_secret_env () {
  local k="$1"
  local v="${!k-}"

  # 값이 이미 “실제 값”이면 패스
  if [[ -n "$v" && "$v" != arn:aws:secretsmanager* ]]; then
    return 0
  fi

  # 1) ARN이 있으면 ARN로 먼저 시도, 없으면 이름으로 시도
  local sid="$k"
  if [[ "$v" == arn:aws:secretsmanager* ]]; then sid="$v"; fi

  local s
  s="$(fetch_secret "$sid")"
  if [[ -z "$s" || "$s" == "None" ]]; then
    s="$(fetch_secret "$k")"
  fi

  if [[ -z "$s" || "$s" == "None" ]]; then
    echo "[WARN] secret not loaded: $k"
    return 0
  fi

  s="$(extract_json_value "$k" "$s")"
  export "$k=$s"
}

# 필요한 키들 로드 (KRX 포함)
for k in KRX_AUTH_KEY SLACK_WEBHOOK_URL DB_PASSWORD \
         OPENAI_API_KEY SENDGRID_API_KEY SERPAPI_KEY \
         BINANCE_API_KEY BINANCE_SECRET_KEY UPBIT_ACCESS_KEY UPBIT_SECRET_KEY \
         TELEGRAM_BOT_TOKEN_SIGNALIST TELEGRAM_BOT_TOKEN_MONEYBAG
do
  ensure_secret_env "$k"
done

# 3) 인코딩 안전장치
export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8

# 4) 파이썬 선택 (EB는 venv 우선)
PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1 || true)"
if [ -z "${PY:-}" ]; then
  PY="$(command -v python3 || command -v python)"
fi
echo "[$(date)] [Runner] using PY=$PY"

# 5) 날짜 생성 + 실행
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

  echo "   [1/3] 종목 리스트 수집"
  "$PY" -m iceage.src.collectors.krx_listing_collector "$d" || echo "[WARN] listing failed: $d"

  echo "   [2/3] 지수(Index) 수집"
  "$PY" -m iceage.src.collectors.krx_index_collector "$d" || echo "[WARN] index failed: $d"

  echo "   [3/3] 일별 시세(Prices) 수집"
  "$PY" -m iceage.src.collectors.krx_daily_price_collector "$d" || echo "[WARN] price failed: $d"

  echo "   ✅ $d 완료. 3초 대기..."
  sleep 3
done

echo "[$(date)] [Runner] KRX_BATCH done"

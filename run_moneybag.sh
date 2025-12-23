#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-morning}"
MODE="$(echo "$MODE" | tr -d '\r' | tr '[:upper:]' '[:lower:]')"

if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
  LOG="/var/log/web.stdout.log"
else
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
  LOG="$APP_DIR/web.stdout.local.log"
fi

cd "$APP_DIR"

# env 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f "$APP_DIR/.env" ]; then
  set -a; . "$APP_DIR/.env"; set +a
fi

# Secrets bootstrap
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-2}"

fetch_secret () {
  local sid="$1"
  aws secretsmanager get-secret-value --secret-id "$sid" --query SecretString --output text 2>/dev/null || true
}

extract_json_value () {
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
  if [[ -n "$v" && "$v" != arn:aws:secretsmanager* ]]; then return 0; fi

  local sid="$k"
  if [[ "$v" == arn:aws:secretsmanager* ]]; then sid="$v"; fi

  local s
  s="$(fetch_secret "$sid")"
  if [[ -z "$s" || "$s" == "None" ]]; then s="$(fetch_secret "$k")"; fi
  if [[ -z "$s" || "$s" == "None" ]]; then echo "[WARN] secret not loaded: $k"; return 0; fi

  s="$(extract_json_value "$k" "$s")"
  export "$k=$s"
}

for k in OPENAI_API_KEY SENDGRID_API_KEY SERPAPI_KEY \
         TELEGRAM_BOT_TOKEN_MONEYBAG SLACK_WEBHOOK_URL DB_PASSWORD
do
  ensure_secret_env "$k"
done

export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8

PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1 || true)"
if [ -z "${PY:-}" ]; then PY="$(command -v python3 || command -v python)"; fi

echo "[$(date)] [Runner] MONEYBAG mode=$MODE start"
echo "[$(date)] [Runner] using PY=$PY"

"$PY" -m moneybag.src.pipelines.daily_runner "$MODE"

echo "[$(date)] [Runner] MONEYBAG mode=$MODE done"

#!/usr/bin/env bash
set -e

ARG="${1:-}"
ARG="$(echo "$ARG" | tr -d '\r')"

if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
  LOG="/var/log/web.stdout.log"

  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a

  PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1)"
else
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
  LOG="$APP_DIR/web.stdout.local.log"
  if command -v python >/dev/null 2>&1; then PY="python"; else PY="python3"; fi
fi

export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8

cd "$APP_DIR"

echo "[$(date)] [Runner] ICEAGE start arg='$ARG'" >> "$LOG"
echo "[$(date)] [Runner] using PY=$PY" >> "$LOG"

if [ -n "$ARG" ]; then
  "$PY" -m iceage.src.pipelines.daily_runner "$ARG" >> "$LOG" 2>&1
else
  "$PY" -m iceage.src.pipelines.daily_runner >> "$LOG" 2>&1
fi

echo "[$(date)] [Runner] ICEAGE done" >> "$LOG"

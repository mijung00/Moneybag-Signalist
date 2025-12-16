#!/usr/bin/env bash
set -euo pipefail

# 1. 앱 경로 설정
if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
else
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

cd "$APP_DIR"

# 2. 환경변수 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f "$APP_DIR/.env" ]; then
  set -a; . "$APP_DIR/.env"; set +a
fi

# 3. 파이썬 실행기 찾기 (★여기가 중요합니다★)
# AWS Linux 2023 (최신) 및 Linux 2 (구버전) 모두 대응
if [ -f /var/app/venv/bin/python ]; then
    PY="/var/app/venv/bin/python"
elif [ -f /var/app/venv/*/bin/python ]; then
    PY="$(ls -1 /var/app/venv/*/bin/python | head -n 1)"
else
    PY="python3"
fi

echo "[$(date)] [Watchdog Manager] 시작합니다..."
echo "[$(date)] Using Python: $PY"

# 4. 왓치독 실행
exec "$PY" watchdogs.py
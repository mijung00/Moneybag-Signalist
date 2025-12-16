#!/usr/bin/env bash
set -euo pipefail

# 1. 경로 설정
if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
else
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

cd "$APP_DIR"

# 2. AWS 환경변수 강제 로드 (★이게 제일 중요합니다★)
# 이 과정이 없으면 파이썬이 os.getenv로 아무리 찾아도 키가 없습니다.
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f "$APP_DIR/.env" ]; then
  set -a; . "$APP_DIR/.env"; set +a
fi

# 3. 파이썬 실행기 찾기 (가상환경 우선)
PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1 || true)"
if [ -z "${PY:-}" ]; then PY="$(command -v python3 || command -v python)"; fi

echo "[$(date)] [Watchdog Manager] 시작합니다..."
echo "[$(date)] Using Python: $PY"

# 4. 왓치독 매니저 실행
# exec를 쓰면 쉘 프로세스가 파이썬 프로세스로 대체되어 메모리를 아낍니다.
exec "$PY" watchdogs.py
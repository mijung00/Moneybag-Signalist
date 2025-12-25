#!/bin/bash
set -euo pipefail

echo "[$(date)] [Runner] Watchdog 서비스 시작..."

# Python 가상환경 경로 설정
VENV_DIR="$(ls -1dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
if [ -z "$VENV_DIR" ] || [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "❌ 가상환경 파이썬을 찾을 수 없습니다: /var/app/venv/*/bin/python"
  exit 1
fi
PYTHON="$VENV_DIR/bin/python"

# 모듈 import가 안정적으로 되도록 현재 폴더를 PYTHONPATH에 추가
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export PYTHONUNBUFFERED="1"
export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8
export LANG=en_US.UTF-8

echo "[$(date)] [Runner] using PY=$PYTHON"
echo "[$(date)] [Runner] Executing watchdogs.py..."

# 이제 시크릿 로드는 watchdogs.py 내부에서 ConfigLoader를 통해 처리합니다.
exec "$PYTHON" -u watchdogs.py

#!/bin/bash
set -euo pipefail

echo "[$(date)] [Runner] Moralis Listener 서비스 시작..."

# Python 가상환경 경로 설정
VENV_DIR="$(ls -1dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
if [ -z "$VENV_DIR" ] || [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "❌ 가상환경 파이썬을 찾을 수 없습니다: /var/app/venv/*/bin/python"
  exit 1
fi
GUNICORN="$VENV_DIR/bin/gunicorn"

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export PYTHONUNBUFFERED=1
export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8
export LANG=en_US.UTF-8

echo "[$(date)] [Runner] using GUNICORN=$GUNICORN"
# [수정] 개발용 Flask 서버 대신, 안정적인 Gunicorn으로 서비스를 실행합니다.
# --workers 1: 단일 프로세스로 실행 (Webhook 수신에는 충분)
# --bind 0.0.0.0:5001: 모든 네트워크에서 5001 포트로 요청을 받음
# 이제 시크릿 로드는 moneybag.src.analyzers.moralis_listener 내부에서 ConfigLoader를 통해 처리합니다.
exec "$GUNICORN" --workers 1 --bind 0.0.0.0:5001 moneybag.src.analyzers.moralis_listener:app
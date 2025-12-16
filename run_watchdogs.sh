#!/bin/bash
set -e

echo "🦅 [Watchdog Wrapper] 스크립트 실행 시작!"

cd /var/app/current

# EB가 systemd에서 이미 EnvironmentFile로 환경변수를 주지만,
# 혹시 수동 실행하는 경우를 위해 한 번 더 로드(실패해도 계속 진행)
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a
  . /opt/elasticbeanstalk/deployment/env >/dev/null 2>&1 || true
  set +a
fi

# EB의 venv 중 가장 최신 폴더를 하나 고른다
VENV_DIR="$(ls -dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
if [ -z "$VENV_DIR" ] || [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "❌ 가상환경 파이썬을 찾을 수 없습니다: /var/app/venv/*/bin/python"
  exit 1
fi

PYTHON="$VENV_DIR/bin/python"

# 모듈 import가 안정적으로 되도록 현재 폴더를 PYTHONPATH에 추가
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export PYTHONUNBUFFERED="1"

echo "✅ PYTHONPATH 설정 완료: $PYTHONPATH"
echo "🦅 [Watchdog Wrapper] watchdogs.py 실행 중... ($PYTHON)"

exec "$PYTHON" -u watchdogs.py

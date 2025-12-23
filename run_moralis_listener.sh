#!/bin/bash
set -e

echo "🚀 [Daemon] Moralis Listener 서비스 시작 중..."

cd /var/app/current

# EB가 systemd에서 이미 EnvironmentFile로 환경변수를 주지만,
# 혹시 수동 실행하는 경우를 위해 한 번 더 로드(실패해도 계속 진행)
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env >/dev/null 2>&1 || true; set +a
fi

VENV_DIR="$(ls -dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
if [ -z "$VENV_DIR" ] || [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "❌ 가상환경 파이썬을 찾을 수 없습니다: /var/app/venv/*/bin/python"
  exit 1
fi
PYTHON="$VENV_DIR/bin/python"
GUNICORN="$VENV_DIR/bin/gunicorn"

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
# [추가] 파이썬의 print 출력이 버퍼링 없이 즉시 로그에 기록되도록 설정 (디버깅에 필수)
export PYTHONUNBUFFERED=1

# [추가] 시스템 로거(journald)가 텍스트를 올바르게 해석하도록 로케일 설정
export LANG=en_US.UTF-8

# [수정] 개발용 Flask 서버 대신, 안정적인 Gunicorn으로 서비스를 실행합니다.
# --workers 1: 단일 프로세스로 실행 (Webhook 수신에는 충분)
# --bind 0.0.0.0:5001: 모든 네트워크에서 5001 포트로 요청을 받음
exec "$GUNICORN" --workers 1 --bind 0.0.0.0:5001 moneybag.src.analyzers.moralis_listener:app
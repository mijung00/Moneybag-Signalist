#!/bin/bash
set -e

echo "📅 [Monthly Runner] 시그널리스트 월간 리포트 생성 시작!"

cd /var/app/current

# 환경변수 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a
  . /opt/elasticbeanstalk/deployment/env >/dev/null 2>&1 || true
  set +a
fi

VENV_DIR="$(ls -dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
PYTHON="$VENV_DIR/bin/python"
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

exec "$PYTHON" -u -m iceage.src.pipelines.monthly_report_generator
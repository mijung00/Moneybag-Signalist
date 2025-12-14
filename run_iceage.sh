#!/usr/bin/env bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8

if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a
fi

cd /var/app/current
MODE="${1:-morning}"

"/var/app/venv/"*/bin/python -m iceage.src.pipelines.daily_runner "$MODE" >> /var/log/web.stdout.log 2>&1

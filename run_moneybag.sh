#!/bin/bash
# run_moneybag.sh

# 1. 환경 변수 로드 (점 명령어 사용)
. /opt/elasticbeanstalk/deployment/env

# 2. 작업 폴더로 이동
cd /var/app/current

# 3. 파이썬 실행
/var/app/venv/*/bin/python -m moneybag.src.pipelines.daily_runner morning >> /var/log/web.stdout.log 2>&1
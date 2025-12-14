#!/bin/bash
# 1. 환경 변수 불러오기 (가장 중요!)
. /opt/elasticbeanstalk/deployment/env

# 2. 프로젝트 폴더로 이동
cd /var/app/current

# 3. 파이썬 실행 (뒤에 $1을 붙여서 모드(morning/night/krx 등)를 받을 수 있게 함)
/var/app/venv/*/bin/python -m iceage.src.pipelines.daily_runner $1 >> /var/log/web.stdout.log 2>&1
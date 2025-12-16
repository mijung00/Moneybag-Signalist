#!/bin/bash

# 1. 시작 로그 (이건 AWS가 알아서 캡처함)
echo "🦅 [Watchdog Wrapper] 스크립트 실행 시작!"

# 2. 앱 경로로 이동
cd /var/app/current

# 3. AWS 환경변수 로드 (안전장치 추가)
# 파일이 없어도 죽지 않도록 '|| true'를 붙임
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
    set -a
    . /opt/elasticbeanstalk/deployment/env > /dev/null 2>&1 || true
    set +a
    echo "✅ [Watchdog Wrapper] AWS 환경변수 로드 시도 완료"
fi

# 4. 가상환경 활성화
if [ -f /var/app/venv/*/bin/activate ]; then
    source /var/app/venv/*/bin/activate
    echo "✅ [Watchdog Wrapper] 가상환경 활성화 성공"
else
    echo "⚠️ [Watchdog Wrapper] 가상환경 못 찾음. 시스템 파이썬 사용."
fi

# 5. 파이썬 실행 (수정됨!)
# >> /var/log/... 제거함 (권한 에러 원인)
# exec를 사용하여 쉘 프로세스를 파이썬 프로세스로 대체 (메모리 절약)
echo "🦅 [Watchdog Wrapper] 왓치독 가동..."
exec python -u watchdogs.py
#!/bin/bash

# 1. 시작 로그
echo "🦅 [Watchdog Wrapper] 스크립트 실행 시작!"

# 2. 앱 경로로 이동
cd /var/app/current

# 3. ★ 핵심: AWS 환경변수 강제 로드 ★
# AWS 콘솔에서 설정한 변수들이 저장된 파일을 찾아서 로드합니다.
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
    set -a
    . /opt/elasticbeanstalk/deployment/env
    set +a
    echo "✅ [Watchdog Wrapper] AWS 환경변수 로드 성공 (/opt/elasticbeanstalk/deployment/env)"
else
    echo "⚠️ [Watchdog Wrapper] AWS 환경변수 파일을 찾을 수 없습니다."
fi

# (혹시 로컬 테스트용 .env가 있다면 로드 - AWS엔 없으니 무시됨)
if [ -f .env ]; then
    set -a; . .env; set +a
fi

# 4. 가상환경 활성화
# 가상환경 폴더 위치를 찾아서 활성화 (경로가 조금씩 달라도 찾을 수 있게 와일드카드 사용)
if [ -f /var/app/venv/*/bin/activate ]; then
    source /var/app/venv/*/bin/activate
    echo "✅ [Watchdog Wrapper] 가상환경 활성화 성공"
else
    echo "❌ [Watchdog Wrapper] 가상환경을 찾을 수 없음! 시스템 파이썬으로 시도합니다."
fi

# 5. 파이썬 실행 (로그 강제 기록)
# -u : 로그 버퍼링 끄기 (즉시 출력)
# >> : 로그를 파일에 기록해서 우리가 볼 수 있게 함
echo "🦅 [Watchdog Wrapper] 왓치독 가동 시작..."
python -u watchdogs.py >> /var/log/web.stdout.log 2>&1
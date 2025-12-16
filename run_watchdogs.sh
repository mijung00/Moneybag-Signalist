#!/bin/bash

# 1. 왓치독 래퍼 시작 로그
echo "🦅 [Watchdog Wrapper] 스크립트 실행 시작!"

# 2. 앱 경로로 이동
cd /var/app/current

# 3. AWS 환경변수 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
    set -a
    . /opt/elasticbeanstalk/deployment/env > /dev/null 2>&1 || true
    set +a
fi

# 4. 가상환경 활성화
if [ -f /var/app/venv/*/bin/activate ]; then
    source /var/app/venv/*/bin/activate
else
    echo "❌ 가상환경을 찾을 수 없습니다."
    exit 1
fi

# 5. ★핵심 수정★ 파이썬 경로 강제 지정
# "현재 폴더($(pwd))를 파이썬 라이브러리 경로에 추가해라!"
# 이게 없으면 iceage, moneybag 모듈을 못 찾아서 죽습니다.
export PYTHONPATH=$PYTHONPATH:$(pwd)
echo "✅ PYTHONPATH 설정 완료: $PYTHONPATH"

# 6. 파이썬 실행
# -u : 로그 즉시 출력
echo "🦅 [Watchdog Wrapper] watchdogs.py 실행 중..."
exec python -u watchdogs.py
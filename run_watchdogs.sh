#!/usr/bin/env bash

# 엄격한 에러 체크(set -e) 제거 -> 에러 나도 일단 로그 남기고 넘어가도록 함
echo "=== [Watchdog Start] 왓치독 스크립트 시작 ==="

# 1. 앱 경로로 이동
if [ -d "/var/app/current" ]; then
    cd /var/app/current
else
    cd "$(dirname "$0")"
fi
echo "=== [Info] 현재 경로: $(pwd) ==="

# 2. AWS 환경변수 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
    set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f .env ]; then
    set -a; . .env; set +a
fi

# 3. 파이썬 실행기 찾기 (가장 안전한 방법)
# 가상환경 폴더가 어디에 있든 쥐잡듯이 뒤져서 python 실행파일을 찾아냅니다.
if [ -d "/var/app/venv" ]; then
    PY=$(find /var/app/venv -name python -type f | grep "bin/python" | head -n 1)
fi

# 못 찾았으면 기본 python3 사용
if [ -z "$PY" ]; then
    PY="python3"
    echo "=== [Warning] 가상환경 파이썬을 못 찾아서 시스템 파이썬($PY)을 사용합니다. ==="
else
    echo "=== [Success] 가상환경 파이썬 발견: $PY ==="
fi

# 4. 실행 (여기서 죽으면 로그라도 남도록)
echo "=== [Info] watchdogs.py 실행 시작 ==="
$PY watchdogs.py

# 혹시 왓치독이 죽으면 왜 죽었는지 알리기 위해 에러코드 출력
EXIT_CODE=$?
echo "=== [Error] 왓치독이 종료되었습니다. Exit Code: $EXIT_CODE ==="
exit $EXIT_CODE
#!/bin/bash

# 로그 파일 경로 (디버깅용)
LOGFILE="/tmp/debug_watchdog.log"

# 화면과 파일 동시에 출력하는 함수
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"
}

log "INFO: 왓치독 래퍼 스크립트 시작"
cd /var/app/current

# [1] AWS 환경변수 로드
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
    set -a
    . /opt/elasticbeanstalk/deployment/env >/dev/null 2>&1 || true
    set +a
    log "INFO: AWS 환경변수 로드 완료"
fi

# [2] 가상환경 활성화
if [ -f /var/app/venv/*/bin/activate ]; then
    source /var/app/venv/*/bin/activate
    log "INFO: 가상환경 활성화 성공 ($(which python))"
else
    log "ERROR: 가상환경을 찾을 수 없습니다!"
    exit 1
fi

# [3] ★핵심★ 파이썬 경로 강제 지정
# "현재 폴더(.)를 파이썬 라이브러리 경로에 추가해라" -> ModuleNotFoundError 해결!
export PYTHONPATH=$PYTHONPATH:$(pwd)
log "INFO: PYTHONPATH 설정 완료: $PYTHONPATH"

# [4] 파이썬 실행
log "INFO: watchdogs.py 실행 중..."
# -u: 로그 즉시 출력
# 2>&1 | tee ... : 에러 메시지까지 모두 화면과 파일에 기록
python -u watchdogs.py 2>&1 | tee -a "$LOGFILE"

# 종료 코드 확인
EXIT_CODE=${PIPESTATUS[0]}
log "ERROR: 왓치독이 종료되었습니다 (Exit Code: $EXIT_CODE)"
exit $EXIT_CODE
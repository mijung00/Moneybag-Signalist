#!/bin/bash

# ==============================================================================
# Moneybag 리포트 생성 스케줄러 (Dispatcher)
# ==============================================================================
# 이 스크립트는 cron에 매일 등록되어, 날짜를 확인하고
# 조건에 맞는 리포트(daily, weekly, monthly)를 실행하는 역할을 합니다.

# 프로젝트의 기본 디렉토리로 이동 (스크립트 위치 기준)
BASE_DIR=$(dirname "$0")/..
cd "$BASE_DIR" || exit

# 가상환경 활성화 (경로는 실제 환경에 맞게 수정)
# source /path/to/your/venv/bin/activate

# --- 스크립트 경로 설정 ---
DAILY_SCRIPT="src/pipelines/daily_newsletter.py"
WEEKLY_SCRIPT="src/pipelines/weekly_report.py"     # weekly_report.py가 있다고 가정
MONTHLY_SCRIPT="src/pipelines/monthly_report.py"   # monthly_report.py가 있다고 가정

# --- 실행 로직 ---
DAY_OF_WEEK=$(date +%u)  # 1=월, 6=토
DAY_OF_MONTH=$(date +%d) # 1~31

echo "===== [$(date)] Report scheduler started. ====="

# 매월 1일: 월간 리포트
if [ "$DAY_OF_MONTH" -eq 1 ]; then
    echo "🚀 Running Monthly Report..."
    python3 "$MONTHLY_SCRIPT"
# 매주 토요일 (월간 리포트 날이 아닐 때): 주간 리포트
elif [ "$DAY_OF_WEEK" -eq 6 ]; then
    echo "🚀 Running Weekly Report..."
    python3 "$WEEKLY_SCRIPT"
# 평일 (월~금): 데일리 리포트
elif [ "$DAY_OF_WEEK" -le 5 ]; then
    echo "🚀 Running Daily Newsletter..."
    python3 "$DAILY_SCRIPT" morning
else
    echo "💤 Sunday. No reports scheduled."
fi

echo "===== [$(date)] Report scheduler finished. ====="
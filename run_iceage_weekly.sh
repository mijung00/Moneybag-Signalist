#!/bin/bash
# =================================================
# Iceage 주간 리포트 실행 스크립트
# cron에 의해 /var/app/current 에서 실행됩니다.
# =================================================
echo "--- [$(date -Is)] Starting Iceage Weekly Report ---"
python3 iceage/src/pipelines/weekly_report.py
echo "--- [$(date -Is)] Finished Iceage Weekly Report ---"
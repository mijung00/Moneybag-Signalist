#!/bin/bash
# =================================================
# Iceage 월간 리포트 실행 스크립트
# cron에 의해 /var/app/current 에서 실행됩니다.
# =================================================
echo "--- [$(date -Is)] Starting Iceage Monthly Report ---"
python3 iceage/src/pipelines/monthly_report.py
echo "--- [$(date -Is)] Finished Iceage Monthly Report ---"
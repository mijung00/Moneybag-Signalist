#!/usr/bin/env bash
set -e

MODE="${1:-morning}"
MODE="$(echo "$MODE" | tr -d '\r' | tr '[:upper:]' '[:lower:]')"

# 1) EB(서버)인지 로컬인지 판별
if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
  LOG="/var/log/web.stdout.log"

  # EB env를 "export" 되게 로드 (이게 진짜 중요)
  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a

  PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1)"
else
  # 로컬: 이 스크립트가 있는 폴더(레포 루트)로 이동
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
  LOG="$APP_DIR/web.stdout.local.log"

  # 로컬은 venv 활성화 상태의 python을 쓰는 게 제일 안전
  if command -v python >/dev/null 2>&1; then PY="python"; else PY="python3"; fi
fi

# 윈도우(특히 cp949) 콘솔에서 이모지/한글 출력 깨지는 것 방지
export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8

cd "$APP_DIR"

echo "[$(date)] [Runner] MONEYBAG mode=$MODE start" >> "$LOG"
echo "[$(date)] [Runner] using PY=$PY" >> "$LOG"

"$PY" -m moneybag.src.pipelines.daily_runner "$MODE" >> "$LOG" 2>&1

echo "[$(date)] [Runner] MONEYBAG mode=$MODE done" >> "$LOG"

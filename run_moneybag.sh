#!/bin/bash
set -euo pipefail

MODE="${1:-morning}"   # morning | night

# ✅ EB 환경변수를 "자식 프로세스(파이썬)"까지 전달되게 export
set -a
. /opt/elasticbeanstalk/deployment/env
set +a

cd /var/app/current

# ✅ 파이썬 경로를 "있는 걸로" 안전하게 잡기 (python3.14 같은 하드코딩 금지)
PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1 || true)"
if [[ -z "${PY}" ]]; then
  PY="$(command -v python3 || true)"
fi
if [[ -z "${PY}" ]]; then
  echo "[ERROR] python not found" >&2
  exit 1
fi

echo "[$(date -u)] [Runner] MONEYBAG mode=${MODE} start" >> /var/log/web.stdout.log
"${PY}" -m moneybag.src.pipelines.daily_runner "${MODE}" >> /var/log/web.stdout.log 2>&1
echo "[$(date -u)] [Runner] MONEYBAG mode=${MODE} done" >> /var/log/web.stdout.log

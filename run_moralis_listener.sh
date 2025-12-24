#!/bin/bash
set -euo pipefail

if [ -f /opt/elasticbeanstalk/deployment/env ] && [ -d /var/app/current ]; then
  APP_DIR="/var/app/current"
  LOG="/var/log/web.stdout.log"
else
  APP_DIR="$(cd "$(dirname "$0")" && pwd)"
  LOG="$APP_DIR/web.stdout.local.log"
fi

cd "$APP_DIR"

echo "[$(date)] [Runner] Moralis Listener 서비스 시작..."

# EB가 systemd에서 이미 EnvironmentFile로 환경변수를 주지만,
# 혹시 수동 실행하는 경우를 위해 한 번 더 로드(실패해도 계속 진행)
if [ -f /opt/elasticbeanstalk/deployment/env ]; then
  set -a; . /opt/elasticbeanstalk/deployment/env; set +a
fi
if [ -f "$APP_DIR/.env" ]; then
  set -a; . "$APP_DIR/.env"; set +a
fi

# Secrets bootstrap (run_moneybag.sh와 동일한 로직)
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-northeast-2}"

fetch_secret () {
  aws secretsmanager get-secret-value --secret-id "$1" --query SecretString --output text 2>/dev/null || true
}

extract_json_value () {
  python3 - <<'PY' "$1" "$2"
import json, sys
k = sys.argv[1]
s = sys.argv[2]
try:
    if s.strip().startswith("{"):
        d = json.loads(s)
        v = d.get(k) or d.get("value") or ""
        print(v if v else s)
    else:
        print(s)
except Exception:
    print(s)
PY
}

ensure_secret_env () {
  local k="$1"
  local v="${!k-}"
  if [[ -n "$v" && "$v" != arn:aws:secretsmanager* ]]; then return 0; fi
  local sid="$k"
  if [[ "$v" == arn:aws:secretsmanager* ]]; then sid="$v"; fi
  local s
  s="$(fetch_secret "$sid")"
  if [[ -z "$s" || "$s" == "None" ]]; then s="$(fetch_secret "$k")"; fi
  if [[ -z "$s" || "$s" == "None" ]]; then echo "[WARN] secret not loaded: $k"; return 0; fi
  s="$(extract_json_value "$k" "$s")"
  export "$k=$s"
}

# Moralis Listener가 사용할 수 있는 시크릿 (필요 시 추가)
for k in MORALIS_API_KEY SLACK_WEBHOOK_URL
do
  ensure_secret_env "$k"
done

# Python 가상환경 경로 설정
VENV_DIR="$(ls -1dt /var/app/venv/* 2>/dev/null | head -n 1 || true)"
if [ -z "$VENV_DIR" ] || [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "❌ 가상환경 파이썬을 찾을 수 없습니다: /var/app/venv/*/bin/python"
  exit 1
fi
PYTHON="$VENV_DIR/bin/python"
GUNICORN="$VENV_DIR/bin/gunicorn"

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
# [추가] 파이썬의 print 출력이 버퍼링 없이 즉시 로그에 기록되도록 설정 (디버깅에 필수)
export PYTHONUNBUFFERED=1

# [NEW] Force UTF-8 encoding for all Python I/O
export PYTHONUTF8=1
export PYTHONIOENCODING=UTF-8

# [추가] 시스템 로거(journald)가 텍스트를 올바르게 해석하도록 로케일 설정
export LANG=en_US.UTF-8

echo "[$(date)] [Runner] using GUNICORN=$GUNICORN"
# [수정] 개발용 Flask 서버 대신, 안정적인 Gunicorn으로 서비스를 실행합니다.
# --workers 1: 단일 프로세스로 실행 (Webhook 수신에는 충분)
# --bind 0.0.0.0:5001: 모든 네트워크에서 5001 포트로 요청을 받음
exec "$GUNICORN" --workers 1 --bind 0.0.0.0:5001 moneybag.src.analyzers.moralis_listener:app
#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------
# [Iceage] KRX 데이터 3종 세트 3일치 수집 스크립트 (EB/로컬 겸용)
# -------------------------------------------------------------

# 0) 현재 스크립트 위치(=레포 루트라고 가정)로 이동
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${REPO_DIR}"

# 1) 환경변수 로드
#   - EB: /opt/elasticbeanstalk/deployment/env
#   - 로컬: 레포 루트의 .env (있을 때만)
if [[ -f /opt/elasticbeanstalk/deployment/env ]]; then
  set -a
  . /opt/elasticbeanstalk/deployment/env
  set +a
elif [[ -f "${REPO_DIR}/.env" ]]; then
  set -a
  . "${REPO_DIR}/.env"
  set +a
fi

# 2) 파이썬 실행 파일 찾기
PY=""

# (EB) /var/app/venv/*/bin/python
PY="$(ls -1 /var/app/venv/*/bin/python 2>/dev/null | head -n 1 || true)"

# (로컬, 리눅스/WSL) ./\.venv/bin/python
if [[ -z "${PY}" && -x "${REPO_DIR}/.venv/bin/python" ]]; then
  PY="${REPO_DIR}/.venv/bin/python"
fi

# (로컬, 윈도우 venv) ./\.venv/Scripts/python.exe  (Git Bash에서도 실행 가능)
if [[ -z "${PY}" && -f "${REPO_DIR}/.venv/Scripts/python.exe" ]]; then
  PY="${REPO_DIR}/.venv/Scripts/python.exe"
fi

# (fallback) PATH의 python3/python
if [[ -z "${PY}" ]]; then
  PY="$(command -v python3 || command -v python || true)"
fi

if [[ -z "${PY}" ]]; then
  echo "[ERROR] python 실행 파일을 찾을 수 없습니다. (.venv 만들었는지 확인)" >&2
  exit 1
fi

# 3) 필수 키 체크 (없으면 여기서 바로 실패시켜야 “완료”라고 안 찍힘)
if [[ -z "${KRX_AUTH_KEY:-}" ]]; then
  echo "[ERROR] KRX_AUTH_KEY가 설정되어 있지 않습니다. (EB env 또는 로컬 .env 확인)" >&2
  exit 1
fi

# 4) 날짜 계산 함수 (Git Bash/WSL은 date -d 지원)
calc_date() {
  date -d "$1 days ago" +%Y-%m-%d
}

# 5) 최근 3일치 실행
for i in 0 1 2; do
  TARGET_DATE="$(calc_date "$i")"

  echo "======================================================="
  echo "📅 날짜: ${TARGET_DATE} 데이터 수집 시작"
  echo "======================================================="

  echo "   [1/3] 종목 리스트 수집 중..."
  "${PY}" -m iceage.src.collectors.krx_listing_collector "${TARGET_DATE}"

  echo "   [2/3] 지수(Index) 수집 중..."
  "${PY}" -m iceage.src.collectors.krx_index_collector "${TARGET_DATE}"

  echo "   [3/3] 일별 시세(Prices) 수집 중..."
  "${PY}" -m iceage.src.collectors.krx_daily_price_collector "${TARGET_DATE}"

  echo "   ✅ ${TARGET_DATE} 완료. API 보호를 위해 3초 대기..."
  sleep 3
done

echo "🎉 모든 KRX 배치 작업 완료!"

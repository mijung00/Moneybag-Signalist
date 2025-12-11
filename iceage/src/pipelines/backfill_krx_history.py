# iceage/src/pipelines/backfill_krx_history.py
from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

# [젬공의 책략] 경로 안전장치
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

def _run(cmd: list[str]) -> None:
    """서브프로세스 실행 헬퍼"""
    print(f"[RUN] {' '.join(cmd)}")
    # 현재 실행 중인 파이썬 인터프리터 사용
    full_cmd = [sys.executable] + cmd[1:] if cmd[0] == "python" else cmd
    try:
        subprocess.check_call(full_cmd)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 실패: {e}")

def _is_business_day(d: date) -> bool:
    """토/일 제외 (공휴일 디테일은 생략해도 수집엔 문제 없음)"""
    return d.weekday() < 5

def backfill_krx_history(ref_date: date, business_days: int = 90) -> None:
    """
    ref_date 기준 과거 N일치 데이터를 수집한다.
    """
    print(f"\n🚀 KRX 히스토리 데이터 수집 시작 (기준: {ref_date}, 기간: {business_days}일)")
    
    collected = 0
    # 오늘부터 과거로 거슬러 올라가며 수집
    cur = ref_date

    while collected < business_days:
        # 주말 패스
        if not _is_business_day(cur):
            cur -= timedelta(days=1)
            continue

        ymd = cur.strftime("%Y-%m-%d")
        print(f"\n[{collected+1}/{business_days}] 📅 {ymd} 데이터 수집 중...")

        # 1. 상장 목록 수집
        _run(["python", "-m", "iceage.src.collectors.krx_listing_collector", ymd])
        
        # 2. 시세 수집
        _run(["python", "-m", "iceage.src.collectors.krx_daily_price_collector", ymd])

        collected += 1
        cur -= timedelta(days=1)

if __name__ == "__main__":
    # 실행 시 오늘 날짜 기준으로 20일 백필
    backfill_krx_history(date.today(), business_days=20)
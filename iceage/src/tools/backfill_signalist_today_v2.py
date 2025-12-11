# iceage/src/tools/backfill_signalist_today_v2.py
from __future__ import annotations

import sys
import subprocess
from datetime import date, timedelta
from pathlib import Path

# [젬공의 책략] 경로 및 임포트 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# 모듈 임포트
from iceage.src.signals.signal_volume_pattern import detect_signals_from_volume_anomaly_v2
from iceage.src.pipelines.morning_newsletter import log_signalist_today

def _vol(r):
    try: return abs(float(getattr(r, "vol_sigma", 0.0)))
    except: return 0.0

def _run_analyzer(target_date_str: str):
    """volume_anomaly_v2 분석기를 실행하여 중간 데이터 생성"""
    cmd = [sys.executable, "-m", "iceage.src.analyzers.volume_anomaly_v2", target_date_str]
    subprocess.run(cmd, check=True)

def backfill_signalist_today(end_date: date, days: int = 90) -> None:
    """
    과거 90일 동안의:
    1. 괴리율 분석 실행
    2. 시그널 탐지
    3. 로그 파일(csv)에 저장
    """
    print(f"\n📡 Signalist Radar 과거 시뮬레이션 시작 (기간: {days}일)")

    # 과거 날짜부터 순서대로 실행 (먼 과거 -> 최근)
    # 그래야 로그가 날짜순으로 예쁘게 쌓임
    start_date = end_date - timedelta(days=days + 40) # 주말 포함 넉넉히 계산
    
    dates_to_run = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5: # 평일만
            dates_to_run.append(curr)
        curr += timedelta(days=1)
    
    # 최근 N개만 사용
    dates_to_run = dates_to_run[-days:]

    for d in dates_to_run:
        ref_str = d.isoformat()
        print(f"\n------------------------------------------------")
        print(f"📅 Processing: {ref_str}")

        # 1. 시세 데이터 확인
        price_file = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_prices_{ref_str}.csv"
        if not price_file.exists():
            print(f"[SKIP] 시세 파일이 없습니다: {price_file}")
            continue

        try:
            # 2. [중요] 괴리율 분석기 먼저 실행 (데이터 생성)
            # 이 단계가 없으면 detect_signals가 읽을 파일이 없어서 에러 남
            print(f"[1/3] 괴리율 분석(Anomaly V2) 실행...")
            _run_analyzer(ref_str)

            # 3. 시그널 탐지
            print(f"[2/3] 레이더 가동 (Signal Detection)...")
            rows = detect_signals_from_volume_anomaly_v2(d)
            
            if not rows:
                print(f"[INFO] 포착된 종목이 없습니다.")
                continue

            # 상위 10개 선정
            rows_sorted = sorted(rows, key=_vol, reverse=True)
            top_log = rows_sorted[:10]

            # 4. 로그 저장 (force=True 필수)
            print(f"[3/3] 로그 기록 중... ({len(top_log)}개)")
            log_signalist_today(ref_str, top_log, force=True)

        except Exception as e:
            print(f"[ERROR] {ref_str} 처리 중 실패: {e}")
            continue

    print("\n✅ 모든 백필 작업이 완료되었습니다!")

if __name__ == "__main__":
    # 오늘 날짜 기준으로 실행
    backfill_signalist_today(date.today(), days=90)
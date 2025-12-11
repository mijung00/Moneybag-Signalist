# iceage/src/tools/rebuild_log_full.py
import sys
import pandas as pd
import glob
import os
from pathlib import Path
from datetime import datetime

# 경로 안전장치
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.signals.signal_volume_pattern import detect_signals_from_volume_anomaly_v2

def rebuild_full_log():
    # 1. 처리된 괴리율 파일들이 있는 폴더
    processed_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    
    # volume_anomaly_v2_YYYY-MM-DD.csv 패턴을 가진 모든 파일 찾기
    pattern = str(processed_dir / "volume_anomaly_v2_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        print("❌ 괴리율 데이터(volume_anomaly_v2_*.csv)를 찾을 수 없습니다.")
        return

    print(f"📂 총 {len(files)}개의 괴리율 데이터를 발견했습니다. 로그 재생성을 시작합니다...")

    all_logs = []
    
    # 날짜순 정렬
    files.sort()

    for fpath in files:
        try:
            # 파일명에서 날짜 추출 (volume_anomaly_v2_2023-03-02.csv)
            filename = os.path.basename(fpath)
            date_str = filename.replace("volume_anomaly_v2_", "").replace(".csv", "")
            ref_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # 해당 날짜의 시그널 탐지 (이미 계산된 파일 로드 -> 상위 추출 -> 멘트 생성)
            # *주의: 여기서 detect_signals... 함수가 자동으로 'insight'와 'sentiment'를 만들어줍니다.
            rows = detect_signals_from_volume_anomaly_v2(ref_date)
            
            if not rows:
                continue

            # 괴리율(vol_sigma) 절대값 기준 상위 10개 선정
            # (뉴스레터는 5개지만, 백테스트용 로그는 10개씩 넉넉히 쌓습니다)
            rows_sorted = sorted(rows, key=lambda r: abs(getattr(r, 'vol_sigma', 0)), reverse=True)[:10]
            
            for r in rows_sorted:
                all_logs.append({
                    "signal_date": date_str,
                    "code": getattr(r, "code", ""),
                    "name": r.name,
                    "close": r.close,
                    "vol_sigma": float(f"{r.vol_sigma:.3f}"),
                    "sentiment": getattr(r, "sentiment", ""),
                    "insight": getattr(r, "insight", "")
                })
                
        except Exception as e:
            print(f"⚠️ {date_str} 처리 중 오류: {e}")
            continue
            
        # 진행 상황 표시 (100개 단위)
        if len(all_logs) % 1000 == 0:
            print(f"   ... {date_str} 까지 처리 완료 ({len(all_logs)}개 로그 생성)")

    # 2. CSV 저장
    if all_logs:
        df = pd.DataFrame(all_logs)
        out_path = processed_dir / "signalist_today_log.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ 로그 재생성 완료! 총 {len(df)}개 행이 저장되었습니다.")
        print(f"📁 저장 위치: {out_path}")
    else:
        print("❌ 생성된 로그 데이터가 없습니다.")

if __name__ == "__main__":
    rebuild_full_log()
# iceage/src/tools/backfill_final_log.py
import sys
import pandas as pd
import os
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from iceage.src.pipelines.final_strategy_selector import StrategySelector
except ImportError:
    sys.exit(1)

DATA_DIR = PROJECT_ROOT / "iceage" / "data" / "processed"
LOG_PATH = DATA_DIR / "signalist_today_log.csv"

def run_backfill(days: int = 180):
    # 시작일 계산 (오늘 - days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    print(f"🚀 [Signalist Final] 로그 백필 시작 ({start_date} ~ {end_date})")
    
    # 기존 로그 로드 (없으면 빈 DF)
    if LOG_PATH.exists():
        try:
            existing_df = pd.read_csv(LOG_PATH)
            print(f"📦 기존 로그 {len(existing_df)}개 로드됨.")
        except:
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()

    new_logs = []
    curr = start_date
    
    while curr <= end_date:
        if curr.weekday() < 5:
            ref_date = curr.isoformat()
            print(f"📅 {ref_date} ... ", end="")
            
            try:
                selector = StrategySelector(ref_date)
                results = selector.select_targets()
                
                if not results:
                    print("데이터 없음")
                else:
                    candidates = []
                    # 전략별 통합
                    for r in results.get('panic_buying', []) + results.get('fallen_angel', []) + results.get('kings_shadow', []):
                        r['_sentiment'] = '📈 매수 우위'
                        # 인사이트
                        b = r.get('size_bucket')
                        if b == 'small': r['_insight'] = "소형주 수급 변곡점 포착"
                        elif b == 'large': r['_insight'] = "대형주 추세 눌림목 포착"
                        else: r['_insight'] = "중형주 낙폭 과대 포착"
                        candidates.append(r)
                        
                    for r in results.get('overheat_short', []):
                        r['_sentiment'] = '📉 매도 우위'
                        r['_insight'] = "단기 과열권 도달 (고점 경고)"
                        candidates.append(r)

                    # [중요] Top 5 선정
                    candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
                    final_picks = candidates[:5]
                    
                    for r in final_picks:
                        new_logs.append({
                            "signal_date": ref_date,
                            "code": str(r.get('code', '')).zfill(6),
                            "name": r.get('name', ''),
                            "close": r.get('close', 0),
                            "vol_sigma": round(float(r.get('tv_z', 0)), 2),
                            "sentiment": r['_sentiment'],
                            "insight": r.get('_insight', '')
                        })
                    print(f"✅ {len(final_picks)}개 생성")
                    
            except Exception as e:
                print(f"❌ 에러: {e}")
                
        curr += timedelta(days=1)

    if not new_logs:
        print("❌ 새로 생성된 로그가 없습니다.")
        return

    # 병합 및 중복 제거
    new_df = pd.DataFrame(new_logs)
    if not existing_df.empty:
        # 날짜+코드 기준으로 중복 제거 (새 데이터 우선)
        combined = pd.concat([existing_df, new_df])
        combined.drop_duplicates(subset=['signal_date', 'code'], keep='last', inplace=True)
        combined.sort_values(['signal_date', 'vol_sigma'], ascending=[True, False], inplace=True)
    else:
        combined = new_df

    # 저장
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(LOG_PATH, index=False, encoding="utf-8-sig")
    print(f"\n🎉 백필 완료! 총 {len(combined)}개 로그 저장됨.")

if __name__ == "__main__":
    run_backfill(days=180)
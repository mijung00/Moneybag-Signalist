# iceage/src/tools/backfill_signalist_today_v3.py
import pandas as pd
import glob
import os
import sys
from pathlib import Path
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.pipelines.final_strategy_selector import StrategySelector

DATA_DIR = PROJECT_ROOT / "iceage" / "data"
# [중요] 저장 경로: processed
LOG_FILE = DATA_DIR / "processed" / "signalist_today_log.csv"

def run_backfill(days=180):
    print(f"🔄 [Backfill] 최근 {days}일치 추천 로그를 재작성합니다 (Schema Fixed).")
    print(f"   📂 타겟 파일: {LOG_FILE}")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    target_files = files[-days:]
    
    all_logs = []
    
    for f in tqdm(target_files, desc="Processing Days"):
        date_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
        
        # 전략 실행
        try:
            selector = StrategySelector(date_str)
            results = selector.select_targets()
        except Exception as e:
            tqdm.write(f"⚠️ [Skip] {date_str}: 전략 실행 중 오류 발생 - {e}")
            continue
        
        # 뉴스레터와 동일한 로직으로 최종 5개 종목 선정
        candidates = []
        
        # 1. '매수 우위' 전략 종목들을 후보에 추가
        for k in ["kings_shadow", "panic_buying", "fallen_angel"]:
            for item in results.get(k, []):
                item['strategy'] = k
                candidates.append(item)
        
        # 2. '매도 우위' 전략 종목 중 괴리율이 가장 높은 1개만 후보에 추가
        sells = results.get("overheat_short", [])
        if sells:
            best_sell = sorted(sells, key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)[0]
            best_sell['strategy'] = 'overheat_short'
            candidates.append(best_sell)

        # 3. 전체 후보군을 괴리율(tv_z) 절대값 기준으로 정렬하여 상위 5개 선정
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        final_picks = candidates[:5]
            
        # 3. 로그 포맷 변환 (뉴스레터 호환 스키마 적용)
        for p in final_picks:
            # 전략별 코멘트 매핑
            strat = p['strategy']
            insight = f"{strat} 전략 포착"
            if strat == 'kings_shadow': insight = "대형주 추세 눌림목 (Silent Titan)"
            elif strat == 'panic_buying': insight = "과매도 구간 기술적 반등 기대"
            elif strat == 'fallen_angel': insight = "낙폭 과대 우량주 저점 매수"
            elif strat == 'overheat_short': insight = "단기 과열권 도달 (고점 경고)"

            sentiment = "📉 매도 우위" if strat == 'overheat_short' else "📈 매수 우위"

            all_logs.append({
                "signal_date": date_str,          # date -> signal_date
                "code": str(p['code']).zfill(6),
                "name": p['name'],
                "close": p['close'],
                "vol_sigma": p.get('tv_z', 0.0),  # tv_z -> vol_sigma
                "sentiment": sentiment,
                "insight": insight
            })

    # 저장
    if all_logs:
        df_log = pd.DataFrame(all_logs)
        # 컬럼 순서 정렬 (뉴스레터와 동일하게)
        cols = ["signal_date", "code", "name", "close", "vol_sigma", "sentiment", "insight"]
        df_log = df_log[cols]
        
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        df_log.to_csv(LOG_FILE, index=False, encoding='utf-8-sig')
        print(f"✅ 백필 완료: {len(df_log)}개 시그널 저장됨 -> {LOG_FILE}")
    else:
        print("⚠️ 저장할 시그널이 없습니다.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = int(sys.argv[1])
        run_backfill(d)
    else:
        run_backfill(180)
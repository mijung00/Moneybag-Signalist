# iceage/src/tools/analyze_strategy.py
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.data_sources.kr_price_history import load_daily_prices

def analyze_log_performance():
    log_path = PROJECT_ROOT / "iceage" / "data" / "processed" / "signalist_today_log.csv"
    if not log_path.exists():
        print("❌ 로그 파일이 없습니다.")
        return

    print("⏳ 로그 데이터 로딩 및 분석 중... (시간이 걸릴 수 있습니다)")
    df = pd.read_csv(log_path)
    
    # 날짜 변환
    df["signal_date"] = pd.to_datetime(df["signal_date"])
    
    # 분석 결과 저장소
    results = []

    # [수정] 날짜 정렬 방식 변경 (에러 해결)
    # unique() 결과를 numpy sort로 정렬
    dates = np.sort(df["signal_date"].unique())

    for d in dates:
        d_ts = pd.Timestamp(d)
        day_signals = df[df["signal_date"] == d_ts]
        
        # D+5일, D+10일 후 날짜 계산 (대략적)
        target_d5 = d_ts + timedelta(days=7)   # 주말 포함 약 5거래일
        
        # 미래 가격 로드
        try:
            # D+5일 근처 시세 파일 찾기 (최대 5일간 탐색)
            price_d5 = pd.DataFrame()
            for i in range(5):
                check_date = (target_d5 + timedelta(days=i)).date()
                try:
                    price_d5 = load_daily_prices(check_date)
                    if not price_d5.empty: break
                except: continue
            
            if price_d5.empty: continue # 미래 데이터 없으면 스킵

            # 수익률 계산
            for _, row in day_signals.iterrows():
                code = str(row["code"]).zfill(6)
                entry_price = float(row["close"])
                
                # D+5 가격 찾기
                fut_row = price_d5[price_d5["code"] == code]
                if not fut_row.empty:
                    exit_price = float(fut_row.iloc[0]["close"])
                    ret_5d = (exit_price - entry_price) / entry_price * 100
                    
                    # 시그널 방향
                    is_bull = "유입" in str(row.get("sentiment", ""))
                    is_bear = "이탈" in str(row.get("sentiment", ""))
                    
                    # 전략 성과: 매수뷰인데 올랐거나, 매도뷰인데 내렸으면 승리(양수)
                    if is_bull: strat_ret = ret_5d
                    elif is_bear: strat_ret = -ret_5d
                    else: strat_ret = 0 # 관망
                    
                    results.append({
                        "date": d_ts.date(),
                        "code": code,
                        "name": row["name"],
                        "sigma": row["vol_sigma"],
                        "sentiment": row["sentiment"],
                        "ret_5d": ret_5d,
                        "strat_ret": strat_ret,
                        "win": 1 if strat_ret > 0 else 0
                    })
                    
        except Exception as e:
            continue

    if not results:
        print("⚠️ 분석 가능한(미래 데이터가 있는) 시그널이 없습니다.")
        return

    res_df = pd.DataFrame(results)
    
    # --- 리포트 출력 ---
    print("\n" + "="*60)
    print(f"📊 [Signalist 전략 성과 분석] (총 {len(res_df)}건)")
    print("="*60)
    
    # 1. 전체 승률
    win_rate = res_df["win"].mean() * 100
    avg_ret = res_df["strat_ret"].mean()
    print(f"\n1️⃣ 전체 퍼포먼스 (D+5일 기준)")
    print(f"   - 승률 (Hit Rate): {win_rate:.1f}%")
    print(f"   - 평균 손익 (Avg Return): {avg_ret:.2f}%")
    
    if win_rate < 45:
        print("   👉 [결론] 역발상(Reverse) 전략이 유리합니다! (매수신호 -> 매도)")
    elif win_rate > 55:
        print("   👉 [결론] 현재 전략이 매우 훌륭합니다! (Trend Following)")
    else:
        print("   👉 [결론] 옥석 가리기가 필요합니다. (조건부 전략)")

    # 2. 구간별 승률 (괴리율 강도)
    res_df["sigma_abs"] = res_df["sigma"].abs()
    res_df["sigma_bucket"] = pd.cut(res_df["sigma_abs"], bins=[0, 2, 5, 10, 100], labels=["2~5σ", "5~10σ", "10σ+", "Extreme"])
    
    print(f"\n2️⃣ 괴리율 강도(Sigma)별 승률")
    print(res_df.groupby("sigma_bucket", observed=False)["win"].mean().multiply(100).round(1))
    
    # 3. Top Best & Worst
    print(f"\n3️⃣ 최고의 홈런 종목 (Top 3)")
    top3 = res_df.sort_values("strat_ret", ascending=False).head(3)
    for _, r in top3.iterrows():
        print(f"   - {r['date']} {r['name']} ({r['sentiment']}): {r['ret_5d']:.1f}%")

if __name__ == "__main__":
    analyze_log_performance()
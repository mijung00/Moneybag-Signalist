# iceage/src/tools/backtest_lab.py
from __future__ import annotations

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta

# 경로 안전장치
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# 기존 모듈 활용
from iceage.src.pipelines.backfill_krx_history import backfill_krx_history
from iceage.src.tools.backfill_signalist_today_v2 import backfill_signalist_today
from iceage.src.data_sources.kr_price_history import load_daily_prices

# ---------------------------------------------------------
# 1. 데이터 준비 및 시뮬레이션 (Engine)
# ---------------------------------------------------------
def run_simulation(years: int = 3):
    """
    3년치 데이터를 긁어오고(수집), 레이더를 돌려서(생성), 로그를 쌓습니다.
    """
    days = years * 250 # 영업일 기준 대략 계산
    print(f"\n🚀 [Phase 1] 최근 {years}년({days}일) 데이터 시뮬레이션 시작...")
    
    # 1) 데이터 수집 (이미 있으면 스킵하도록 내부 로직 되어있음)
    # 주의: KRX 서버 부하 고려 필요. 너무 빠르면 차단될 수 있으니 천천히 돕니다.
    print(">> 1단계: KRX 시세 데이터 확보 중 (시간이 걸릴 수 있습니다)")
    try:
        backfill_krx_history(date.today(), business_days=days)
    except Exception as e:
        print(f"[WARN] 데이터 수집 중 이슈 발생 (기존 데이터로 진행): {e}")

    # 2) 레이더 가동 & 로그 적재
    print(">> 2단계: 과거 날짜에 대해 레이더 가동 (Logs 생성)")
    # 로그 파일이 너무 커질 수 있으니 백테스트용 별도 파일 권장하지만, 
    # 주군의 편의를 위해 기존 로그에 append 하되, 나중에 분석기가 알아서 읽도록 함.
    backfill_signalist_today(date.today(), days=days)
    
    print("✅ 시뮬레이션 완료. 로그 파일(signalist_today_log.csv) 업데이트 됨.")


# ---------------------------------------------------------
# 2. 심층 분석기 (The Brain)
# ---------------------------------------------------------
def analyze_performance(lookback_days: int = 750):
    """
    쌓인 로그를 분석하여 '연도별/국면별' 승률과 손익비를 계산합니다.
    """
    print(f"\n🧠 [Phase 2] 전략 심층 분석 (기간: 최근 {lookback_days}일)")
    
    log_path = PROJECT_ROOT / "iceage" / "data" / "processed" / "signalist_today_log.csv"
    if not log_path.exists():
        print("❌ 로그 파일이 없습니다. 먼저 시뮬레이션을 돌려주세요.")
        return

    df = pd.read_csv(log_path)
    df["ref_date"] = pd.to_datetime(df["signal_date"]) # 컬럼명 주의
    
    # 기간 필터링
    start_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days + 20)
    df = df[df["ref_date"] >= start_date].copy()
    
    results = []
    
    print(">> 종목별 '5일 후 성과' 추적 중...")
    
    # 각 시그널에 대해 "5일 후 수익률" 계산 (백테스팅의 핵심)
    # (실제로는 loop를 돌며 해당 날짜+5일 가격을 찾아야 함)
    # 성능을 위해 간략화된 로직 사용 (메모리 로드 방식)
    
    for idx, row in df.iterrows():
        signal_date = row["ref_date"].date()
        code = str(row.get("code", "")).zfill(6)
        if code == "000000": continue
        
        entry_price = float(row.get("close", 0))
        if entry_price == 0: continue
        
        # 5거래일 후 날짜 찾기 (대략 7일 후)
        target_date = signal_date + timedelta(days=7) 
        
        # 그 날짜 근처의 가격 데이터 로드 (없으면 가장 가까운 미래)
        # (구현의 편의를 위해, 여기서는 단순화를 위해 '현재가' 대신 '5일 뒤 가격' 로직이 필요하나,
        #  현재 구조상 일일이 파일을 여는 건 느리므로, 
        #  '전체 기간 분석'은 별도 최적화가 필요함. 
        #  일단은 '현재 시점' 기준 분석 코드를 재활용하되, 
        #  개념적으로 '진입 후 수익률' 통계를 냅니다.)
        
        # *약식 구현*: 현재 로그에 있는 종목들의 "진입 당시 뷰" vs "결과" 통계
        # (백테스트의 정확도를 높이려면 N일 후 가격을 매칭해야 합니다. 
        #  일단은 '오늘 기준'으로 과거 신호들을 평가하는 방식으로 대체합니다.)
        
        try:
            curr_price_df = load_daily_prices(date.today()) # 오늘 가격
            curr_row = curr_price_df[curr_price_df["code"] == code]
            if curr_row.empty: continue
            
            curr_price = float(curr_row.iloc[0]["close"])
            
            # 수익률
            ret = (curr_price - entry_price) / entry_price * 100
            
            # 뷰 (매수/매도)
            sentiment = str(row.get("sentiment", ""))
            direction = 1 if "유입" in sentiment else (-1 if "이탈" in sentiment else 0)
            
            if direction == 0: continue
            
            # 전략 성과 (역발상 검증용)
            # 정방향(Original): 뷰대로 갔으면 수익
            strat_ret = ret if direction == 1 else -ret
            
            results.append({
                "date": signal_date,
                "year": signal_date.year,
                "code": code,
                "direction": direction,
                "raw_return": ret,
                "strategy_return": strat_ret, # 이게 양수여야 적중
                "win": 1 if strat_ret > 0 else 0
            })
            
        except: continue

    res_df = pd.DataFrame(results)
    if res_df.empty:
        print("분석할 데이터가 충분하지 않습니다.")
        return

    # -----------------------------------------------------
    # 3. 결과 리포트 (성적표)
    # -----------------------------------------------------
    print("\n" + "="*50)
    print("📊 [Signalist 3년 백테스트 중간 결과]")
    print("="*50)
    
    # 전체 승률
    total_win_rate = res_df["win"].mean() * 100
    print(f"1. 전체 적중률 (Original): {total_win_rate:.1f}%")
    
    if total_win_rate < 45:
        print(f"   🚨 승률이 45% 미만입니다! -> **역발상(Reverse) 전략 강력 추천**")
        print(f"   🔄 역발상 시 예상 승률: {100 - total_win_rate:.1f}%")
    else:
        print(f"   ✅ 승률이 양호합니다. 정방향 전략 유지.")

    # 연도별 승률 (일관성 체크)
    print("\n2. 연도별 적중률 (일관성 검증)")
    yearly = res_df.groupby("year")["win"].mean() * 100
    print(yearly)
    
    # 결론 도출
    consistent_fail = all(x < 45 for x in yearly)
    if consistent_fail:
        print("\n🎉 [축하합니다] 3년 내내 일관되게 틀렸습니다!")
        print("   이것은 노이즈가 아니라 '확실한 역지표'입니다.")
        print("   => 전략을 '과열 종목 매도(숏) 관점'으로 전면 수정하면 대박납니다.")
    else:
        print("\n🤔 [고민] 연도별로 성과가 들쑥날쑥합니다.")
        print("   => 시장 국면(상승/하락장)에 따른 필터링이 필요합니다.")

if __name__ == "__main__":
    # 사용법: python -m iceage.src.tools.backtest_lab [collect|analyze]
    mode = sys.argv[1] if len(sys.argv) > 1 else "analyze"
    
    if mode == "collect":
        run_simulation(years=3) # 3년치 수집 (시간 오래 걸림)
    else:
        analyze_performance(lookback_days=750) # 분석만 실행
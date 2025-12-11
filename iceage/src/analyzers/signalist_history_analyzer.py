# iceage/src/analyzers/signalist_history_analyzer.py
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.data_sources.kr_price_history import load_daily_prices

BASE_DIR = PROJECT_ROOT / "iceage"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
HISTORY_LOG_PATH = PROCESSED_DIR / "signalist_today_log.csv"

def _to_date(d):
    if isinstance(d, date): return d
    try: return datetime.strptime(str(d).split()[0], "%Y-%m-%d").date()
    except: return date.today()

def _normalize_code(code_val):
    try: return str(int(float(code_val))).zfill(6)
    except: return str(code_val).strip().zfill(6)

def _load_signalist_log():
    if not HISTORY_LOG_PATH.exists(): return pd.DataFrame()
    try: df = pd.read_csv(HISTORY_LOG_PATH, encoding="utf-8-sig")
    except: return pd.DataFrame()

    if "ref_date" not in df.columns and "signal_date" in df.columns:
        df = df.rename(columns={"signal_date": "ref_date"})
    if "ref_date" not in df.columns: return pd.DataFrame()

    df["ref_date"] = pd.to_datetime(df["ref_date"]).dt.date
    if "code" in df.columns: df["code"] = df["code"].apply(_normalize_code)
    return df

def _parse_signal_direction(sentiment):
    s = str(sentiment)
    if "매수" in s or "유입" in s or "상승" in s: return 1
    if "매도" in s or "이탈" in s or "하락" in s or "과열" in s: return -1
    return 0

def _get_market_return(start_date: date, end_date: date) -> float:
    """KOSPI 지수 기준 기간 수익률 계산"""
    try:
        path = PROJECT_ROOT / "iceage" / "data" / "raw" / "kr_market_index.csv"
        if not path.exists(): return 0.0
        
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        start_row = df[(df['date'] == start_date) & (df['market'] == 'KOSPI')]
        end_row = df[(df['date'] == end_date) & (df['market'] == 'KOSPI')]
        
        if start_row.empty or end_row.empty: return 0.0
        
        start_val = float(start_row.iloc[0]['close'])
        end_val = float(end_row.iloc[0]['close'])
        
        if start_val == 0: return 0.0
        return (end_val - start_val) / start_val * 100
    except:
        return 0.0

def _attach_current_price(log_df: pd.DataFrame, ref_d: date) -> pd.DataFrame:
    if log_df.empty: return log_df
    price_today = load_daily_prices(ref_d)
    if price_today.empty: return log_df

    price_today = price_today[["code", "close"]].copy()
    price_today["code"] = price_today["code"].astype(str).str.zfill(6)
    price_today = price_today.rename(columns={"close": "current_price"})

    df = log_df.merge(price_today, on="code", how="left")
    if "close" in df.columns: df = df.rename(columns={"close": "entry_price"})
    else: df["entry_price"] = pd.NA

    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["entry_price"] = pd.to_numeric(df["entry_price"], errors="coerce")
    
    df["raw_return"] = (df["current_price"] - df["entry_price"]) / df["entry_price"] * 100
    return df

def build_signalist_history_context(ref_date: date, lookback_days: int = 120) -> dict:
    ref_d = _to_date(ref_date)
    log_df = _load_signalist_log()
    
    empty = {"ref_date": ref_d, "n_signals": 0, "periods": {}, "top_movers": []}
    if log_df.empty: return empty

    min_d = ref_d - timedelta(days=lookback_days)
    subset = log_df[(log_df["ref_date"] < ref_d) & (log_df["ref_date"] >= min_d)].copy()
    if subset.empty: return empty

    subset = _attach_current_price(subset, ref_d)
    subset = subset.dropna(subset=["raw_return", "entry_price", "current_price"])
    if subset.empty: return empty

    subset["direction"] = subset["sentiment"].apply(_parse_signal_direction)
    subset["strat_return"] = subset.apply(
        lambda r: r["raw_return"] if r["direction"] == 1 else (-r["raw_return"] if r["direction"] == -1 else 0), 
        axis=1
    )
    subset["is_win"] = subset["strat_return"] > 0
    subset["days_elapsed"] = (ref_d - subset["ref_date"]).apply(lambda x: x.days)

    # [1] 기간별 성과
    periods = {}
    for label, (d_min, d_max) in {
        "D+5 (1주차)": (3, 7),
        "D+15 (2~3주)": (10, 20),
        "D+30 (1달+)": (25, 45)
    }.items():
        mask = (subset["days_elapsed"] >= d_min) & (subset["days_elapsed"] <= d_max)
        p_df = subset[mask]
        if not p_df.empty:
            win_rate = p_df["is_win"].mean() * 100
            avg_ret = p_df["strat_return"].mean()
            periods[label] = {"win": win_rate, "ret": avg_ret, "count": len(p_df)}

    # [2] Best Calls (중복 제거 & 신구 조화)
    # A. 적중한 것만 필터링
    win_candidates = subset[(subset["direction"] != 0) & (subset["strat_return"] > 0)].copy()
    
    # B. 중복 제거 (같은 종목이면 수익률이 가장 높은 1개만 남김)
    win_candidates = win_candidates.sort_values("strat_return", ascending=False)
    win_candidates = win_candidates.drop_duplicates(subset=["code"], keep="first")

    final_picks = []

    # C. 전략적 선발 (Legend 3 + Rising 2)
    if not win_candidates.empty:
        # 1) Rising Stars: 최근 20일 이내 포착된 놈 중 베스트
        recent_mask = win_candidates["days_elapsed"] <= 20
        rising_candidates = win_candidates[recent_mask].head(2) # 최대 2개
        
        # 2) Hall of Fame: 전체 기간 중 베스트 (Rising에 뽑힌 놈 제외)
        rising_codes = set(rising_candidates["code"])
        legend_candidates = win_candidates[~win_candidates["code"].isin(rising_codes)].head(5) # 넉넉히 뽑음
        
        # 3) 슬롯 채우기 (총 5개)
        # Rising이 있으면 먼저 넣고, 나머지는 Legend로 채움
        final_picks.extend(rising_candidates.to_dict('records'))
        
        needed = 5 - len(final_picks)
        final_picks.extend(legend_candidates.head(needed).to_dict('records'))
        
        # 4) 다시 수익률 순 정렬 (보여줄 때는 1등부터)
        final_picks.sort(key=lambda x: x["strat_return"], reverse=True)

    top_movers = []
    for r in final_picks:
        direction_icon = "📈 매수" if r["direction"] == 1 else "📉 매도"
        
        # 포맷팅
        perf_str = f"+{r['strat_return']:.1f}%"
        raw_str = f"{r['raw_return']:.1f}%"
        if r["raw_return"] > 0: raw_str = f"+{raw_str}"
        
        top_movers.append({
            "name": r["name"],
            "days": r["days_elapsed"],
            "view": direction_icon,
            "raw_move": raw_str,      
            "profit": perf_str,       
            "ref_date": r["ref_date"],
            "entry_price": r["entry_price"],
            "current_price": r["current_price"]
        })

    return {
        "ref_date": ref_d,
        "n_signals": len(subset),
        "periods": periods,
        "top_movers": top_movers
    }

def build_signalist_history_markdown(ref_date: date, lookback_days: int = 90) -> str:
    ctx = build_signalist_history_context(ref_date, lookback_days)
    ref_d = ctx["ref_date"]
    
    if ctx["n_signals"] == 0:
        return f"## Signalist History\n\n아직 분석할 데이터가 충분하지 않습니다."

    lines = []
    lines.append("## Signalist History")
    lines.append(f"기준일: {ref_d}")
    lines.append("")
    lines.append("과거 레이더에 포착된 종목들의 **추적 관찰 성과**입니다.")
    lines.append("방향성(매수/매도)이 적중했을 경우의 평균 수익률을 집계합니다.")
    lines.append("")

    if ctx["periods"]:
        lines.append("### 📊 경과 기간별 평균 성과")
        lines.append("| 경과 기간 | 적중률(Win Rate) | 평균 성과 | 샘플 수 |")
        lines.append("|---|---|---|---|")
        for label, stat in ctx["periods"].items():
            win_str = f"{stat['win']:.1f}%"
            ret_str = f"{stat['ret']:+.1f}%"
            if stat['ret'] > 0: ret_str = f"**{ret_str}** 🔴"
            lines.append(f"| {label} | {win_str} | {ret_str} | {stat['count']} |")
        lines.append("")
        lines.append("👉 _시간이 지날수록 수익이 누적되는지 확인하세요. (추세 추종 검증)_")
        lines.append("")

    if ctx["top_movers"]:
        lines.append("### 🏆 명예의 전당 (Best Calls)")
        lines.append("방향성을 정확히 예측하여 높은 수익을 낸 사례입니다. (최근 및 역대 최고)")
        lines.append("")
        lines.append("| 종목명 | 포착 경과 | 뷰(View) | 가격 변화 | 성과 (시장대비) |")
        lines.append("|---|---|---|---|---|")
        
        for r in ctx["top_movers"]:
            entry_price = int(r.get('entry_price', 0))
            curr_price = int(r.get('current_price', 0))
            price_change = f"{entry_price:,} → {curr_price:,}"
            
            market_ret = _get_market_return(r['ref_date'], ref_d)
            try:
                strat_ret = float(r['profit'].replace('%', '').replace('+', ''))
            except: strat_ret = 0.0
                
            alpha = strat_ret - market_ret
            alpha_str = f"{alpha:+.1f}%p"
            if alpha > 0: alpha_str = f"(+{alpha_str})"
            else: alpha_str = f"({alpha_str})"
            
            final_perf = f"**{r['profit']}**<br><small>{alpha_str}</small>"
            
            # New 뱃지: 20일 이내면 표시
            badge = "🆕" if r['days'] <= 20 else ""
            
            lines.append(f"| {r['name']} {badge}| D+{r['days']} | {r['view']} | {price_change} | {final_perf} |")
    else:
        lines.append("### 최근 적중 사례 없음")
        lines.append("최근 변동성 구간에서 유의미한 적중 사례가 나오지 않았습니다.")

    lines.append("")
    lines.append("_* 과거 성과가 미래 수익을 보장하지 않습니다. 매도(Short) 성과는 하락률을 수익으로 환산한 것입니다._")
    
    return "\n".join(lines)
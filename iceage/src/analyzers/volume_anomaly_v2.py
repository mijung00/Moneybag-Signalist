# iceage/src/analyzers/volume_anomaly_v2.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any

import pandas as pd

from iceage.src.data_sources.kr_price_history import (
    load_daily_prices,
    load_price_history,
    load_listing,
)


# -------------------------
# 전역 상수
# -------------------------
MIN_MARKET_CAP_WON = 80_000_000_000  # 800억 (원 단위)



# 프로젝트 루트 (C:/project/iceage)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# 유틸
# =========================

def _parse_ref_date(argv: list[str]) -> date:
    """CLI 인자로 YYYY-MM-DD 를 받는다."""
    if len(argv) < 2:
        print(
            "Usage: python -m iceage.src.analyzers.volume_anomaly_v2 YYYY-MM-DD",
            file=sys.stderr,
        )
        sys.exit(1)
    ref_date_str = argv[1]
    return datetime.strptime(ref_date_str, "%Y-%m-%d").date()


def _assign_size_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """
    체급 버킷 생성 (small / mid / large / unknown).

    - 시총 800억 미만, ETF/ETN/리츠 등은 앞에서 이미 컷되었다고 가정.
    - 여기서는 'avg_trading_value(60일 평균 거래대금)'의 누적 비중으로 체급을 나눈다.

      * large: 누적 평균 거래대금 비중 0.0 ~ 0.5
      * mid  : 0.5 ~ 0.8
      * small: 0.8 ~ 1.0
    """
    if "avg_trading_value" not in df.columns:
        df["size_bucket"] = "unknown"
        return df

    caps = df[["code", "avg_trading_value"]].dropna()
    if caps.empty:
        df["size_bucket"] = "unknown"
        return df

    caps = caps.copy()
    caps["avg_trading_value"] = pd.to_numeric(
        caps["avg_trading_value"], errors="coerce"
    )
    caps = caps.dropna(subset=["avg_trading_value"])
    if caps.empty:
        df["size_bucket"] = "unknown"
        return df

    # 평균 거래대금 내림차순 정렬
    caps = caps.sort_values("avg_trading_value", ascending=False)

    total_tv = caps["avg_trading_value"].sum()
    if total_tv <= 0:
        df["size_bucket"] = "unknown"
        return df

    # 누적 비중 (0~1)
    caps["cum_share"] = caps["avg_trading_value"].cumsum() / total_tv

    LARGE_CUTOFF = 0.60  # 상위 60% → large
    MID_CUTOFF = 0.90    # 60~90% → mid, 이후 small

    def _bucket_from_share(cum_share: float) -> str:
        if pd.isna(cum_share):
            return "unknown"
        if cum_share <= LARGE_CUTOFF:
            return "large"
        elif cum_share <= MID_CUTOFF:
            return "mid"
        else:
            return "small"

    caps["size_bucket"] = caps["cum_share"].apply(_bucket_from_share)

    # 원본 df에 size_bucket merge
    df = df.copy()
    df = df.merge(
        caps[["code", "size_bucket"]],
        on="code",
        how="left",
        suffixes=("", "_bucket"),
    )
    df["size_bucket"] = df["size_bucket"].fillna("unknown")

    return df

def _compute_volume_patterns(
    hist: pd.DataFrame,
    ref_date: date,
    window_days: int = 20,
) -> pd.DataFrame:
    """
    최근 window_days 동안의 거래량 패턴을 코드별로 라벨링.

    패턴 정의(예시):
      - steady_accumulation : 최근 5일 vol_z 평균 >= +1.0, 최소값 > 0
      - sudden_spike        : 오늘 직전 10일 max vol_z < +1.0 이면서, 최근일 vol_z >= +2.5
      - boom_and_fade       : 최근 5일 내 max vol_z >= +2.5 이고, 최근일 vol_z <= 0
      - dead_silent         : 20일 평균 vol_z <= -0.5 이고, 20일 max vol_z < 0
      - 나머지              : normal
    """
    if hist.empty:
        return pd.DataFrame(columns=["code", "pattern_label"])

    # ref_date 이전 데이터만 사용
    past = hist[hist["trade_date"] < ref_date].copy()
    if past.empty:
        return pd.DataFrame(columns=["code", "pattern_label"])

    # 최근 window_days 일만 사용 (캘린더 기준)
    cutoff = ref_date - timedelta(days=window_days)
    past = past[past["trade_date"] >= cutoff].copy()
    if past.empty:
        return pd.DataFrame(columns=["code", "pattern_label"])

    # 코드/날짜 순서 정렬
    past = past.sort_values(["code", "trade_date"]).copy()

    records = []

    for code, g in past.groupby("code"):
        g = g.copy()
        # 표준화: 각 코드별로 20일 기준 z-score
        vols = g["volume"].astype(float)
        mean_v = vols.mean()
        std_v = vols.std(ddof=0)
        if std_v == 0 or pd.isna(std_v):
            pattern = "unknown"
        else:
            g["vol_z"] = (vols - mean_v) / std_v
            z = g["vol_z"]

            if len(z) < 5:
                pattern = "unknown"
            else:
                last = z.iloc[-1]
                recent5 = z.iloc[-5:]
                prev10 = z.iloc[:-1].iloc[-10:] if len(z) > 1 else pd.Series([], dtype=float)

                # 조건들
                if (
                    last >= 2.5
                    and (prev10.empty or prev10.max() < 1.0)
                ):
                    pattern = "sudden_spike"
                elif recent5.max() >= 2.5 and last <= 0:
                    pattern = "boom_and_fade"
                elif recent5.mean() >= 1.0 and recent5.min() > 0:
                    pattern = "steady_accumulation"
                elif z.mean() <= -0.5 and z.max() < 0:
                    pattern = "dead_silent"
                else:
                    pattern = "normal"

        records.append({"code": code, "pattern_label": pattern})

    return pd.DataFrame(records)

def _assign_signal_tone(row: pd.Series) -> dict:
    """
    tv_z_rel + change_rate + 시장 레짐을 이용해서
    시그널 톤(🟢/⚪/🔴)과 강도(0~3)를 지정.
    """
    tv_rel = row.get("tv_z_rel")
    regime = row.get("market_regime")
    chg = row.get("change_rate")

    try:
        tv_rel = float(tv_rel)
    except Exception:
        tv_rel = float("nan")

    try:
        chg = float(chg) if chg is not None else 0.0
    except Exception:
        chg = 0.0

    # 기본값
    tone = "⚪"
    strength = 0

    if pd.isna(tv_rel):
        return {"signal_tone": tone, "signal_strength": strength}

    # 강한 괴리: tv_z_rel >= 2.0
    if tv_rel >= 2.5:
        # 이미 당일 급등(예: +8% 이상)이면 과열 경고 느낌으로 🔴
        if chg >= 8.0:
            tone = "🔴"
        else:
            tone = "🟢"
        strength = 3
    elif tv_rel >= 1.5:
        tone = "🟢"
        strength = 2
    elif tv_rel >= 1.0:
        tone = "🟢"
        strength = 1
    elif tv_rel >= 0.5:
        tone = "⚪"
        strength = 1
    else:
        tone = "⚪"
        strength = 0

    # 시장이 공포(panic)인데 강한 음수 수익률이면, 보수적으로 톤을 한 단계 낮출 수도 있음
    if regime == "panic" and chg <= -5.0 and strength >= 2:
        strength = max(1, strength - 1)

    return {"signal_tone": tone, "signal_strength": strength}


# =========================
# 시장 레짐 계산
# =========================

def _compute_market_regime(
    hist: pd.DataFrame,
    today: pd.DataFrame,
    ref_date: date,
) -> Dict[str, Any]:
    """
    과거 히스토리(hist)와 오늘 데이터(today)를 이용해
    '시장 전체' 기준 거래대금/수익률 레짐을 계산한다.

    반환 값 예시:
    {
      "market_tv_today": float,
      "market_tv_mean": float,
      "market_tv_std": float,
      "market_tv_z": float,
      "market_ret_today": float,
      "market_ret_mean": float,
      "market_ret_std": float,
      "market_ret_z": float,
      "market_regime": "panic" | "euphoria" | "calm" | "normal",
    }
    """
    # 과거(오늘 이전) 데이터만 사용
    past = hist[hist["trade_date"] < ref_date].copy()
    if past.empty:
        # 과거 데이터가 없으면 레짐 계산 불가 → 전부 NaN
        return {
            "market_tv_today": float("nan"),
            "market_tv_mean": float("nan"),
            "market_tv_std": float("nan"),
            "market_tv_z": float("nan"),
            "market_ret_today": float("nan"),
            "market_ret_mean": float("nan"),
            "market_ret_std": float("nan"),
            "market_ret_z": float("nan"),
            "market_regime": "unknown",
        }

    # ------------------------
    # 1) 날짜별 시장 거래대금 (전체 합)
    # ------------------------
    past_tv = (
        past.groupby("trade_date")["trading_value"]
        .sum()
        .rename("market_tv")
        .sort_index()
    )

    tv_mean = past_tv.mean()
    tv_std = past_tv.std()

    tv_today = today["trading_value"].sum()
    if tv_std and tv_std > 0:
        tv_z = (tv_today - tv_mean) / tv_std
    else:
        tv_z = float("nan")

    # ------------------------
    # 2) 날짜별 시장 수익률 (시총 가중 평균)
    # ------------------------
    # 과거
    past = past.copy()
    # 날짜별 total mcap
    total_mcap_by_date = (
        past.groupby("trade_date")["market_cap"]
        .sum()
        .rename("total_mcap")
    )
    past = past.merge(total_mcap_by_date, on="trade_date", how="left")

    # weight = 각 종목 시총 / 전체 시총
    past["w_mcap"] = past["market_cap"] / past["total_mcap"]

    # 각 날짜별 시총 가중 수익률
    # change_rate 가 %, 소수 등 어떤 포맷인지에 따라 해석은 다를 수 있지만
    # 여기서는 '상대 비교용'이므로 일관성만 보장되면 됨.
    past_ret = (
        (past["change_rate"] * past["w_mcap"])
        .groupby(past["trade_date"])
        .sum()
        .rename("market_ret")
        .sort_index()
    )

    ret_mean = past_ret.mean()
    ret_std = past_ret.std()

    # 오늘도 같은 방식으로
    today = today.copy()
    total_mcap_today = today["market_cap"].sum()
    if total_mcap_today and total_mcap_today > 0:
        today["w_mcap"] = today["market_cap"] / total_mcap_today
        ret_today = float((today["change_rate"] * today["w_mcap"]).sum())
    else:
        ret_today = float("nan")

    if ret_std and ret_std > 0:
        ret_z = (ret_today - ret_mean) / ret_std
    else:
        ret_z = float("nan")

    # ------------------------
    # 3) 시장 레짐 라벨링
    # ------------------------
    regime = "normal"

    # 기준은 대략적인 heuristic (나중에 경험 쌓이면서 튜닝)
    if not pd.isna(ret_z) and not pd.isna(tv_z):
        if ret_z <= -1.5 and tv_z >= 1.0:
            regime = "panic"      # 공포/투매
        elif ret_z >= 1.5 and tv_z >= 1.0:
            regime = "euphoria"   # 과열/환호
        elif abs(ret_z) <= 0.5 and abs(tv_z) <= 0.5:
            regime = "calm"       # 아주 평온
        else:
            regime = "normal"
    else:
        regime = "unknown"

    return {
        "market_tv_today": float(tv_today),
        "market_tv_mean": float(tv_mean),
        "market_tv_std": float(tv_std if tv_std == tv_std else 0.0),
        "market_tv_z": float(tv_z) if tv_z == tv_z else float("nan"),
        "market_ret_today": float(ret_today),
        "market_ret_mean": float(ret_mean),
        "market_ret_std": float(ret_std if ret_std == ret_std else 0.0),
        "market_ret_z": float(ret_z) if ret_z == ret_z else float("nan"),
        "market_regime": regime,
    }


# =========================
# 괴리율 v2 메인 로직
# =========================

def run_volume_anomaly_v2(
    ref_date: date,
    window_days: int = 60,
    min_history_days: int = 20,
    top_n_per_bucket: int = 30,
) -> Path:
    """
    ref_date 기준으로 최근 window_days 영업일의 거래대금/거래량 괴리율을 계산한다.

    핵심 포인트:
      - 개별 종목의 거래대금 괴리율(tv_z)을 자기 과거 대비로 계산
      - 동시에 시장 전체 거래대금 z-score(market_tv_z)를 계산
      - 종목별 '시장 대비' z-score: tv_z_rel = tv_z - market_tv_z
      - 시가총액 기반 체급(bucket)별 상위 N개를 is_top_bucket=True로 태깅

    출력:
      - data/processed/volume_anomaly_v2_YYYY-MM-DD.csv
    """

    ref_date_str = ref_date.strftime("%Y-%m-%d")
    print(
        f"[INFO] 괴리율 v2 계산 시작: ref_date={ref_date_str}, "
        f"window={window_days}d, min_history_days={min_history_days}"
    )

    # 1) 데이터 로드
    hist = load_price_history(ref_date, window_days=window_days)
    today = load_daily_prices(ref_date)
    listing = load_listing(ref_date)

    # 2) 보통주 필터링 (리스팅 기준)
    listing = listing.copy()
    if "stock_kind" in listing.columns:
        listing = listing[listing["stock_kind"] == "보통주"].copy()

    # 3) 오늘 시세에 리스팅 정보 조인 (섹터 / 상장주식수 / 종목구분 등)
    merge_cols = [
        "code",
        "stock_kind",
        "sector_name",
        "listed_shares",
        "security_group",  # ETF/ETN/리츠 제거용
    ]
    merge_cols = [c for c in merge_cols if c in listing.columns]

    today = today.merge(
        listing[merge_cols].drop_duplicates("code"),
        on="code",
        how="left",
    )

    # 혹시 시세 쪽에 stock_kind가 있다면 보통주만 사용
    if "stock_kind" in today.columns:
        today = today[today["stock_kind"] == "보통주"].copy()

    # ETF / ETN / 리츠 제거
    if "security_group" in today.columns:
        etf_like_groups = ["ETF", "ETN", "리츠"]
        before_cnt = len(today)
        today = today[~today["security_group"].isin(etf_like_groups)].copy()
        after_cnt = len(today)
        print(f"[INFO] ETF/ETN/리츠 제거: {before_cnt} → {after_cnt} 종목")


    # 4) 히스토리에서 당일(ref_date) 제거 후, 과거 N일만으로 통계 계산
    hist_past = hist[hist["trade_date"] < ref_date].copy()
    if hist_past.empty:
        raise RuntimeError(
            f"[ERROR] {ref_date_str} 기준으로 과거 데이터가 없어 "
            "괴리율 v2를 계산할 수 없습니다."
        )

    grouped = hist_past.groupby("code")

    stats = grouped.agg(
        history_days=("trade_date", "nunique"),
        avg_trading_value=("trading_value", "mean"),
        std_trading_value=("trading_value", "std"),
        avg_volume=("volume", "mean"),
        std_volume=("volume", "std"),
    ).reset_index()

    # 5) 오늘 데이터 + 히스토리 통계 조인
    df = today.merge(
        stats,
        on="code",
        how="left",
        suffixes=("", "_hist"),
    )

    # 5-1) 시총 하한선 필터 (800억 미만 컷)
    if "market_cap" in df.columns:
        df = df.copy()
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
        before_cnt = len(df)
        df = df[df["market_cap"] >= MIN_MARKET_CAP_WON].copy()
        after_cnt = len(df)
        print(
            f"[INFO] 시총 {MIN_MARKET_CAP_WON:,.0f}원 이상 종목만 사용: "
            f"{before_cnt} → {after_cnt}"
        )

    # 5-2) 충분한 히스토리/거래대금이 없는 종목 제외
    df = df[
        (df["history_days"].fillna(0) >= min_history_days)
        & (df["avg_trading_value"].fillna(0) > 0)
    ].copy()

    if df.empty:
        raise RuntimeError(
            f"[ERROR] {ref_date_str} 기준으로 "
            "조건(history_days/min_history_days)에 맞는 종목이 없습니다."
        )

    # 6) 개별 종목 거래대금 기준 괴리율 (비율 + z-score)
    # [개선] .apply 대신 벡터 연산을 사용하여 성능 대폭 향상
    # 분모가 0인 경우를 대비하여 np.divide 사용
    import numpy as np
    df['tv_ratio'] = np.divide(df['trading_value'], df['avg_trading_value'], 
                             out=np.full_like(df['trading_value'], np.nan, dtype=float), 
                             where=df['avg_trading_value'] > 0)
    
    df['tv_z'] = np.divide(df['trading_value'] - df['avg_trading_value'], df['std_trading_value'],
                         out=np.full_like(df['trading_value'], np.nan, dtype=float),
                         where=df['std_trading_value'] > 0)

    df['vol_ratio'] = np.divide(df['volume'], df['avg_volume'],
                              out=np.full_like(df['volume'], np.nan, dtype=float),
                              where=df['avg_volume'] > 0)

    df['vol_z'] = np.divide(df['volume'] - df['avg_volume'], df['std_volume'],
                          out=np.full_like(df['volume'], np.nan, dtype=float),
                          where=df['std_volume'] > 0)

    # 7) 시장 레짐 계산 (전체 시장 기준)
    market_info = _compute_market_regime(hist, today, ref_date)
    market_tv_z = market_info.get("market_tv_z")

    # 8) 시장 대비 괴리율 (tv_z_rel)
    if not pd.isna(market_tv_z):
        df["tv_z_rel"] = df["tv_z"] - market_tv_z
    else:
        # 시장 z-score가 없으면 그냥 tv_z 그대로 사용
        df["tv_z_rel"] = df["tv_z"]

    # 9) 시가총액 기반 체급 (small/mid/large)
    df = _assign_size_bucket(df)

    # 10) 랭킹 계산 (시장 대비 tv_z_rel 기준)
    df = df.sort_values("tv_z_rel", ascending=False)
    df["rank_overall"] = df["tv_z_rel"].rank(method="dense", ascending=False)

    # 체급별 랭킹
    df["rank_in_bucket"] = df.groupby("size_bucket")["tv_z_rel"].rank(
        method="dense", ascending=False
    )

    # 상위 일부만 태깅 (LLM/뉴스레터에서 우선적으로 사용)
    df["is_top_bucket"] = df["rank_in_bucket"] <= top_n_per_bucket
    
        # 10-1) 최근 20일 거래량 패턴 라벨링
    pattern_df = _compute_volume_patterns(hist, ref_date, window_days=20)
    if not pattern_df.empty:
        df = df.merge(pattern_df, on="code", how="left")
    else:
        df["pattern_label"] = "unknown"

    # 10-2) 시그널 톤/강도 계산
    tone_info = df.apply(_assign_signal_tone, axis=1, result_type="expand")
    df["signal_tone"] = tone_info["signal_tone"]
    df["signal_strength"] = tone_info["signal_strength"]


    # 11) 메타 정보(시장 레짐)를 모든 행에 붙이기
    df["market_tv_today"] = market_info.get("market_tv_today")
    df["market_tv_mean"] = market_info.get("market_tv_mean")
    df["market_tv_std"] = market_info.get("market_tv_std")
    df["market_tv_z"] = market_info.get("market_tv_z")

    df["market_ret_today"] = market_info.get("market_ret_today")
    df["market_ret_mean"] = market_info.get("market_ret_mean")
    df["market_ret_std"] = market_info.get("market_ret_std")
    df["market_ret_z"] = market_info.get("market_ret_z")

    df["market_regime"] = market_info.get("market_regime")
    df["window_days"] = window_days
    df["min_history_days"] = min_history_days

    # 12) 저장
    out_path = DATA_PROCESSED_DIR / f"volume_anomaly_v2_{ref_date_str}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    total_cnt = len(df)
    top_cnt = int(df["is_top_bucket"].sum())

    print(
        f"[OK] 괴리율 v2 계산 완료: {ref_date_str} "
        f"(전체 {total_cnt}종목, 체급별 상위 {top_cnt}종목 is_top_bucket=True)"
    )
    print(
        f"[INFO] 시장 레짐: regime={market_info.get('market_regime')}, "
        f"tv_z={market_info.get('market_tv_z'):.2f} "
        f"ret_z={market_info.get('market_ret_z'):.2f}"
        if not pd.isna(market_info.get("market_tv_z"))
        and not pd.isna(market_info.get("market_ret_z"))
        else f"[INFO] 시장 레짐 계산 불가 (데이터 부족)"
    )
    print(f"[OK] 저장 경로: {out_path}")

    return out_path


def main():
    ref_date = _parse_ref_date(sys.argv)
    run_volume_anomaly_v2(ref_date)


if __name__ == "__main__":
    main()

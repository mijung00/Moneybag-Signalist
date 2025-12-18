# iceage/src/data_sources/market_snapshot.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Dict, Tuple, Optional

import requests
import yfinance as yf

# 야후 티커 매핑
INDEX_TICKERS = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
    "Dow Jones": "^DJI",
    "KOSPI": "^KS11",
    "KOSDAQ": "^KQ11",  # ✅ 추가: 코스닥 종합지수
}

FX_TICKERS = {
    "USD/KRW": "USDKRW=X",
    "USD/JPY": "USDJPY=X",
    "DXY": "DX-Y.NYB",
}

COMMODITY_TICKERS = {
    "WTI": "CL=F",
    "Brent": "BZ=F",
    "Gold": "GC=F",
}

CRYPTO_TICKERS = {
    "BTC/USD": "BTC-USD",
    "ETH/USD": "ETH-USD",
}


def _fetch_one(ticker: str, ref: date) -> Optional[Tuple[float, float]]:
    """
    ref 기준(보통 전 영업일)의 종가와 전일 대비 %를 반환.
    ref에 데이터가 없으면 최대 10일 전까지 거슬러 올라가서 최근 값을 사용.
    네트워크 등 일시적 오류에 대비해 3회 재시도 로직 추가.
    """
    session = requests.Session()
    session.headers[
        "User-Agent"
    ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    df = None
    for attempt in range(3):
        try:
            df = yf.Ticker(ticker, session=session).history(
                period="1mo", auto_adjust=False
            )
            if not df.empty:
                break  # 성공 시 루프 탈출
        except Exception as e:
            logging.error(
                f"YFINANCE_EXCEPTION for ticker {ticker} (attempt {attempt + 1}/3): {e}"
            )

        if df is None or df.empty:
            logging.warning(
                f"YFINANCE_EMPTY for ticker {ticker} (attempt {attempt + 1}/3). Retrying in 2s..."
            )
            time.sleep(2)
        else:  # df가 비어있지 않으면 break
            break

    if df is None or df.empty:
        logging.error(f"YFINANCE_FAILED for ticker {ticker} after 3 attempts.")
        return None

    df = df.tz_localize(None)
    df["d"] = df.index.date

    back = 0
    target_idx = None
    while back < 10:
        try_d = ref - timedelta(days=back)
        rows = df[df["d"] == try_d]
        if not rows.empty:
            target_idx = rows.index[-1]
            break
        back += 1

    if target_idx is None:
        return None

    loc = df.index.get_loc(target_idx)
    if isinstance(loc, slice):
        loc = loc.stop - 1

    prev_idx = df.index[loc - 1] if loc > 0 else None
    close = float(df.loc[target_idx, "Close"])

    if prev_idx is not None:
        prev_close = float(df.loc[prev_idx, "Close"])
        chg_pct = (close / prev_close - 1.0) * 100.0
    else:
        chg_pct = 0.0

    return round(close, 4), round(chg_pct, 2)


def get_market_overview(ref_date: date) -> Dict[str, Dict[str, Tuple[float, float]]]:
    """
    {"indices": {...}, "fx": {...}, "commodities": {...}, "crypto": {...}}
    형태로 반환.
    """
    out = {"indices": {}, "fx": {}, "commodities": {}, "crypto": {}}

    for name, t in INDEX_TICKERS.items():
        v = _fetch_one(t, ref_date)
        if v:
            out["indices"][name] = v

    for name, t in FX_TICKERS.items():
        v = _fetch_one(t, ref_date)
        if v:
            out["fx"][name] = v

    for name, t in COMMODITY_TICKERS.items():
        v = _fetch_one(t, ref_date)
        if v:
            out["commodities"][name] = v

    for name, t in CRYPTO_TICKERS.items():
        v = _fetch_one(t, ref_date)
        if v:
            out["crypto"][name] = v

    return out


def _format_value(category: str, value: float) -> str:
    """
    카테고리별 숫자 포맷統一 (지수/환율/원자재/코인 모두 소수점 2자리).
    """
    return f"{value:,.2f}"


def format_for_markdown(
    snapshot: Dict[str, Dict[str, Tuple[float, float]]]
) -> str:
    """
    마크다운 요약 생성.
    값이 없는 카테고리는 아예 출력하지 않음.
    """
    lines = []

    def add_block(title: str, key: str):
        data = snapshot.get(key, {})
        if not data:
            return
        lines.append(f"### {title}")
        for name, (val, pct) in data.items():
            val_str = _format_value(key, val)
            sign = "+" if pct >= 0 else ""
            lines.append(f"- {name}: {val_str} ({sign}{pct}%)")
        lines.append("")

    add_block("지수", "indices")
    add_block("환율", "fx")
    add_block("원자재", "commodities")
    add_block("암호화폐", "crypto")

    return "\n".join(lines).strip()

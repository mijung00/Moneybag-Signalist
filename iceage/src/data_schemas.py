# iceage/src/data_schemas.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import List


# 한국 주식 일별 시세 표준 컬럼 정의
KR_PRICE_COLUMNS: List[str] = [
    "code",            # 종목코드 (문자열, 6자리)
    "name",            # 종목명
    "market",          # 시장 (KOSPI / KOSDAQ 등)
    "date",            # 날짜 (YYYY-MM-DD 문자열)
    "open",
    "high",
    "low",
    "close",
    "prev_close",      # 전일 종가
    "change_pct",      # 등락률 (%)
    "volume",          # 거래량
    "avg_20d_volume",  # 최근 20일 평균 거래량 (없으면 volume으로 대체)
]


KR_PRICE_COLUMNS: List[str] = [
    "code",            # 종목코드 (옵션)
    "name",            # 종목명
    "market",          # 시장 (옵션)
    "date",            # 날짜 (YYYY-MM-DD)
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "change_pct",
    "volume",
    "avg_20d_volume",
]


def validate_kr_price_columns(cols: List[str]) -> bool:
    """
    최소한 name/close/volume 만 있어도 동작하게 완화.
    """
    required = {"name", "close", "volume"}
    return required.issubset(set(cols))
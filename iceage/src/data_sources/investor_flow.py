# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date
from pathlib import Path
from typing import Dict

import pandas as pd


@dataclass
class InvestorFlowSummary:
    market: str
    net_by_investor: Dict[str, float]


def _parse_number(x) -> float:
    """네이버 표에서 온 문자열 숫자를 float으로 변환."""
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "" or s == "-":
        return 0.0
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def load_investor_flow(ref_date: _date) -> Dict[str, InvestorFlowSummary]:
    """
    raw CSV (kr_investor_flow_YYYY-MM-DD.csv)를 읽어
    시장별(코스피/코스닥) 개인·외국인·기관 순매수 합계를 반환.
    기관은 '기관' 컬럼이 없으면 금융투자/투신/연기금 등 합산.
    """
    path = (
        Path("iceage")
        / "data"
        / "raw"
        / f"kr_investor_flow_{ref_date.isoformat()}.csv"
    )
    if not path.exists():
        return {}

    df = pd.read_csv(path, dtype=str)

    if "market_label" not in df.columns:
        return {}

    summaries: Dict[str, InvestorFlowSummary] = {}

    for _, row in df.iterrows():
        market = str(row["market_label"])

        personal = _parse_number(row.get("개인"))
        foreign = _parse_number(row.get("외국인"))

        # 기관 계열: '기관' 컬럼이 있으면 우선 사용,
        # 없으면 나머지 기관 계열(금융투자, 투신, 연기금 등)을 합산
        inst = 0.0
        if "기관" in df.columns:
            inst = _parse_number(row.get("기관"))
        else:
            for col in df.columns:
                if col in ("날짜", "market_label", "개인", "외국인"):
                    continue
                inst += _parse_number(row.get(col))

        summaries[market] = InvestorFlowSummary(
            market=market,
            net_by_investor={
                "개인": personal,
                "외국인": foreign,
                "기관": inst,
            },
        )

    return summaries

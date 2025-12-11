# iceage/src/data_sources/signalist_today.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd


@dataclass
class SignalRow:
    name: str
    close: int
    vol_sigma: float
    sentiment: str
    event: str
    insight: str


def _csv_path(ref_date: date) -> Path:
    # 예: iceage/data/signalist_today_2025-10-31.csv
    return Path("iceage") / "data" / f"signalist_today_{ref_date.isoformat()}.csv"


def load_signalist_today(ref_date: date) -> List[SignalRow]:
    """
    ref_date 기준 CSV 파일을 읽어 SignalRow 리스트로 변환.
    파일이 없거나 오류가 나면 빈 리스트 반환.
    """
    path = _csv_path(ref_date)
    if not path.exists():
        return []

    try:
        df = pd.read_csv(path)
    except Exception:
        return []

    rows: List[SignalRow] = []

    for _, row in df.iterrows():
        try:
            rows.append(
                SignalRow(
                    name=str(row["name"]),
                    close=int(row["close"]),
                    vol_sigma=float(row["vol_sigma"]),
                    sentiment=str(row["sentiment"]),
                    event=str(row.get("event", "")),
                    insight=str(row.get("insight", "")),
                )
            )
        except Exception:
            # 한 줄 에러나도 전체는 멈추지 않게 무시
            continue

    return rows

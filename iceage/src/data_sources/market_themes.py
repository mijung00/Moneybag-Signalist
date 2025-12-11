# iceage/src/data_sources/market_themes.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List


@dataclass
class MarketThemeSummary:
    name: str
    mention_count: int
    avg_return: float
    top_stocks: List[str]


def get_market_themes(ref_date: date) -> List[MarketThemeSummary]:
    path = Path("iceage") / "data" / "processed" / f"market_themes_{ref_date.isoformat()}.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [MarketThemeSummary(**t) for t in raw]

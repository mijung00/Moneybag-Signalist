# iceage/src/data_sources/sector_themes.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List


@dataclass
class SectorThemeSummary:
    sector: str
    avg_return: float
    breadth: float
    turnover_sum: float
    score: float
    top_stocks: List[str]


def get_sector_themes(ref_date: date) -> List[SectorThemeSummary]:
    path = Path("iceage") / "data" / "processed" / f"kr_sector_themes_{ref_date.isoformat()}.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [SectorThemeSummary(**r) for r in raw]

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
from typing import List

@dataclass
class GlobalHeadlineSummary:
    region: str
    topic: str
    headline: str
    detail: str
    risk_tone: str  # "risk_on", "risk_off", "neutral"

def get_global_minute(ref_date: date) -> List[GlobalHeadlineSummary]:
    path = Path("iceage") / "data" / "processed" / f"global_minute_{ref_date}.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [GlobalHeadlineSummary(**h) for h in raw]

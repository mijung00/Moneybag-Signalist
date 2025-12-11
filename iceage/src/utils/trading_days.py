# -*- coding: utf-8 -*-
"""
iceage.src.utils.trading_days
-----------------------------
KRX 영업일 캘린더 유틸 (Asia/Seoul 기준).

환경 변수:
- ICEAGE_CALENDAR_PATH : 커스텀 캘린더 JSON 경로 지정(옵션)
- FORCE_REFERENCE_DATE  : YYYY-MM-DD, 기준일 강제(옵션)
- ALLOW_RUN_NON_BUSINESS: 1이면 비영업일에도 실행 허용(옵션)
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Set, Optional
import json, os
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

DEFAULT_CALENDAR_PATH = os.getenv(
    "ICEAGE_CALENDAR_PATH",
    "iceage/configs/calendar/business_days_2025_2026.json"
)

@dataclass(frozen=True)
class CalendarConfig:
    json_path: str = DEFAULT_CALENDAR_PATH

class TradingCalendar:
    def __init__(self, cfg: CalendarConfig = CalendarConfig()):
        self.cfg = cfg
        self._business_days: Set[date] = set()
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.cfg.json_path):
            raise FileNotFoundError(f"캘린더 파일이 없습니다: {self.cfg.json_path}")
        with open(self.cfg.json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        weekends_removed = data.get("business_days_weekends_removed", {})
        for _, days in weekends_removed.items():
            for iso in days:
                self._business_days.add(date.fromisoformat(iso))

        holidays = data.get("holidays", {})
        for _, holis in holidays.items():
            for iso in holis:
                d = date.fromisoformat(iso)
                self._business_days.discard(d)

    def is_business_day(self, d: date) -> bool:
        return d in self._business_days

    def previous_business_day(self, d: date, n: int = 1) -> date:
        cur = d
        count = 0
        while count < n:
            cur -= timedelta(days=1)
            if self.is_business_day(cur):
                count += 1
        return cur

    def next_business_day(self, d: date, n: int = 1) -> date:
        cur = d
        count = 0
        while count < n:
            cur += timedelta(days=1)
            if self.is_business_day(cur):
                count += 1
        return cur

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def compute_reference_date(cal: TradingCalendar, when: Optional[datetime] = None) -> date:
    forced = os.getenv("FORCE_REFERENCE_DATE")
    if forced:
        return date.fromisoformat(forced)
    dt = when or now_kst()
    return cal.previous_business_day(dt.date(), 1)

def may_run_today(cal: TradingCalendar, when: Optional[datetime] = None) -> bool:
    dt = when or now_kst()
    if os.getenv("ALLOW_RUN_NON_BUSINESS") == "1":
        return True
    return cal.is_business_day(dt.date())

if __name__ == "__main__":
    cal = TradingCalendar()
    now = now_kst()
    print("[KST now]", now)
    print("[can run today?]", may_run_today(cal, now))
    print("[reference date]", compute_reference_date(cal, now))

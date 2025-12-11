# iceage/src/data_sources/signalist_today_stub.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List


@dataclass
class SignalRow:
    name: str        # 종목명
    close: int       # 종가
    vol_sigma: float # 거래량 괴리율 (σ 단위)
    sentiment: str   # 심리지수 (아이콘 + 텍스트)
    event: str       # 내부 이벤트(공시/IR 등)
    insight: str     # 시그널 해석 문장


def get_signalist_today(ref_date: date) -> List[SignalRow]:
    """
    나중에는 여기서 진짜 시그널 엔진 결과를 가져오면 됨.
    지금은 예시 데이터만 리턴.
    """
    return [
        SignalRow(
            name="삼성전자",
            close=78200,
            vol_sigma=2.8,
            sentiment="🟢 강세",
            event="-",
            insight="AI 반도체 테마 강세. 외국인 순매수세 지속.",
        ),
        SignalRow(
            name="POSCO홀딩스",
            close=554000,
            vol_sigma=3.4,
            sentiment="🟡 보합",
            event="신규 IR 발표",
            insight="2차전지 원료 수급 이슈로 단기 과열. 주의 요망.",
        ),
        SignalRow(
            name="한화시스템",
            close=24950,
            vol_sigma=5.1,
            sentiment="🔴 과열",
            event="방산 수출 계약",
            insight="방산·위성 테마 강세 지속. 단기 조정 가능성.",
        ),
    ]

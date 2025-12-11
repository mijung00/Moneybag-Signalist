# iceage/src/data_sources/market_themes_stub.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List


@dataclass
class MarketTheme:
    name: str          # 테마 이름 (예: AI 반도체)
    summary: str       # 한 줄 요약
    drivers: str       # 촉발 요인 (뉴스/이벤트)
    example_stocks: str  # 대표 종목


def get_market_themes(ref_date: date) -> List[MarketTheme]:
    """
    나중에는 뉴스/섹터 데이터에서 자동 생성.
    지금은 예시 스텁.
    """
    return [
        MarketTheme(
            name="AI 반도체",
            summary="엔비디아·TSMC 실적 기대와 함께 국내 AI 서버/메모리 관련주에 매수세 유입.",
            drivers="해외 AI 투자 확대, 데이터센터 증설 뉴스.",
            example_stocks="삼성전자, SK하이닉스, 한미반도체",
        ),
        MarketTheme(
            name="방산·우주",
            summary="중동/유럽 국방비 확대와 위성 발사 모멘텀으로 방산·위성 관련주 강세.",
            drivers="해외 방산 계약, 군비 증액 뉴스.",
            example_stocks="한화에어로스페이스, 한화시스템",
        ),
        MarketTheme(
            name="건설·인프라",
            summary="국내 SOC 투자 재개 기대와 분양 회복 기대감으로 대형 건설주에 수급 유입.",
            drivers="정부 예산, 인프라 투자 계획 기사.",
            example_stocks="현대건설, DL이앤씨",
        ),
    ]

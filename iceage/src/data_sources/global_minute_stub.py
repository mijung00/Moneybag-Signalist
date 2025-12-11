# iceage/src/data_sources/global_minute_stub.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List


@dataclass
class GlobalHeadline:
    region: str   # 지역 (US / China / Europe ...)
    title: str    # 한 줄 제목
    impact: str   # 시장에 어떤 영향을 주는지


def get_global_minute(ref_date: date) -> List[GlobalHeadline]:
    """
    나중에는 SERPAPI + 매크로 캘린더와 연동.
    지금은 예시 스텁.
    """
    return [
        GlobalHeadline(
            region="US",
            title="미국 국채금리 완화, 성장주 밸류에이션 부담 완화",
            impact="장기 금리가 내려오면서 기술주·성장주에 우호적인 환경이 조성되고 있습니다.",
        ),
        GlobalHeadline(
            region="China",
            title="중국 제조 PMI 50.2, 기준선 상회",
            impact="제조업 심리 개선 신호로, 원자재·수출 민감 섹터에 긍정적으로 해석됩니다.",
        ),
        GlobalHeadline(
            region="Europe",
            title="유럽 에너지 재고 안정, 가스 가격 변동성 둔화",
            impact="에너지 비용 압력이 완화되며 유럽 경기침체 우려가 다소 완화되고 있습니다.",
        ),
    ]

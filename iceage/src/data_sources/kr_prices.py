# iceage/src/data_sources/kr_prices.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from iceage.src.processors.kr_prices_normalizer import ensure_normalized


def load_normalized_prices(ref_date: date) -> pd.DataFrame:
    """
    ref_date 기준 표준 시세 데이터 로드.
    - processed 파일 없으면 raw에서 normalize 후 읽음.
    """
    processed_path: Path = ensure_normalized(ref_date)
    return pd.read_csv(processed_path)

# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from pathlib import Path
from datetime import date as _date

def detect_signals_from_prices(ref: _date):
    """
    volume_anomaly_v2 결과 기반으로 시그널 리스트를 생성
    (뉴스레터에서 사용되는 detect_signals_from_prices 대체 버전)
    """
    path = Path("iceage") / "data" / "processed" / f"volume_anomaly_v2_{ref.isoformat()}.csv"
    if not path.exists():
        print(f"[WARN] volume_anomaly_v2 파일 없음: {path}")
        return []

    df = pd.read_csv(path)

    # 상위 버킷만 사용
    df = df[df["is_top_bucket"] == True].copy()

    # SignalRow 형태 흉내내기
    results = []
    for _, r in df.iterrows():
        row = type(
            "SignalRow",
            (),
            dict(
                name=r["name"],
                code=r["code"],
                close=r["close"],
                vol_sigma=r["tv_z"],  # 거래대금 기반 z-score 사용
                sentiment="",         # 뉴스 기반 감정 지표 자리
                insight="",           # LLM 코멘트 자리
                sector=getattr(r, "sector_name_y", ""),
            ),
        )()
        results.append(row)

    return results

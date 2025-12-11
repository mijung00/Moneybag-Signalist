# iceage/src/processors/kr_themes_detector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import List, Dict

import pandas as pd

from iceage.src.data_schemas import KR_PRICE_COLUMNS  # 이미 있음 (안 써도 상관없음)


@dataclass
class ThemeAggregate:
    name: str
    mention_count: int
    avg_return: float
    top_stocks: List[str]


def _cleaned_news_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "processed" / f"kr_news_cleaned_{ref_date.isoformat()}.jsonl"


def _processed_price_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "processed" / f"kr_prices_{ref_date.isoformat()}.csv"


def _load_themes_configs() -> List[Dict]:
    cfg_path = Path("iceage") / "configs" / "themes_kr.json"
    if not cfg_path.exists():
        return []
    return json.loads(cfg_path.read_text(encoding="utf-8"))


def detect_themes(ref_date: date) -> Path:
    news_path = _cleaned_news_path(ref_date)
    if not news_path.exists():
        raise FileNotFoundError(news_path)

    themes_cfg = _load_themes_configs()
    if not themes_cfg:
        raise RuntimeError("themes_kr.json 이 비어있습니다.")

    # 뉴스 로드
    news = []
    with news_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            news.append(json.loads(line))

    # 가격 데이터 (있으면 수익률 계산, 없으면 0으로)
    price_path = _processed_price_path(ref_date)
    if price_path.exists():
        df_price = pd.read_csv(price_path)
    else:
        df_price = pd.DataFrame(columns=["name", "change_pct"])

    themes_out: List[ThemeAggregate] = []

    for t in themes_cfg:
        name = t["name"]
        keywords = t.get("keywords", [])
        stocks = t.get("stocks", [])

        # 뉴스에서 키워드 매칭
        count = 0
        for art in news:
            text = (art.get("title", "") + " " + art.get("snippet", "")).lower()
            if any(kw.lower() in text for kw in keywords):
                count += 1

        # 가격 데이터에서 해당 테마 종목 수익률 평균
        avg_ret = 0.0
        if not df_price.empty and stocks:
            # df_price의 'name' 이 우리 stocks랑 매칭된다고 가정
            sub = df_price[df_price["name"].isin(stocks)]
            if not sub.empty and "change_pct" in sub.columns:
                avg_ret = float(sub["change_pct"].mean())

        themes_out.append(
            ThemeAggregate(
                name=name,
                mention_count=count,
                avg_return=avg_ret,
                top_stocks=stocks,
            )
        )

    # 출력 저장
    out_dir = Path("iceage") / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"market_themes_{ref_date.isoformat()}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(t) for t in themes_out], f, ensure_ascii=False, indent=2)

    print(f"✅ 테마 집계 저장 완료: {out_path}")
    return out_path


if __name__ == "__main__":
    import sys
    from datetime import date as _date

    if len(sys.argv) >= 2:
        ref = _date.fromisoformat(sys.argv[1])
    else:
        ref = _date.today()

    detect_themes(ref)

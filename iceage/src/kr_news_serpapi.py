# iceage/src/collectors/kr_news_serpapi.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict

import requests
import json


SERP_ENDPOINT = "https://serpapi.com/search"


def _get_api_key() -> str:
    key = os.getenv("SERPAPI_KEY")
    if not key:
        raise RuntimeError("SERPAPI_KEY 환경변수가 설정되지 않았습니다.")
    return key


def fetch_kr_news_raw(ref_date: date, num_results: int = 50) -> List[Dict]:
    """
    SerpAPI를 이용해 국내 증시 관련 구글 뉴스 수집.
    ref_date 근처 뉴스만 뽑는 느낌으로 쿼리.
    """
    api_key = _get_api_key()

    params = {
        "engine": "google_news",
        "q": "코스피 OR 코스닥 OR 증시 OR 주식 시장",
        "hl": "ko",
        "gl": "kr",
        "api_key": api_key,
        "num": num_results,
    }

    res = requests.get(SERP_ENDPOINT, params=params, timeout=20)
    res.raise_for_status()
    data = res.json()

    articles = []
    for item in data.get("news_results", []):
        articles.append(
            {
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "source": item.get("source", {}).get("name") if isinstance(item.get("source"), dict) else item.get("source"),
                "link": item.get("link", ""),
                "date": item.get("date", ""),  # "3 hours ago" 형식일 수 있음
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )
    return articles


def save_kr_news_raw(ref_date: date) -> Path:
    articles = fetch_kr_news_raw(ref_date)

    raw_dir = Path("iceage") / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"kr_news_{ref_date.isoformat()}.jsonl"

    with path.open("w", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")

    print(f"✅ 국내 뉴스 raw 저장 완료: {path}")
    return path


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    save_kr_news_raw(ref)

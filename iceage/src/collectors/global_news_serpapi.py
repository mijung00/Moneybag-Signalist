# iceage/src/collectors/global_news_serpapi.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()

SERP_ENDPOINT = "https://serpapi.com/search"


def translate_en_to_ko(text: str) -> str:
    """
    TODO: 나중에 ChatGPT API 붙여서 자연스러운 번역으로 교체.
    지금은 구조만 맞춰두고, 일단 원문 그대로 반환.
    """
    return text or ""


# ----------------------------
# 관심도 기반 랭킹 헬퍼 함수들
# ----------------------------

_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "for", "to", "and", "or",
    "stock", "stocks", "market", "markets", "equities", "share", "shares",
    "index", "indexes", "indices", "today", "rise", "rises", "fall", "falls",
    "up", "down", "after", "as", "amid",
}

# 메이저 매체에 가중치 부여
_SOURCE_WEIGHTS: Dict[str, float] = {
    "Bloomberg": 2.0,
    "Reuters": 2.0,
    "CNBC": 2.0,
    "The Wall Street Journal": 2.0,
    "WSJ": 2.0,
    "Financial Times": 2.0,
    "FT.com": 2.0,
    "Yahoo Finance": 1.5,
    "MarketWatch": 1.5,
    "Barron's": 1.5,
}


def _normalize_text(text: str) -> List[str]:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [t for t in text.split() if t and t not in _STOPWORDS]
    return tokens


def _build_title_tokens(articles: List[Dict]) -> List[set]:
    token_sets: List[set] = []
    for art in articles:
        title = (art.get("title_en") or art.get("title") or "").strip()
        tokens = set(_normalize_text(title))
        token_sets.append(tokens)
    return token_sets


def _rank_articles_by_attention(articles: List[Dict]) -> List[Dict]:
    """
    - 여러 메이저 매체에서 반복 언급된 이슈일수록 점수 ↑
    - 소스가 유명할수록 점수 ↑
    - 결국 score 기준으로 내림차순 정렬
    """
    if not articles:
        return []

    token_sets = _build_title_tokens(articles)
    n = len(articles)

    # 1) 토큰 빈도로 대략적인 "이슈 중심 키워드" 파악
    all_tokens: Counter = Counter()
    for ts in token_sets:
        all_tokens.update(ts)

    # 자주 등장하는 토큰만 이슈 키워드로 간주
    issue_tokens = {tok for tok, cnt in all_tokens.items() if cnt >= 2}

    scores: List[float] = []
    for i, art in enumerate(articles):
        title_tokens = token_sets[i]

        # (1) 이슈 토큰과의 겹침 정도
        overlap = len(title_tokens & issue_tokens)

        # (2) 다른 기사들과의 유사도 기반 "군집 크기"
        cluster_size = 1
        for j in range(n):
            if i == j:
                continue
            other = token_sets[j]
            if not other or not title_tokens:
                continue
            inter = len(title_tokens & other)
            union = len(title_tokens | other)
            if union == 0:
                continue
            jaccard = inter / union
            if jaccard >= 0.4:
                cluster_size += 1

        # (3) 소스 가중치
        source_name = (art.get("source") or "").strip()
        weight = 1.0
        for key, w in _SOURCE_WEIGHTS.items():
            if key.lower() in source_name.lower():
                weight = max(weight, w)

        score = weight * (cluster_size + overlap * 0.5)
        art["score"] = float(score)
        scores.append(score)

    # score 기준 내림차순 정렬, tie-breaker: 원래 순서 유지
    articles_sorted = sorted(
        articles,
        key=lambda x: x.get("score", 0.0),
        reverse=True,
    )
    return articles_sorted


# ----------------------------
# SerpAPI 호출 + 저장
# ----------------------------

def fetch_global_stock_news(ref_date: date) -> List[Dict]:
    # 여러 이름 중 하나라도 있으면 사용
    api_key = (
        os.getenv("SERPAPI_API_KEY")
        or os.getenv("SERP_API_KEY")
        or os.getenv("SERPAPI_KEY")
    )

    if not api_key:
        # 키가 없으면 그냥 해외 뉴스는 건너뛰고, 파이프라인은 계속 돌게 한다
        print("[WARN] SerpAPI API key not found (.env). 글로벌 뉴스 수집을 건너뜁니다.")
        return []

    # 해외 주식/시장 관련 키워드 (필요하면 나중에 튜닝)
    query = "stock market OR stocks OR equities OR S&P 500 OR Nasdaq OR Dow Jones"

    params = {
        "engine": "google_news",
        "q": query,
        "hl": "en",
        "gl": "us",
        "api_key": api_key,
        "sort_by": "date",
    }

    try:
        res = requests.get(SERP_ENDPOINT, params=params, timeout=20)
        res.raise_for_status()
        data = res.json()
    except requests.exceptions.HTTPError as e:
        print(f"[WARN] 글로벌 뉴스 SerpAPI HTTP 오류 발생: {e}")
        return []
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[WARN] 글로벌 뉴스 SerpAPI 네트워크/타임아웃 오류 발생: {e}")
        return []
    except Exception as e:
        print(f"[WARN] 글로벌 뉴스 SerpAPI 요청 중 알 수 없는 예외: {e}")
        return []


    raw_articles: List[Dict] = []
    for item in data.get("news_results", []):
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        link = item.get("link", "")
        source_name = ""
        src = item.get("source")
        if isinstance(src, dict):
            source_name = src.get("name", "")

        title_ko = translate_en_to_ko(title)
        summary_ko = translate_en_to_ko(snippet or title)

        raw_articles.append(
            {
                "title_en": title,
                "summary_en": snippet,
                "title_ko": title_ko,
                "summary_ko": summary_ko,
                "source": source_name,
                "link": link,
                "published_at": item.get("date", ""),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    # 🔹 관심도 기반으로 랭킹 재정렬
    ranked = _rank_articles_by_attention(raw_articles)
    return ranked


def save_global_news(ref_date: date) -> Path:
    articles = fetch_global_stock_news(ref_date)

    out_dir = Path("iceage") / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"global_news_{ref_date.isoformat()}.jsonl"

    with path.open("w", encoding="utf-8") as f:
        for art in articles:
            f.write(json.dumps(art, ensure_ascii=False) + "\n")

    print(f"✅ 해외 뉴스 저장 완료: {path}")
    return path


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    save_global_news(ref)

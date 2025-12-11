# iceage/src/collectors/kr_stock_event_serpapi.py
# -*- coding: utf-8 -*-
"""
[SerpAPI 최적화 버전 - Final Fix]
- 뉴스레터 생성 로직(StrategySelector)과 동일한 기준으로 타겟 종목을 선정합니다.
- 수집된 뉴스는 'kr_stock_event_news_{날짜}.jsonl'에 저장되어 뉴스레터와 키워드가 1:1 매칭됩니다.
"""

from __future__ import annotations

import json
import os
import sys
import pandas as pd
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# --- 경로 설정 ---
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# StrategySelector 임포트 (뉴스레터와 동일한 종목 선정을 위해)
try:
    from iceage.src.pipelines.final_strategy_selector import StrategySelector
except ImportError:
    print("[ERROR] StrategySelector를 찾을 수 없습니다. 경로를 확인하세요.")
    sys.exit(1)

load_dotenv(PROJECT_ROOT / ".env")

SERPAPI_ENDPOINT = "https://serpapi.com/search"

@dataclass
class TargetStock:
    code: str
    name: str

def get_target_stocks_from_strategy(ref_date: str) -> List[TargetStock]:
    """
    [핵심 수정] 단순 괴리율 상위가 아니라, 
    뉴스레터에 실제로 실릴 '전략 선정 종목'들을 가져옵니다.
    """
    try:
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        
        targets = []
        seen_codes = set()

        # 뉴스레터에 사용되는 모든 전략의 종목을 수집 대상에 포함
        # (Panic Buying, Fallen Angel, King's Shadow, Overheat Short)
        all_candidates = (
            results.get('panic_buying', []) + 
            results.get('fallen_angel', []) + 
            results.get('kings_shadow', []) + 
            results.get('overheat_short', [])
        )

        # 괴리율(tv_z) 순으로 정렬해서 상위 5개만 (뉴스레터와 동일 기준)
        # (뉴스레터는 보통 상위 5개만 보여주므로, 그들에 대한 뉴스만 있으면 됨)
        all_candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        top_candidates = all_candidates[:5]

        for row in top_candidates:
            code = str(row.get('code', '')).zfill(6)
            name = str(row.get('name', ''))
            
            if code and code not in seen_codes:
                targets.append(TargetStock(code=code, name=name))
                seen_codes.add(code)
        
        return targets

    except Exception as e:
        print(f"[ERROR] 전략 종목 선정 실패: {e}")
        return []

def fetch_stock_event_news_batch(ref_date: date, max_total: int = 30) -> List[Dict]:
    api_key = os.getenv("SERPAPI_API_KEY") or os.getenv("SERPAPI_KEY")
    if not api_key:
        print("[ERROR] SERPAPI_API_KEY 가 없습니다.")
        return []

    # [변경] 전략 기반으로 타겟 선정
    target_stocks = get_target_stocks_from_strategy(ref_date.isoformat())
    
    if not target_stocks:
        print(f"[INFO] {ref_date} 기준 타겟 종목이 없습니다.")
        return []

    names = [t.name for t in target_stocks]
    print(f"[INFO] 뉴스 수집 대상(전략 선발): {names}")

    # 쿼리 생성
    names_query = " OR ".join([f'"{n}"' for n in names])
    final_query = f"({names_query}) (특징주 OR 공시 OR 이슈)"
    
    print(f"[INFO] SerpAPI 검색: {final_query}")

    params = {
        "engine": "google_news",
        "q": final_query,
        "hl": "ko",
        "gl": "kr",
        "api_key": api_key,
        "num": max_total,
        "tbs": "qdr:d" 
    }

    all_articles = []
    
    try:
        resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        news_results = data.get("news_results", [])
        
        print(f"[INFO] 수집된 뉴스: {len(news_results)}건")

        for item in news_results:
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            full_text = (title + " " + snippet)
            
            matched_stock = None
            for t in target_stocks:
                if t.name in full_text:
                    matched_stock = t
                    break 
            
            if matched_stock:
                art = {
                    "title": title,
                    "link": item.get("link", ""),
                    "source": item.get("source", {}).get("name", ""),
                    "date": item.get("date", ""),
                    "fetched_at": datetime.utcnow().isoformat() + "Z",
                    "kind": "stock_event",
                    "code": matched_stock.code,
                    "name": matched_stock.name,
                    "stock_name": matched_stock.name # 뉴스레터 매핑용 키
                }
                all_articles.append(art)
                
    except Exception as e:
        print(f"[ERROR] SerpAPI 요청 실패: {e}")

    return all_articles

def append_stock_event_news(ref_date: date) -> None:
    articles = fetch_stock_event_news_batch(ref_date)
    
    # 뉴스가 없어도 파일은 생성해야(빈 리스트라도) 뉴스레터가 안 죽음
    raw_dir = PROJECT_ROOT / "iceage" / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # [중요] 파일명 일치: kr_stock_event_news_YYYY-MM-DD.jsonl
    raw_path = raw_dir / f"kr_stock_event_news_{ref_date.isoformat()}.jsonl"

    with raw_path.open("w", encoding="utf-8") as f:
        if articles:
            for art in articles:
                f.write(json.dumps(art, ensure_ascii=False) + "\n")
        else:
            print("[INFO] 관련 뉴스가 없어 빈 파일을 생성합니다.")

    print(f"✅ 이벤트 뉴스 저장 완료: {raw_path.name} ({len(articles)}건)")

def main() -> None:
    if len(sys.argv) >= 2:
        ref = datetime.fromisoformat(sys.argv[1]).date()
    else:
        ref = date.today()
    append_stock_event_news(ref)

if __name__ == "__main__":
    main()
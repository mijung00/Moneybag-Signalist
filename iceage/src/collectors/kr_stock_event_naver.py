# iceage/src/collectors/kr_stock_event_naver.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup  # pip install beautifulsoup4

from iceage.src.signals.signal_volume_pattern import detect_signals_from_prices

RAW_DIR = Path("iceage") / "data" / "raw"
REF_DIR = Path("iceage") / "data" / "reference"


def _raw_path(ref_date: date) -> Path:
    """
    국내 뉴스 raw 파일 (시장 뉴스 + 종목 이벤트 뉴스가 같이 쌓이는 파일)
    """
    return RAW_DIR / f"kr_news_{ref_date.isoformat()}.jsonl"


def _fetch_company_news_html(code: str, page: int = 1) -> str:
    """
    네이버 금융 개별 종목 뉴스 페이지 HTML 가져오기.
    예: https://finance.naver.com/item/news_news.naver?code=005930&page=1&sm=title_basic
    """
    url = "https://finance.naver.com/item/news_news.naver"
    params = {"code": code, "page": page, "sm": "title_basic"}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.text


def _parse_company_news(code: str, name: str, html: str, max_items: int = 3) -> List[Dict]:
    """
    네이버 개별 종목 뉴스 테이블(type5)을 파싱해서 상위 max_items개를 뽑는다.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.type5")
    if not table:
        # 테이블 자체를 못 찾는 경우
        debug_dir = Path("iceage") / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_path = debug_dir / f"naver_news_{code}_no_table.html"
        try:
            with open(debug_path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(html)
            print(f"[WARN] table.type5를 찾지 못했습니다. HTML을 {debug_path}로 저장했습니다.")
        except Exception as e:
            print(f"[WARN] 디버그 HTML 저장 실패: {e}")
        return []

    rows = table.select("tr")
    print(f"[DEBUG] code={code} table.type5 row 수: {len(rows)}")

    results: List[Dict] = []
    for idx, tr in enumerate(rows):
        tds = tr.select("td")
        if len(tds) < 3:
            continue

        # td[0] 안에 a가 없으면 tr 전체에서라도 찾아본다
        a = tds[0].select_one("a") or tr.select_one("a")
        if not a:
            continue

        title = a.get_text(strip=True)
        href = a.get("href", "")
        if not href:
            continue

        link = requests.compat.urljoin("https://finance.naver.com", href)
        source = tds[1].get_text(strip=True)
        date_str = tds[2].get_text(strip=True)  # 예: '2025.11.07 15:32'

        results.append(
            {
                "title": title,
                "link": link,
                "snippet": "",
                "source": source,
                "published_at": date_str,
                "kind": "stock_event",
                "code": code,
                "name": name,
            }
        )

        if len(results) >= max_items:
            break

    # 테이블은 있었는데 결과가 0개인 경우도 HTML을 떨어뜨려서 구조를 보자
    if not results:
        debug_dir = Path("iceage") / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_path = debug_dir / f"naver_news_{code}_empty_rows.html"
        try:
            with open(debug_path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(html)
            print(
                f"[WARN] table.type5는 있었지만 뉴스 row를 파싱하지 못했습니다. "
                f"HTML을 {debug_path}로 저장했습니다."
            )
        except Exception as e:
            print(f"[WARN] 디버그 HTML 저장 실패: {e}")

    return results





def _load_name_to_code_map(ref_date: date) -> dict[str, str]:
    """
    상장 목록 CSV에서 종목명 -> 종목코드 매핑을 만든다.
    kr_listing_{ref_date}.csv 를 사용.
    """
    path = REF_DIR / f"kr_listing_{ref_date.isoformat()}.csv"
    if not path.exists():
        print(f"[WARN] 상장 목록 파일이 없습니다: {path}")
        return {}

    df = pd.read_csv(path, dtype=str)

    name_col = None
    code_col = None
    for c in df.columns:
        if c in ("name", "종목명"):
            name_col = c
        if c in ("code", "종목코드"):
            code_col = c

    if not name_col or not code_col:
        print(f"[WARN] 상장 목록 컬럼을 찾지 못했습니다: {df.columns.tolist()}")
        return {}

    df[name_col] = df[name_col].astype(str).str.strip()
    df[code_col] = df[code_col].astype(str).str.strip()

    mapping: dict[str, str] = {}
    for _, row in df.iterrows():
        n = row[name_col]
        c = row[code_col]
        if not n or not c:
            continue
        if n not in mapping:
            mapping[n] = c

    print(f"[DEBUG] name->code 매핑 개수: {len(mapping)}")
    return mapping


def fetch_stock_event_news_from_naver(
    ref_date: date, max_per_stock: int = 3
) -> List[Dict]:
    """
    Signalist Today 후보 종목들을 기준으로,
    네이버 개별 종목 뉴스 페이지에서 이벤트성 뉴스를 가져온다.
    """
    # 1) ref_date 기준 Signalist Today 후보 종목 리스트
    signal_rows = detect_signals_from_prices(ref_date)
    print(f"[DEBUG] detect_signals_from_prices rows: {len(signal_rows)}")

    name_to_code = _load_name_to_code_map(ref_date)

    targets: List[Tuple[str, str]] = []  # (code, name)
    seen_codes: set[str] = set()

    for r in signal_rows:
        name = str(r.name).strip()

        # 1순위: SignalRow에 code가 이미 붙어 있으면 사용
        code = getattr(r, "code", "") or ""
        code = code.strip()

        # 2순위: 상장 목록에서 이름으로 코드 찾기
        if not code:
            code = name_to_code.get(name, "")

        if not code:
            continue

        if code in seen_codes:
            continue

        seen_codes.add(code)
        targets.append((code, name))

    print(f"[DEBUG] 네이버 뉴스 타겟 종목 수: {len(targets)}")

    if not targets:
        return []

    # 2) 각 종목별로 최신 뉴스 파싱
    all_articles: List[Dict] = []
    for code, name in targets:
        try:
            html = _fetch_company_news_html(code)
        except Exception as e:
            print(f"[WARN] 네이버 뉴스 요청 실패: code={code}, err={e}")
            continue

        articles = _parse_company_news(code, name, html, max_items=max_per_stock)
        print(f"[DEBUG] {code} {name} 뉴스 {len(articles)}개")
        all_articles.extend(articles)

    return all_articles


def append_stock_event_news(ref_date: date, max_per_stock: int = 3) -> Path:
    """
    kr_news_{ref_date}.jsonl 에 종목 이벤트 뉴스를 이어 붙인다.
    - 파일이 없으면 새로 만들고(w), 이미 있으면 append(a).
    """
    path = _raw_path(ref_date)
    articles = fetch_stock_event_news_from_naver(ref_date, max_per_stock=max_per_stock)
    if not articles:
        print("[INFO] 종목 이벤트 뉴스가 없습니다.")
        return path

    mode = "a" if path.exists() else "w"
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, mode, encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")

    print(f"✅ 네이버 종목 이벤트 뉴스 추가 저장 완료: {path} (+{len(articles)}건)")
    return path


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    append_stock_event_news(ref)

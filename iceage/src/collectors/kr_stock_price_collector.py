# iceage/src/collectors/kr_stock_price_collector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Optional, List, Dict

import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs


NAVER_MARKET_SUM_URL = "https://finance.naver.com/sise/sise_market_sum.naver"


def _extract_code_from_href(href: str) -> str:
    """
    href 에서 6자리 종목코드를 뽑는다.
    예: /item/main.naver?code=005930
    """
    if not href:
        return ""

    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        code = qs.get("code", [""])[0]
        if code:
            return code.zfill(6)
    except Exception:
        pass

    # fallback: 숫자 6자리만 강제로 뽑기
    digits = "".join(ch for ch in href if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return ""


def _parse_market_sum_page(html: str, market_label: str) -> pd.DataFrame:
    """
    네이버 시가총액 페이지에서
    종목명/현재가/전일비/등락률/거래량(+code, market)을 추출한다.
    """
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        return pd.DataFrame()

    # 실제 종목행들은 onmouseover="mouseOver(this)" 속성이 있음
    rows = tbody.find_all("tr", attrs={"onmouseover": "mouseOver(this)"})

    records: List[Dict[str, str]] = []
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 5:
            continue

        name_tag = tds[1].find("a")
        if not name_tag:
            continue

        name = name_tag.get_text(strip=True)
        href = name_tag.get("href", "")
        code = _extract_code_from_href(href)

        current = tds[2].get_text(strip=True)
        diff = tds[3].get_text(strip=True)
        change_rate = tds[4].get_text(strip=True)

        # 거래량은 대략 10번째 컬럼 (시가총액/상장주식수/외국인비율 뒤)
        volume = ""
        if len(tds) > 9:
            volume = tds[9].get_text(strip=True)

        # 거래대금은 이 페이지에는 직접 안 나와서,
        # raw 단계에서는 비워두고, 노말라이저에서 close*volume으로 채우게 둔다.
        records.append(
            {
                "종목명": name,
                "현재가": current,
                "전일비": diff,
                "등락률": change_rate,
                "거래량": volume,
                "거래대금": "",  # placeholder
                "market": market_label,
                "code": code,
            }
        )

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def _get_last_page(soup: BeautifulSoup) -> int:
    """
    하단 페이지 네비게이션에서 '맨뒤' 링크의 page 값을 읽어서 마지막 페이지 추정.
    실패하면 1페이지로 가정.
    """
    try:
        a = soup.select_one("td.pgRR a")
        if not a:
            return 1
        href = a.get("href", "")
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        page = qs.get("page", ["1"])[0]
        return int(page)
    except Exception:
        return 1


def _fetch_market_all(market_label: str, sosok: str) -> pd.DataFrame:
    """
    KOSPI/KOSDAQ 전체 시가총액 리스트를 여러 페이지 돌면서 수집.
    - market_label: "KOSPI" / "KOSDAQ"
    - sosok: "0" (코스피), "1" (코스닥)
    """
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1) 1페이지 먼저 요청 + 마지막 페이지 수 추출
    params = {"sosok": sosok, "page": 1}
    res = requests.get(NAVER_MARKET_SUM_URL, params=params, headers=headers, timeout=10)
    res.raise_for_status()
    html = res.text

    soup = BeautifulSoup(html, "html.parser")
    first_df = _parse_market_sum_page(html, market_label)
    last_page = _get_last_page(soup)

    frames: List[pd.DataFrame] = []
    if not first_df.empty:
        frames.append(first_df)

    # 2) 2페이지부터 마지막 페이지까지 순회
    for page in range(2, last_page + 1):
        params["page"] = page
        res = requests.get(NAVER_MARKET_SUM_URL, params=params, headers=headers, timeout=10)
        res.raise_for_status()
        df_page = _parse_market_sum_page(res.text, market_label)
        if df_page.empty:
            # 비어 있으면 조기 종료
            break
        frames.append(df_page)

    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)

    # 종목명 / code 기준으로 중복 제거
    if "code" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["code"])
    else:
        df_all = df_all.drop_duplicates(subset=["종목명"])

    return df_all


def collect_price(ref_date: Optional[datetime.date] = None) -> Path:
    """
    네이버 금융 '시가총액' 페이지에서
    KOSPI/KOSDAQ 전 종목의 시세를 수집해서
    iceage/data/raw/kr_prices_YYYY-MM-DD.csv 로 저장.

    - ref_date: 파일 이름에만 사용 (내용은 네이버의 현재 시세)
    """
    if ref_date is None:
        ref_date = datetime.date.today()

    # KOSPI (sosok=0), KOSDAQ (sosok=1)
    df_kospi = _fetch_market_all("KOSPI", "0")
    df_kosdaq = _fetch_market_all("KOSDAQ", "1")

    df_all = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    if df_all.empty:
        raise RuntimeError("네이버 시가총액 페이지에서 데이터를 가져오지 못했습니다.")

    raw_dir = Path("iceage") / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    out_path = raw_dir / f"kr_prices_{ref_date.isoformat()}.csv"
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ 주가 데이터 저장 완료: {out_path}")
    return out_path


if __name__ == "__main__":
    # 사용 예:
    #   python -m iceage.src.collectors.kr_stock_price_collector
    #   python -m iceage.src.collectors.kr_stock_price_collector 2025-11-03
    import sys

    if len(sys.argv) >= 2:
        ref = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        ref = datetime.date.today()

    collect_price(ref)

# iceage/src/collectors/kr_listing_collector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd
import requests

# KRX KIND - 상장법인목록 (검색 타입 13: 전체)
KRX_LISTING_URL = (
    "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
)


def fetch_kr_listing() -> pd.DataFrame:
    """
    KRX KIND에서 상장법인 목록 엑셀을 다운로드해
    code / name / industry / market 컬럼으로 정리해서 반환.
    """
def fetch_kr_listing() -> pd.DataFrame:
    """
    KRX KIND에서 상장법인 목록을 다운로드해
    code / name / industry / market 컬럼으로 정리해서 반환.
    실제로는 HTML 테이블을 엑셀처럼 내려주기 때문에 read_html을 사용.
    """
    res = requests.get(KRX_LISTING_URL, timeout=20)
    res.raise_for_status()

    # KRX KIND는 보통 euc-kr 인코딩
    res.encoding = "euc-kr"

    # HTML 안의 첫 번째 테이블 읽기 (StringIO로 감싸서 FutureWarning 제거)
    html_buf = io.StringIO(res.text)
    tables = pd.read_html(html_buf, header=0)
    if not tables:
        raise RuntimeError("상장법인 목록 테이블을 찾지 못했습니다.")

    df = tables[0]


    # KRX 엑셀/테이블 컬럼 이름은 조금씩 달 수 있으니, "포함 관계"로 매핑
    col_map = {}
    for col in df.columns:
        c = str(col)
        if "회사" in c:
            col_map[col] = "name"
        elif "종목코드" in c:
            col_map[col] = "code"
        elif "업종" in c:
            col_map[col] = "industry"
        elif "시장" in c:
            col_map[col] = "market"

    df = df.rename(columns=col_map)

    # 필수 컬럼 정리
    required = ["code", "name", "industry"]
    for r in required:
        if r not in df.columns:
            raise RuntimeError(f"상장법인 목록에서 '{r}' 컬럼을 찾지 못했습니다.")

    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()

    if "market" in df.columns:
        df["market"] = df["market"].astype(str).str.strip()
        cols: List[str] = ["code", "name", "industry", "market"]
    else:
        cols = ["code", "name", "industry"]

    df = df[cols]
    return df


    # KRX 엑셀 컬럼 이름은 조금씩 달 수 있으니, "포함 관계"로 매핑
    col_map = {}
    for col in df.columns:
        c = str(col)
        if "회사" in c:
            col_map[col] = "name"
        elif "종목코드" in c:
            col_map[col] = "code"
        elif "업종" in c:
            col_map[col] = "industry"
        elif "시장" in c:
            col_map[col] = "market"

    df = df.rename(columns=col_map)

    # 필수 컬럼 정리
    required = ["code", "name", "industry"]
    for r in required:
        if r not in df.columns:
            raise RuntimeError(f"상장법인 목록에서 '{r}' 컬럼을 찾지 못했습니다.")

    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()

    if "market" in df.columns:
        df["market"] = df["market"].astype(str).str.strip()
        cols: List[str] = ["code", "name", "industry", "market"]
    else:
        cols = ["code", "name", "industry"]

    df = df[cols]
    return df


def save_kr_listing(ref_date: date) -> Path:
    """
    ref_date 기준 상장법인 목록을 csv로 저장.
    (실제로는 '오늘 시점' 목록이지만, 파일 이름만 날짜로 구분)
    """
    df = fetch_kr_listing()

    out_dir = Path("iceage") / "data" / "reference"
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"kr_listing_{ref_date.isoformat()}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")

    print(f"✅ 상장법인 목록 저장 완료: {path}")
    return path


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    save_kr_listing(ref)

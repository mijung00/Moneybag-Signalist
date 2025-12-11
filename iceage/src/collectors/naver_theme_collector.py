# iceage/src/collectors/naver_theme_collector.py
# -*- coding: utf-8 -*-
"""
[Signalist Upgrade]
네이버 테마 랭킹(순위)에 의존하지 않고, '전체 테마 리스트'를 모두 수집합니다.
새벽 시간대 네이버 순위 초기화(0%) 이슈를 방어하기 위함입니다.
수집된 전체 테마 매핑 정보를 바탕으로 Aggregator가 직접 수익률 순위를 계산합니다.
"""
from __future__ import annotations

import re
import time
import sys
from datetime import date
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from requests import RequestException
from bs4 import BeautifulSoup

# 프로젝트 루트 설정
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.append(str(PROJECT_ROOT))
except Exception:
    pass

THEME_LIST_URL = "https://finance.naver.com/sise/theme.naver"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def _safe_get(url: str, timeout: float = 10.0):
    try:
        res = SESSION.get(url, timeout=timeout)
        res.raise_for_status()
        return res
    except RequestException as e:
        print(f"[WARN] 요청 실패: {url} -> {e}")
        return None

def _fetch_theme_list_all_pages() -> List[Dict]:
    """
    네이버 테마 목록의 '모든 페이지'를 순회하여 전체 테마 목록을 수집합니다.
    (기존: 1페이지만 수집 -> 변경: 끝까지 수집)
    """
    all_themes = []
    seen_ids = set()
    page = 1
    
    print("🔄 [전수 조사] 네이버 전체 테마 리스트 수집 시작...")

    while True:
        url = f"{THEME_LIST_URL}?page={page}"
        res = _safe_get(url, timeout=10)
        if res is None:
            break
        res.encoding = "euc-kr"

        soup = BeautifulSoup(res.text, "lxml")
        table = soup.select_one("table.type_1.theme")
        
        if not table:
            print(f"[INFO] {page}페이지에서 테이블 없음. 수집 종료.")
            break

        # 페이지 내 테마 추출
        found_on_page = 0
        for a in table.select("td.col_type1 a[href*='sise_group_detail.naver']"):
            href = a.get("href", "")
            # 예: /sise/sise_group_detail.naver?type=theme&no=575
            m = re.search(r"no=(\d+)", href)
            if not m:
                continue
            
            theme_id = m.group(1)
            name = a.get_text(strip=True)
            if not name:
                continue
                
            if theme_id in seen_ids:
                continue

            seen_ids.add(theme_id)
            all_themes.append({"theme_id": theme_id, "theme_name": name})
            found_on_page += 1

        # 다음 페이지 판단 로직
        # 네이버 테마 페이지는 보통 7~8페이지 정도입니다.
        # 맨 뒤 페이지 버튼이 현재 페이지보다 작거나 같으면 종료
        pg_last = soup.select_one("td.pgRR a")
        
        if found_on_page == 0:
            break
            
        print(f"  - Page {page}: {found_on_page}개 테마 발견")
        
        # 마지막 페이지 체크 (pgRR 태그가 없으면 마지막 페이지임)
        if not pg_last:
            break
            
        # 안전장치: 20페이지 넘어가면 강제 종료 (무한루프 방지)
        if page >= 20:
            break
            
        page += 1
        time.sleep(0.2) # 페이지 넘김 간 매너 딜레이

    print(f"📌 전체 테마 목록 수집 완료: 총 {len(all_themes)}개 테마")
    return all_themes

def _fetch_stocks_for_theme(theme_id: str, theme_name: str) -> List[Dict]:
    """
    개별 테마 페이지에서 종목 리스트 추출
    """
    url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_id}"
    res = _safe_get(url, timeout=10)
    if res is None:
        return []
    res.encoding = "euc-kr"

    soup = BeautifulSoup(res.text, "lxml")
    records: List[Dict] = []

    for a in soup.select("a[href*='/item/main.naver?code=']"):
        name = a.get_text(strip=True)
        if not name: continue
        
        href = a.get("href", "")
        m = re.search(r"code=(\d+)", href)
        if not m: continue
        
        code = m.group(1).zfill(6)
        records.append({
            "code": code,
            "name": name,
            "naver_label": theme_name,
        })

    if records:
        # 종목 중복 제거 (혹시나 해서)
        seen_code = set()
        unique_records = []
        for r in records:
            if r['code'] not in seen_code:
                seen_code.add(r['code'])
                unique_records.append(r)
        return unique_records
        
    return []

def save_naver_themes(ref_date: date) -> Path:
    """
    메인 실행 함수: 전체 테마 수집 -> 전체 종목 매핑 -> 저장
    """
    # 1. 전체 테마 리스트 확보 (랭킹 무관)
    themes = _fetch_theme_list_all_pages()
    
    if not themes:
        raise RuntimeError("네이버 테마 목록을 수집하지 못했습니다.")

    all_records: List[Dict] = []
    
    print(f"🚀 개별 테마 상세 수집 시작 (대상: {len(themes)}개)...")
    
    # 2. 각 테마별 구성 종목 수집
    for idx, t in enumerate(themes, 1):
        tid = t["theme_id"]
        tname = t["theme_name"]
        
        try:
            stocks = _fetch_stocks_for_theme(tid, tname)
            all_records.extend(stocks)
            
            # 진행 상황 로깅 (너무 많으니 10개 단위로)
            if idx % 10 == 0:
                print(f"  [{idx}/{len(themes)}] '{tname}' 등 수집 중...")
                
            time.sleep(0.05) # 서버 부하 방지용 미세 딜레이
            
        except Exception as e:
            print(f"[WARN] 테마 {tname}({tid}) 수집 오류: {e}")

    if not all_records:
        raise RuntimeError("네이버 테마 종목을 하나도 수집하지 못했습니다.")

    # 3. 데이터 저장
    df = pd.DataFrame(all_records).drop_duplicates()
    
    out_dir = Path("iceage") / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"naver_themes_{ref_date.isoformat()}.csv"
    
    df.to_csv(path, index=False, encoding="utf-8-sig")

    print("\n" + "="*50)
    print(f"✅ 네이버 테마 전체 전수 조사 완료")
    print(f"📂 저장 경로: {path}")
    print(f"📊 총 수집된 매핑: {len(df)} rows (테마-종목 쌍)")
    print("="*50 + "\n")
    
    return path

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()
    save_naver_themes(ref)
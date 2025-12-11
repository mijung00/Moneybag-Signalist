# iceage/src/collectors/kr_ipo_collector.py
# -*- coding: utf-8 -*-
import sys
import json
import re
import requests
import pandas as pd
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# 38커뮤니케이션 URL
URL_SUB = "http://www.38.co.kr/html/fund/index.htm?o=k"  # 공모주 청약일정
URL_LISTING = "http://www.38.co.kr/html/fund/index.htm?o=nw" # 신규 상장

def _clean_text(x):
    if pd.isna(x): return ""
    return str(x).strip()

def _is_spac(name: str) -> bool:
    target = name.replace(" ", "")
    if "스팩" in target or "기업인수목적" in target:
        return True
    return False

def _parse_rate_to_float(raw_val) -> float:
    if pd.isna(raw_val): return 0.0
    # "1,200.5:1" -> "1200.5"
    s = str(raw_val).replace(",", "").split(":")[0]
    s = re.sub(r"[^\d\.]", "", s)
    try: return float(s)
    except: return 0.0

def collect_ipo_data(ref_date: date):
    print(f"🚀 [IPO] 38커뮤니케이션 데이터 수집 시작 (기준일: {ref_date})")
    results = {"subscription": [], "listing": []}
    
    # --- 1. 청약 일정 수집 ---
    try:
        resp = requests.get(URL_SUB, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.encoding = "euc-kr"
        dfs = pd.read_html(resp.text)
        
        target_df = None
        # [FIX] 데이터가 가장 많은(행 수가 많은) 테이블을 메인 테이블로 간주
        # 38커뮤니케이션은 상단에 작은 요약 테이블들이 있어서 엉뚱한 걸 잡을 수 있음
        candidate_dfs = []
        for df in dfs:
            # 컬럼 이름에 공백 제거 후 확인
            cols = [str(c).replace(" ", "") for c in df.columns]
            if "종목명" in cols and "공모주일정" in cols:
                df.columns = cols # 컬럼명 공백 제거 적용
                candidate_dfs.append(df)
        
        if candidate_dfs:
            # 행 수가 가장 많은 것이 진짜 리스트일 확률 높음
            target_df = max(candidate_dfs, key=len)
        
        if target_df is not None:
            target_df = target_df.dropna(subset=["종목명"])
            
            # 경쟁률 컬럼 찾기 (기관경쟁률, 청약경쟁률 등 포함)
            comp_col = next((c for c in target_df.columns if "경쟁률" in c), None)
            
            for _, row in target_df.iterrows():
                name = _clean_text(row.get("종목명"))
                if _is_spac(name): continue
                    
                schedule = _clean_text(row.get("공모주일정"))
                price = row.get("확정공모가", "")
                band = row.get("희망공모가", "")
                underwriter = _clean_text(row.get("주간사", ""))
                
                competition_str = ""
                competition_rate = 0.0
                
                if comp_col:
                    competition_str = row.get(comp_col, "")
                    competition_rate = _parse_rate_to_float(competition_str)
                
                is_exceed_band = False
                try:
                    confirmed = int(str(price).replace(",", ""))
                    upper_band = int(str(band).split("~")[-1].replace(",", "").strip())
                    if confirmed > upper_band: is_exceed_band = True
                except: pass

                results["subscription"].append({
                    "name": name,
                    "schedule": schedule,
                    "price": str(price),
                    "band": str(band),
                    "underwriter": underwriter,
                    "competition_str": str(competition_str),
                    "competition_rate": competition_rate,
                    "is_exceed_band": is_exceed_band
                })
            print(f"  - 청약 일정 {len(results['subscription'])}건 수집 완료 (스팩 제외)")
            
    except Exception as e:
        print(f"[WARN] 청약 일정 수집 실패: {e}")

    # --- 2. 신규 상장 일정 수집 ---
    try:
        resp = requests.get(URL_LISTING, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.encoding = "euc-kr"
        dfs = pd.read_html(resp.text)
        
        target_df = None
        candidate_dfs = []
        for df in dfs:
            cols = [str(c).replace(" ", "") for c in df.columns]
            if "기업명" in cols and "상장일" in cols:
                df.columns = cols
                candidate_dfs.append(df)
        
        if candidate_dfs:
            target_df = max(candidate_dfs, key=len)
                
        if target_df is not None:
            target_df = target_df.dropna(subset=["기업명"])
            for _, row in target_df.iterrows():
                name = _clean_text(row.get("기업명"))
                if _is_spac(name): continue
                    
                date_str = _clean_text(row.get("상장일")) 
                price_offer = row.get("공모가(원)", "")
                
                results["listing"].append({
                    "name": name,
                    "date": date_str,
                    "price_offer": str(price_offer).replace(",", "").strip(),
                })
            print(f"  - 신규 상장 일정 {len(results['listing'])}건 수집 완료 (스팩 제외)")
            
    except Exception as e:
        print(f"[WARN] 신규 상장 일정 수집 실패: {e}")

    out_dir = PROJECT_ROOT / "iceage" / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"kr_ipo_info_{ref_date.isoformat()}.json"
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
        
    print(f"✅ IPO 데이터 저장 완료: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()
    collect_ipo_data(ref)
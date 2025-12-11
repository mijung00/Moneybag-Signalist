# iceage/src/collectors/kr_investor_flow_collector.py
import time
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# 프로젝트 루트 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

##from iceage.src.utils.trading_days import get_trading_days_range

DATA_DIR = PROJECT_ROOT / "iceage" / "data"
RAW_DIR = DATA_DIR / "raw"

# [수정] 올바른 URL로 변경
# 투자자별 매매동향(일별) : investor_deal_trend_day.naver
BASE_URL = "https://finance.naver.com/sise/investor_deal_trend_day.naver"

def _fetch_investor_flow(page=1):
    """
    네이버 금융 '투자자별 매매동향' 일별 데이터 수집
    """
    params = {
        "bizdate": datetime.now().strftime("%Y%m%d"), # 오늘 기준 조회
        "sosok": "", # 코스피/코스닥 전체 등 (필요시 조정)
        "page": page
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # URL 호출
    resp = requests.get(BASE_URL, params=params, headers=headers)
    resp.raise_for_status()
    
    # HTML 파싱 (euc-kr 인코딩 주의)
    try:
        dfs = pd.read_html(resp.text, header=0, encoding='euc-kr')
    except:
        # 가끔 encoding 문제 생기면 바로 text로 시도
        dfs = pd.read_html(resp.content, header=0)
        
    if not dfs:
        return pd.DataFrame()

    df = dfs[0]
    
    # 날짜 컬럼(날짜)이 있는 행만 유효함 (NaN 제거)
    df = df.dropna(subset=['날짜'])
    
    # 컬럼 정리 (날짜, 개인, 외국인, 기관계 ...)
    # 네이버 테이블 컬럼명이 상황에 따라 다를 수 있어 위치 기반 혹은 이름 기반 매핑 필요
    # 보통: [날짜, 개인, 외국인, 기관계, 금융투자, 보험, 투신, 기타금융, 은행, 연기금등, 사모펀드, 국가지자체, 기타법인]
    
    return df

def save_investor_flow(ref_date: str):
    print(f"💰 [Investor Flow] {ref_date} 투자자별 매매동향 수집 시작")
    
    all_rows = []
    # 최근 5페이지 정도만 긁어서 ref_date 찾기
    for p in range(1, 6):
        try:
            df = _fetch_investor_flow(p)
            if df.empty:
                break
            
            # 날짜 포맷 통일 (YY.MM.DD -> YYYY-MM-DD)
            # 네이버는 '25.12.08' 형태로 줌
            df['date_str'] = df['날짜'].apply(lambda x: "20" + x.replace(".", "-") if isinstance(x, str) and len(x) == 8 else x)
            
            all_rows.append(df)
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            print(f"   [WARN] page {p} 수집 실패: {e}")
            
    if not all_rows:
        print("❌ 수집된 데이터가 없습니다.")
        return

    full_df = pd.concat(all_rows).drop_duplicates('date_str').reset_index(drop=True)
    
    # 해당 날짜(ref_date) 데이터만 필터링해서 저장해도 되고, 전체를 저장해도 됨
    # 여기선 ref_date에 해당하는 날이 있는지 확인
    target_row = full_df[full_df['date_str'] == ref_date]
    
    file_path = RAW_DIR / f"kr_investor_flow_{ref_date}.csv"
    if not target_row.empty:
        target_row.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"✅ {ref_date} 데이터 저장 완료: {file_path}")
    else:
        print(f"⚠️ {ref_date} 데이터를 찾을 수 없습니다. (장 마감 전이거나 휴장일 수 있음)")
        # 빈 파일이라도 생성하여 파이프라인 중단 방지
        pd.DataFrame(columns=['date_str', '개인', '외국인', '기관계']).to_csv(file_path, index=False)

def main():
    if len(sys.argv) > 1:
        ref_date = sys.argv[1]
    else:
        ref_date = datetime.now().strftime("%Y-%m-%d")
    
    save_investor_flow(ref_date)

if __name__ == "__main__":
    main()
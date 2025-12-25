# iceage/src/collectors/krx_index_collector.py
import os
import sys
import requests
import pandas as pd
import time
import urllib3
from datetime import datetime, timedelta
from pathlib import Path

# [수정] 모든 환경 설정은 common.config가 책임집니다.
import common.config

# [젬공의 책략] 경로 설정 & SSL 경고 끄기
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KRX_API_KEY = os.getenv("KRX_AUTH_KEY")

# ---------------------------------------------------------
# 1. KRX API 수집기 (Primary)
# ---------------------------------------------------------
def fetch_krx_index(market: str, ref_date: str) -> dict:
    """
    market: 'KOSPI' or 'KOSDAQ'
    ref_date: 'YYYYMMDD'
    """
    # Spec 문서 기준 URL (data-dbg)
    if market == "KOSPI":
        url = "https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd"
    else:
        url = "https://data-dbg.krx.co.kr/svc/apis/idx/kosdaq_dd_trd"
        
    headers = {"AUTH_KEY": KRX_API_KEY}
    params = {"basDd": ref_date}
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5, verify=False)
        if resp.status_code == 200:
            return resp.json()
        return {}
    except Exception as e:
        # print(f"[WARN] KRX {market} API Fail: {e}")
        return {}

# ---------------------------------------------------------
# 2. Naver 금융 크롤러 (Fallback)
# ---------------------------------------------------------
def fetch_naver_index_fallback(market: str, target_date_str: str) -> dict:
    """
    KRX API 실패 시 네이버 금융 일별 시세 페이지를 크롤링하여 데이터 확보
    target_date_str: 'YYYY-MM-DD'
    """
    code = "KOSPI" if market == "KOSPI" else "KOSDAQ"
    url = f"https://finance.naver.com/sise/sise_index_day.naver?code={code}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # 1페이지(최신 6일치)만 가져옴
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        
        # pandas로 HTML 테이블 파싱
        dfs = pd.read_html(resp.text)
        if not dfs: return {}
        
        df = dfs[0].dropna() # 첫 번째 테이블이 시세 데이터
        
        # 날짜 포맷 통일 (YYYY.MM.DD -> YYYY-MM-DD)
        target_dot = target_date_str.replace("-", ".")
        
        # 해당 날짜 행 찾기
        row = df[df['날짜'] == target_dot]
        
        if row.empty:
            # print(f"[WARN] 네이버에도 {target_date_str} 데이터가 없습니다.")
            return {}
            
        # 데이터 추출
        # 체결가(종가), 등락률(전일비 비율)
        close = str(row.iloc[0]['체결가']).replace(",", "")
        
        # 등락률 계산 (네이버는 '등락률' 컬럼이 바로 긁히기도 하지만, 전일비로 계산하는 게 안전)
        # 하지만 표에 %가 있다면 그걸 씀. 보통 네이버 표에는 '체결가', '전일비', '등락률'이 있음.
        # read_html 결과 컬럼명 확인 필요. 보통 ['날짜', '체결가', '전일비', '등락률', '거래량', '거래대금']
        
        fluc_rate = "0.0"
        if '등락률' in row.columns:
            fluc_rate = str(row.iloc[0]['등락률']).replace("%", "").strip()
        
        # 네이버 '전일비'는 화살표 이미지가 텍스트로 섞일 수 있어 주의. 등락률 쓰는 게 나음.
        
        print(f"   👉 [Fallback] 네이버 금융에서 {market} 데이터 확보 성공!")
        return {
            "close": close,
            "fluc_rate": fluc_rate
        }

    except Exception as e:
        print(f"   ❌ [Fallback] 네이버 크롤링 실패: {e}")
        return {}

# ---------------------------------------------------------
# 3. 메인 실행기
# ---------------------------------------------------------
def run_collector(target_date_str: str):
    # YYYY-MM-DD -> YYYYMMDD
    ref_date_clean = target_date_str.replace("-", "")
    print(f"📊 [{target_date_str}] 지수 수집 시도...", end=" ")
    
    records = []
    
    markets = ["KOSPI", "KOSDAQ"]
    
    for m in markets:
        # 1차 시도: KRX API
        val_close = None
        val_fluc = None
        
        data_krx = fetch_krx_index(m, ref_date_clean)
        
        # KRX 응답 파싱
        if data_krx:
            for item in data_krx.get("OutBlock_1", []):
                # '코스피' 또는 '코스닥' (정확한 지수명 매칭)
                idx_nm = item.get("IDX_NM", "")
                if (m == "KOSPI" and idx_nm == "코스피") or (m == "KOSDAQ" and idx_nm == "코스DAQ" or idx_nm == "코스닥"):
                    val_close = str(item.get("CLSPRC_IDX", "0")).replace(",", "")
                    val_fluc = str(item.get("FLUC_RT", "0")).replace(",", "")
                    break
        
        # 2차 시도: 실패 시 네이버 폴백
        if val_close is None:
            print(f"(KRX불통->네이버{m})", end=" ")
            data_naver = fetch_naver_index_fallback(m, target_date_str)
            if data_naver:
                val_close = data_naver.get("close")
                val_fluc = data_naver.get("fluc_rate")
        
        # 결과 저장
        if val_close is not None:
            records.append({
                "date": target_date_str,
                "market": m,
                "close": val_close,
                "fluc_rate": val_fluc
            })

    if not records:
        print("실패 (휴장일 또는 데이터 없음)")
        return

    # CSV 저장 (누적)
    out_dir = PROJECT_ROOT / "iceage" / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "kr_market_index.csv"
    
    df_new = pd.DataFrame(records)
    
    if out_path.exists():
        df_old = pd.read_csv(out_path)
        # 날짜 포맷 통일 등 전처리 후 병합
        df = pd.concat([df_old, df_new], ignore_index=True)
        # 중복 제거 (같은 날짜, 같은 시장이면 최신 걸로 덮어쓰기)
        df.drop_duplicates(subset=['date', 'market'], keep='last', inplace=True)
        df.sort_values('date', inplace=True)
    else:
        df = df_new
        
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"성공 ✅ (저장: {len(df)} rows)")

def backfill_index(days: int = 365 * 3):
    print(f"🚀 지수 데이터 {days}일 백필 시작...")
    end = datetime.now().date()
    start = end - timedelta(days=days)
    
    curr = start
    while curr <= end:
        # 주말 제외
        if curr.weekday() < 5: 
            run_collector(curr.isoformat())
            # API 부하 방지 (네이버 크롤링 시 너무 빠르면 차단될 수 있음)
            time.sleep(0.5) 
        curr += timedelta(days=1)

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "backfill":
        backfill_index()
    else:
        # 인자가 없으면 오늘 날짜
        target = sys.argv[1] if len(sys.argv) >= 2 else datetime.now().strftime("%Y-%m-%d")
        run_collector(target)
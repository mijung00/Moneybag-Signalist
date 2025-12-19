# iceage/src/pipelines/kis_domestic_historical_test.py
import os
import requests
import json
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env 파일 로드 성공")
except ImportError:
    print("⚠️ python-dotenv가 설치되지 않았습니다. .env 파일이 적용되지 않을 수 있습니다.")
    print("   - 설치 명령어: pip install python-dotenv")


# --- 환경 변수에서 정보 읽기 ---
APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
ACCOUNT_NUM = os.getenv("KIS_ACCOUNT_NUM")
ACCOUNT_PROD_CODE = os.getenv("KIS_ACCOUNT_PROD_CODE")
# -----------------------------------

BASE_URL = "https://openapi.koreainvestment.com:9443"

def run_historical_test():
    # 0. 필수 정보 확인
    if not all([APP_KEY, APP_SECRET, ACCOUNT_NUM, ACCOUNT_PROD_CODE]):
        print("\n❌ [실패] .env 파일에 아래 4가지 정보가 모두 설정되었는지 확인해주세요:")
        print("- KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NUM, KIS_ACCOUNT_PROD_CODE")
        return

    print("\n✅ [준비 완료] API 키와 계좌 정보 로딩 성공")
    print(f"   - 계좌번호: {ACCOUNT_NUM[:4]}****")

    # 1. 토큰 발급
    print("\n--- 1. 토큰 발급 시도 ---")
    auth_url = f"{BASE_URL}/oauth2/tokenP"
    auth_data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    try:
        auth_res = requests.post(auth_url, json=auth_data, timeout=10)
        if auth_res.status_code != 200:
            print(f"❌ [실패] 토큰 발급 실패 (Status: {auth_res.status_code})")
            print(f"   - 응답: {auth_res.text}")
            return
        
        token = auth_res.json().get('access_token')
        print(f"✅ [성공] 토큰 발급 성공: {token[:10]}...")
    except Exception as e:
        print(f"❌ [실패] 토큰 발급 중 예외 발생: {e}")
        return


    # 2. 코스피 '어제' 일자 지수 조회 (계좌 정보 포함)
    print("\n--- 2. 코스피(KOSPI) '일자별' 지수 조회 시도 ---")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"   - 조회 기준일: {yesterday}")
    
    price_url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-index"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKUP03530100", # 국내업종기간별시세
        "custtype": "P",
        "cano": ACCOUNT_NUM,
        "acnt_prdt_cd": ACCOUNT_PROD_CODE
    }
    # 코스피 업종 코드: 0001
    params = {
        "fid_cond_mrkt_div_code": "U", 
        "fid_input_iscd": "0001",
        "fid_input_date_1": yesterday,
        "fid_input_date_2": yesterday,
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "0"  # [수정] 수정주가 반영 여부 파라미터 추가
    }
    
    try:
        res = requests.get(price_url, headers=headers, params=params, timeout=10)
        
        print(f"\n--- 3. API 응답 결과 ---")
        print(f"   - HTTP Status: {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            if data.get("rt_cd") == "0":
                # 이 API는 output2를 사용합니다.
                output = data.get('output2')
                if output and isinstance(output, list):
                    price = output[0].get('bstp_nmix_prpr')
                    print(f"✅ [성공] 코스피 {yesterday} 종가: {price}")
                    print("\n🎉🎉🎉 축하합니다! 프로젝트에 필요한 핵심 API 호출에 성공했습니다.")
                else:
                    print(f"❌ [실패] API는 성공했으나, 응답에 'output2' 데이터가 없습니다.")
                    print(f"   - 전체 응답: {data}")
            else:
                print(f"❌ [실패] API 오류 메시지: {data.get('msg1')}")
                print(f"   - 전체 응답: {data}")
        else:
            print(f"❌ [실패] 전체 응답 Body: {res.text}")
    except Exception as e:
        print(f"❌ [실패] API 호출 중 예외 발생: {e}")

if __name__ == "__main__":
    run_historical_test()
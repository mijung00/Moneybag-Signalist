# iceage/src/pipelines/morning_newsletter.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import sys
import re  # [필수] 정규식 모듈 유지
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
import pandas as pd
from pathlib import Path
import csv
from typing import List, Optional
import requests
from datetime import date as _date, timedelta, datetime
from textwrap import dedent
import tempfile
import boto3
from botocore.exceptions import ClientError

# 프로젝트 루트 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------
# ✅ SecretsManager를 JSON 형태로 저장했을 때도 동작하게(OPENAI_API_KEY 등)
# ---------------------------------------------------------------------
def _normalize_json_env(env_key: str) -> None:
    raw = os.getenv(env_key, "")
    if not raw:
        return
    s = raw.strip()

    # JSON 형태 아니면 그대로 둠
    if not (s.startswith("{") and s.endswith("}")):
        return

    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return

        # 1) env_key와 같은 키가 있으면 그 값을 사용
        v = obj.get(env_key)

        # 2) 없으면 value라는 관용 키를 사용
        if not v:
            v = obj.get("value")

        # 3) 그것도 없으면 dict 안의 "첫번째 문자열 값"을 사용
        if not v:
            for vv in obj.values():
                if isinstance(vv, str) and vv.strip():
                    v = vv.strip()
                    break

        if isinstance(v, str) and v.strip():
            os.environ[env_key] = v.strip()
    except Exception:
        pass
_normalize_json_env("OPENAI_API_KEY")

try:
    from iceage.src.llm.openai_driver import generate_newsletter_bundle
except Exception as e:
    logging.warning(f"[LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다: {e}")
    # AI 기능이 실패해도 프로그램이 멈추지 않도록 None으로 설정
    generate_newsletter_bundle = None

from iceage.src.analyzers.signalist_history_analyzer import build_signalist_history_markdown

try:
    from iceage.src.pipelines.final_strategy_selector import StrategySelector
except ImportError:
    pass

from iceage.src.data_sources.signalist_today import SignalRow
from iceage.src.signals.signal_volume_pattern import detect_signals_from_volume_anomaly_v2
from iceage.src.data_sources.market_themes import get_market_themes, MarketThemeSummary
from iceage.src.data_sources.sector_themes import get_sector_themes, SectorThemeSummary
from iceage.src.data_sources.investor_flow import load_investor_flow
from iceage.src.data_sources.kr_prices import load_normalized_prices
from iceage.src.data_sources.market_snapshot import get_market_overview
from iceage.src.utils.trading_days import (
    TradingCalendar,
    CalendarConfig,
    compute_reference_date,
    may_run_today,
)
from common.s3_manager import S3Manager


# LLM 캐시
_LLM_BUNDLE_CACHE: dict[str, dict] = {}

# ---------------------------------------------------------------------
# ✅ 한국투자증권(KIS) API 클라이언트 (뉴스레터용)
# ---------------------------------------------------------------------
class KisClient:
    def __init__(self):
        self.app_key = os.getenv("KIS_APP_KEY")
        self.app_secret = os.getenv("KIS_APP_SECRET")
        self.base_url = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
        # S3 설정
        self.bucket_name = "fincore-output-storage"
        self.s3_key = "config/kis_token.json"
        self.s3 = boto3.client("s3", region_name="ap-northeast-2")
        self.token = None

    def _get_access_token(self):
        if self.token: return self.token

        # 1. S3 캐시 확인
        try:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            cache = json.loads(obj["Body"].read().decode("utf-8"))
            if cache.get("expires_at", 0) > datetime.now().timestamp() + 60:
                    self.token = cache["access_token"]
                    return self.token
        except Exception as e:
            logging.warning(f"[KIS Client] S3 토큰 캐시를 읽는 중 오류 발생: {e}")

        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        try:
            res = requests.post(url, json=body, timeout=5)
            if res.status_code == 200:
                data = res.json()
                self.token = data["access_token"]
                
                # 2. S3 저장 (내가 처음이면 저장)
                try:
                    expires_in = int(data.get("expires_in", 86400))
                    payload = {
                        "access_token": self.token,
                        "expires_at": datetime.now().timestamp() + expires_in - 60
                    }
                    self.s3.put_object(Bucket=self.bucket_name, Key=self.s3_key, Body=json.dumps(payload), ContentType="application/json")
                except Exception as e:
                    logging.warning(f"[KIS Client] S3에 새 토큰을 저장하는 중 오류 발생: {e}")
                
                return self.token
        except Exception as e:
            logging.error(f"[KIS Client] API에서 새 토큰을 발급받는 중 오류 발생: {e}")
        return None

    def get_index_price(self, market_code: str, date_str: str) -> tuple[float, float] | None:
        """[수정] 지정된 날짜의 국내 지수 종가와 등락률을 조회합니다."""
        logging.info(f"[KIS Client] {date_str} 기준 지수 가격({market_code}) 조회 시도...")
        token = self._get_access_token()
        if not token: return None

        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKUP03530100",  # 국내업종기간별시세
            "custtype": "P"
        }
        params = {
            "fid_cond_mrkt_div_code": "U",
            "fid_input_iscd": market_code,
            "fid_input_date_1": date_str.replace("-", ""),
            "fid_input_date_2": date_str.replace("-", ""),
            "fid_period_div_code": "D",
        }
        res: Optional[requests.Response] = None
        try:
            res = requests.get(url, headers=headers, params=params, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if data.get('rt_cd') == '0':
                    output = data.get('output2')
                    if output and len(output) > 0:
                        day_data = output[0]
                        return float(day_data['bstp_nmix_clpr']), float(day_data['prdy_ctrt'])
                    else:
                        logging.warning(f"[KIS Client] {date_str} 지수({market_code}) 데이터가 없습니다. 응답: {res.text[:200]}")
                else:
                    logging.warning(f"[KIS Client] API 오류 ({market_code}, {date_str}): {data.get('msg1')}")
            else:
                logging.warning(f"[KIS Client] HTTP 오류 ({market_code}, {date_str}): Status {res.status_code}, Body: {res.text[:200]}")
        except Exception as e:
            logging.error(f"[KIS Client] 지수 일별 가격({market_code}, {date_str}) 조회 중 오류 발생: {e}")
        return None

# ---------------------------------------------------------------------
# ✅ 네이버 금융 클라이언트 (API 키 없이 무료 사용)
# ---------------------------------------------------------------------
class NaverClient:
    def get_index(self, symbol: str) -> tuple[float, float] | None:
        """
        네이버 모바일 API를 통해 지수 조회
        symbol: KOSPI, KOSDAQ, NAS@IXIC(나스닥), SPI@SPX(S&P500), DJI@DJI(다우)
        반환: (현재가, 등락률)
        """
        res: Optional[requests.Response] = None
        try:
            # 국내/해외 URL 분기
            if symbol in ["KOSPI", "KOSDAQ"]:
                url = f"https://m.stock.naver.com/api/index/{symbol}/basic"
            else:
                url = f"https://api.stock.naver.com/index/{symbol}/basic"
            
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            if res.status_code == 200:
                data = res.json()
                # [수정] API 변경에 대응하기 위해 여러 키를 시도
                price_str = data.get('closePrice') or data.get('lastPrice') or data.get('compareToPreviousClosePrice')
                
                # 1순위: fluctuationsRatio (등락률)
                rate_val = data.get('fluctuationsRatio') 
                
                # 2순위: compareToPreviousPrice.rate (객체 내부)
                if rate_val is None:
                    comp = data.get('compareToPreviousPrice', {})
                    rate_val = comp.get('rate')

                if price_str is not None and rate_val is not None:
                    price = float(str(price_str).replace(',', ''))
                    rate = float(rate_val)
                    return price, rate # 등락률은 % 단위로 가정
                logging.warning(f"[Naver Client] 지수({symbol})에서 예상한 키를 찾지 못했습니다. 응답: {json.dumps(data)}")
        except Exception as e:
            response_text = ""
            if res and hasattr(res, 'text'):
                response_text = res.text[:500]
            logging.error(f"[Naver Client] 지수({symbol}) 조회 중 오류 발생: {e}. 응답: {response_text}")
        return None

    def get_exchange(self, symbol="FX_USDKRW") -> tuple[float, float] | None:
        """환율 조회 (기본: 원달러)"""
        res: Optional[requests.Response] = None
        try:
            url = f"https://m.stock.naver.com/front-api/marketIndex/productDetail?category=exchange&reutersCode={symbol}"
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            if res.status_code == 200:
                data = res.json().get('result', {})
                price_str = data.get('closePrice') or data.get('lastPrice') or data.get('compareToPreviousClosePrice')
                
                # 1순위: fluctuationsRatio (등락률)
                rate_val = data.get('fluctuationsRatio')
                
                # 2순위: compareToPreviousPrice.rate (객체 내부)
                if rate_val is None:
                    comp = data.get('compareToPreviousPrice', {})
                    rate_val = comp.get('rate')

                if price_str is not None and rate_val is not None:
                    price = float(str(price_str).replace(',', ''))
                    return price, float(rate_val)
                logging.warning(f"[Naver Client] 환율({symbol})에서 예상한 키를 찾지 못했습니다. 응답: {json.dumps(data)}")
        except Exception as e:
            response_text = ""
            if res and hasattr(res, 'text'):
                response_text = res.text[:500]
            logging.error(f"[Naver Client] 환율({symbol}) 조회 중 오류 발생: {e}. 응답: {response_text}")
        return None

    def get_oil(self, symbol="OIL_CL") -> tuple[float, float] | None:
        """유가 조회 (기본: WTI)"""
        res: Optional[requests.Response] = None
        try:
            url = f"https://m.stock.naver.com/front-api/marketIndex/productDetail?category=oil&reutersCode={symbol}"
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            if res.status_code == 200:
                data = res.json().get('result', {})
                price_str = data.get('closePrice') or data.get('lastPrice') or data.get('compareToPreviousClosePrice')
                
                # 1순위: fluctuationsRatio (등락률)
                rate_val = data.get('fluctuationsRatio')
                
                # 2순위: compareToPreviousPrice.rate (객체 내부)
                if rate_val is None:
                    comp = data.get('compareToPreviousPrice', {})
                    rate_val = comp.get('rate')

                if price_str is not None and rate_val is not None:
                    price = float(str(price_str).replace(',', ''))
                    return price, float(rate_val)
                logging.warning(f"[Naver Client] 유가({symbol})에서 예상한 키를 찾지 못했습니다. 응답: {json.dumps(data)}")
        except Exception as e:
            response_text = ""
            if res and hasattr(res, 'text'):
                response_text = res.text[:500]
            logging.error(f"[Naver Client] 유가({symbol}) 조회 중 오류 발생: {e}. 응답: {response_text}")
        return None

class KisApiExtension:
    """
    한국투자증권(KIS) API를 사용하여 해외 지수, 환율, 원자재 데이터를 수집하는 확장 클래스.
    모든 API 호출 시 상세 로깅을 통해 실패 원인을 추적합니다.
    """
    def __init__(self, app_key, app_secret, s3_bucket="fincore-output-storage", s3_key_path="config/kis_token.json"):
        self.base_url = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
        self.app_key = app_key
        self.app_secret = app_secret
        self.s3_bucket = s3_bucket
        self.s3_key_path = s3_key_path
        self.s3_client = boto3.client('s3')
        self.access_token = self._get_valid_token()

    def _get_valid_token(self):
        """S3에서 토큰을 확인하고, 없거나 만료된 경우 새로 발급받습니다."""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key_path)
            token_data = json.loads(response['Body'].read().decode('utf-8'))
            if token_data.get("expires_at", 0) > datetime.now().timestamp() + 60:
                return token_data['access_token']
        except Exception:
            logging.info("[KisApiExtension] 유효한 캐시 토큰이 없어 새로 발급을 진행합니다.")

        url = f"{self.base_url}/oauth2/tokenP"
        payload = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        try:
            res = requests.post(url, json=payload, timeout=5)
            if res.status_code == 200:
                data = res.json()
                new_token = data['access_token']
                try:
                    expires_in = int(data.get("expires_in", 86400))
                    save_data = {
                        "access_token": new_token,
                        "expires_at": datetime.now().timestamp() + expires_in - 60
                    }
                    self.s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key_path, Body=json.dumps(save_data))
                except Exception as e:
                    logging.warning(f"[KisApiExtension] S3에 새 토큰을 저장하는 중 오류 발생: {e}")
                return new_token
        except Exception as e:
            logging.error(f"[KisApiExtension] 토큰 발급 실패: {e}")
        return None

    def _get_headers(self, tr_id):
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key, "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P" # 개인 고객 기준
        }

    def get_overseas_index(self, symbol: str) -> tuple[float, float] | None:
        """
        해외 지수(S&P 500, 나스닥, 다우존스, 달러인덱스) 조회
        TR_ID: HHDFS00000300
        """
        mapping = {
            'SPI@SPX': ('AMS', '.SPX'),  # S&P 500
            'NAS@IXIC': ('NAS', '.IXIC'), # 나스닥 종합
            'DJI@DJI': ('NYS', '.DJI'),   # 다우존스
            'FX_USDX': ('NYS', '.DXY')    # 달러 인덱스
        }
        
        if symbol not in mapping:
            logging.error(f"[KisApiExtension] 지원하지 않는 지수 심볼: {symbol}")
            return None
        
        excd, symb = mapping[symbol]
        url = f"{self.base_url}/uapi/overseas-stock/v1/quotations/inquire-price"
        params = {"AUTH": "", "EXCD": excd, "SYMB": symb}
        
        try:
            res = requests.get(url, headers=self._get_headers("HHDFS00000300"), params=params)
            data = res.json()
            
            if data.get("rt_cd") == "0" and 'output' in data:
                output = data['output']
                # ovrs_nmix_prpr: 현재 지수, prdy_ctrt: 대비율
                return float(output['ovrs_nmix_prpr']), float(output['prdy_ctrt'])
            else:
                logging.warning(
                    f"[KisApiExtension] 지수 조회 실패({symbol}): {data.get('msg1')}\n"
                    f"파라미터: {params}, 응답전문: {res.text[:500]}"
                )
        except Exception as e:
            logging.error(f"[KisApiExtension] 지수 호출 중 예외 발생: {str(e)}")
        return None

    def get_exchange_rate(self, symbol: str) -> tuple[float, float] | None:
        """
        원/달러 환율 조회 및 달러인덱스(지수 API 활용)
        """
        if symbol == 'FX_USDX':
            return self.get_overseas_index('FX_USDX')
        
        if symbol != 'FX_USDKRW':
            return None

        url = f"{self.base_url}/uapi/overseas-stock/v1/quotations/inquire-price"
        # 환율 데이터의 경우 거래소를 FX로 지정하여 지수 API를 통해 조회하는 방식이 가장 안정적입니다.
        params = {"AUTH": "", "EXCD": "FX", "SYMB": "USDKRW"}
        
        try:
            res = requests.get(url, headers=self._get_headers("HHDFS00000300"), params=params)
            data = res.json()
            if data.get("rt_cd") == "0" and 'output' in data:
                output = data['output']
                return float(output['ovrs_nmix_prpr']), float(output['prdy_ctrt'])
            else:
                logging.warning(
                    f"[KisApiExtension] 환율 조회 실패: {data.get('msg1')}\n"
                    f"응답전문: {res.text[:500]}"
                )
        except Exception as e:
            logging.error(f"[KisApiExtension] 환율 호출 중 예외 발생: {str(e)}")
        return None

    def get_commodity_price(self, symbol: str) -> tuple[float, float] | None:
        """
        WTI 유가 선물 조회 (TR_ID: HHDFS76240000)
        """
        if symbol != 'OIL_CL':
            return None
            
        url = f"{self.base_url}/uapi/overseas-future/v1/quotations/inquire-price"
        # CL000: WTI 선물 최근물(연속) 심볼
        params = {"SYMB": "CL000"}
 
        try:
            res = requests.get(url, headers=self._get_headers("HHDFS76240000"), params=params)
            data = res.json()
            
            # 해외선물의 경우 응답 구조가 'output1'임에 주의
            if data.get("rt_cd") == "0" and 'output1' in data:
                output = data['output1'] # 해외선물은 output1
                return float(output['last']), float(output['rate'])
            else:
                logging.warning(
                    f"[KisApiExtension] 원자재 조회 실패: {data.get('msg1')}\n"
                    f"파라미터: {params}, 응답전문: {res.text[:500]}"
                )
        except Exception as e:
            logging.error(f"[KisApiExtension] 원자재 호출 중 예외 발생: {str(e)}")
        return None

# 캐시 추가 (중복 호출 방지)
_MARKET_OVERVIEW_CACHE = {}

def get_market_overview_safe(ref_date: _date) -> dict:
    """기존 데이터 조회 실패 시 KIS API로 국내 지수 심폐소생"""
    ref_str = str(ref_date)
    if ref_str in _MARKET_OVERVIEW_CACHE:
        return _MARKET_OVERVIEW_CACHE[ref_str]

    snap = {"indices": {}, "fx": {}, "commodities": {}, "crypto": {}}
    
    indices = snap.setdefault("indices", {})
    fx = snap.setdefault("fx", {})
    commodities = snap.setdefault("commodities", {})

    # --- [수정] 클라이언트 인스턴스 생성 ---
    kis_ext = None
    if os.getenv("KIS_APP_KEY"):
        kis_ext = KisApiExtension(app_key=os.getenv("KIS_APP_KEY"), app_secret=os.getenv("KIS_APP_SECRET"))
    
    nc = NaverClient() # NaverClient는 최종 비상용으로 유지
    
    # 1. [수정] 로컬에 수집된 지수 파일에서 데이터 읽기 (가장 정확)
    # daily_runner가 수집한 kr_market_index.csv 파일을 사용하여 어제 종가 기준으로 조회
    try:
        index_file = PROJECT_ROOT / "iceage" / "data" / "raw" / "kr_market_index.csv"
        if index_file.exists():
            df_idx = pd.read_csv(index_file, thousands=',') # 쉼표(,)를 숫자로 인식
            
            # 날짜 컬럼 찾기 및 datetime 객체로 변환
            date_col = next((c for c in df_idx.columns if '날짜' in c or 'date' in c.lower()), None)
            if date_col:
                df_idx[date_col] = pd.to_datetime(df_idx[date_col])
                ref_date_data = df_idx[df_idx[date_col].dt.date == ref_date].copy()

                if not ref_date_data.empty:
                    name_col = next(c for c in df_idx.columns if '지수명' in c or 'name' in c.lower())
                    close_col = next(c for c in df_idx.columns if '종가' in c or 'close' in c.lower())
                    rate_col = next(c for c in df_idx.columns if '등락률' in c or 'rate' in c.lower())

                    kospi_row = ref_date_data[ref_date_data[name_col] == '코스피']
                    if not kospi_row.empty: indices["KOSPI"] = (kospi_row.iloc[0][close_col], kospi_row.iloc[0][rate_col])

                    kosdaq_row = ref_date_data[ref_date_data[name_col] == '코스닥']
                    if not kosdaq_row.empty: indices["KOSDAQ"] = (kosdaq_row.iloc[0][close_col], kosdaq_row.iloc[0][rate_col])
    except Exception as e:
        logging.warning(f"[get_market_overview_safe] 로컬 지수 파일 처리 중 오류: {e}")

    # 2. KIS API (로컬 파일 실패 시 백업) - [수정] 전일 종가 조회
    if "KOSPI" not in indices and os.getenv("KIS_APP_KEY"):
        kis = KisClient()
        date_str = ref_date.isoformat()
        if (k_data := kis.get_index_price("0001", date_str)): indices["KOSPI"] = k_data
        if (k_data := kis.get_index_price("1001", date_str)): indices["KOSDAQ"] = k_data

    # 3. [수정] KIS 확장 API로 해외 시세 조회 (1순위) / 실패 시 Naver로 대체 (2순위)
    def fetch_with_fallback(kis_method, nc_method, *args):
        if kis_ext and (data := kis_method(*args)):
            return data
        return nc_method(*args)

    # 국내 지수 최종 백업
    if "KOSPI" not in indices: indices["KOSPI"] = nc.get_index("KOSPI")
    if "KOSDAQ" not in indices: indices["KOSDAQ"] = nc.get_index("KOSDAQ")

    # 해외 지수
    indices["S&P 500"] = fetch_with_fallback(kis_ext.get_overseas_index, nc.get_index, "SPI@SPX")
    indices["NASDAQ"] = fetch_with_fallback(kis_ext.get_overseas_index, nc.get_index, "NAS@IXIC")
    indices["Dow Jones"] = fetch_with_fallback(kis_ext.get_overseas_index, nc.get_index, "DJI@DJI")

    # 환율 및 달러인덱스
    fx["USD/KRW"] = fetch_with_fallback(kis_ext.get_exchange_rate, nc.get_exchange, "FX_USDKRW")
    fx["DXY"] = fetch_with_fallback(kis_ext.get_exchange_rate, nc.get_exchange, "FX_USDX")

    # 원자재
    commodities["WTI"] = fetch_with_fallback(kis_ext.get_commodity_price, nc.get_oil, "OIL_CL")

    # 데이터가 없는 항목은 제거
    snap["indices"] = {k: v for k, v in indices.items() if v}
    snap["fx"] = {k: v for k, v in fx.items() if v}
    snap["commodities"] = {k: v for k, v in commodities.items() if v}

    _MARKET_OVERVIEW_CACHE[ref_str] = snap
    return snap

def _get_newsletter_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    if env in ("", "prod"):
        return ""
    return f"-{env}"

# 1. LLM에게 보낼 재료를 풍성하게 만드는 함수
def _build_llm_payload(ref_date: str) -> dict:
    """LLM에게 보낼 재료 준비 (전략 의도 포함)"""
    ref = _date.fromisoformat(ref_date)
    
    # (1) 시장 요약
    snap = get_market_overview_safe(ref)
    
    headline_bits = []
    indices = snap.get("indices", {})
    if "KOSPI" in indices: headline_bits.append(f"코스피 {indices['KOSPI'][1]:+.2f}%")
    if "KOSDAQ" in indices: headline_bits.append(f"코스닥 {indices['KOSDAQ'][1]:+.2f}%")
    if "S&P 500" in indices: headline_bits.append(f"S&P500 {indices['S&P 500'][1]:+.2f}%")
    index_summary = " · ".join(headline_bits)

    # (2) 시그널 종목
    signal_items = []
    try:
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        
        candidates = []
        for r in results.get('panic_buying', []):
            r['_strat_hint'] = "투매가 과도하여 기술적 반등이 기대되는 구간"
            candidates.append(r)
        for r in results.get('fallen_angel', []):
            r['_strat_hint'] = "낙폭 과대 우량주의 저점 매수 기회"
            candidates.append(r)
        for r in results.get('kings_shadow', []):
            r['_strat_hint'] = "대형주 상승 추세 중 매물 소화 과정 (눌림목)"
            candidates.append(r)
        for r in results.get('overheat_short', []):
            r['_strat_hint'] = "단기 폭등으로 인한 피로감 누적, 차익 실현 매물 주의 (고점 징후)"
            candidates.append(r)
                         
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        top_rows = candidates[:5]
        
        event_map = _get_internal_events(ref_date)
        
        for r in top_rows:
            name = r.get('name', '')
            item = {
                "name": name,
                "change_rate": f"{float(r.get('chg', 0)):+.2f}%",
                "volume_z": f"{float(r.get('tv_z', 0)):.1f}배",
                "strategy_intent": r.get('_strat_hint', ''),
                "keywords": event_map.get(name, ""),
                "is_bull": "매수" in r.get('_sentiment', '매수')
            }
            signal_items.append(item)
            
    except Exception as e:
        print(f"[WARN] LLM Payload 생성 오류: {e}")

    global_items = load_global_news(ref_date, limit=3)
    articles = [{"title": i.get("title_en",""), "snippet": i.get("summary_en","")} for i in global_items]

    return {
        "ref_date": ref_date,
        "index_summary_line": index_summary,
        "signals": signal_items,
        "global_news": articles,
    }

# 2. 테마 섹션
def section_themes(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    themes = get_sector_themes(ref)
    if not themes: return "## Today’s Market Themes\n\n오늘은 섹터 기준으로 두드러진 테마 움직임이 크게 관찰되지 않았습니다."
    
    total_turnover = sum(max(getattr(t, "turnover_sum", 0.0), 0.0) for t in themes)
    
    lines = ["## Today’s Market Themes", f"기준일: {ref_date}", ""]
    
    for t in themes[:3]:
        lines.append(f"### {t.sector}")
        lines.append(f"- 섹터 평균 수익률: **{t.avg_return:+.2f}%**")
        
        if total_turnover > 0:
            share = max(getattr(t, "turnover_sum", 0.0), 0.0) / total_turnover * 100
            flame_count = min(5, int(share // 10))
            if flame_count == 0 and share > 1.0: flame_count = 1
            flames = "🔥" * flame_count
            
            if share > 30: comment = "돈이 이 섹터에 쏟아졌습니다."
            elif share > 20: comment = "주도 섹터의 모습을 보였습니다."
            elif share > 10: comment = "유의미한 자금이 유입되었습니다."
            else: comment = "특정 종목 위주로 움직였습니다."
            
            lines.append(f"- 💰 수급 집중도: {flames} **({share:.1f}%)** - _{comment}_")
        
        lines.append(f"- 대표 종목: {', '.join(t.top_stocks)}")
        lines.append("")
        
    return "\n".join(lines)

def _ensure_llm_bundle(ref_date: str) -> dict:
    if ref_date in _LLM_BUNDLE_CACHE:
        return _LLM_BUNDLE_CACHE[ref_date]

    bundle = {}
    # LLM 드라이버가 성공적으로 import되었는지 확인
    if generate_newsletter_bundle:
        payload = _build_llm_payload(ref_date)
        try:
            bundle = generate_newsletter_bundle(payload)
        except Exception as e:
            print("[WARN] LLM 번들 생성 실패:", repr(e))
            bundle = {}
    
    _LLM_BUNDLE_CACHE[ref_date] = bundle
    return bundle

ENABLE_INVESTOR_FLOW_SECTION = False 

def section_header_intro(ref_date: str) -> str:
    bundle = _ensure_llm_bundle(ref_date)
    title = bundle.get("title") or f"The Signalist Daily — {ref_date}"
    kicker = bundle.get("kicker") or ""
    market_summary = bundle.get("market_one_liner") or ""
    
    ref = _date.fromisoformat(ref_date)
    snap = get_market_overview_safe(ref)
    
    indices = snap.get("indices", {})
    fx = snap.get("fx", {})
    commodities = snap.get("commodities", {})
    crypto = snap.get("crypto", {})

    def _fmt(key, label=None):
        if key in indices: val, pct = indices[key]
        elif key in fx: val, pct = fx[key]
        elif key in commodities: val, pct = commodities[key]
        elif key in crypto: val, pct = crypto[key]
        elif "WTI" in key and "WTI" in commodities: val, pct = commodities["WTI"]
        elif "WTI" in key and "WTI Crude" in commodities: val, pct = commodities["WTI Crude"]
        elif "BTC" in key and "BTC/USD" in crypto: val, pct = crypto["BTC/USD"]
        else: return ""
        
        icon = "🔺" if pct > 0 else ("🔹" if pct < 0 else "-")
        lbl = label if label else key
        return f"{lbl} {val:,.2f} ({icon} {pct:+.2f}%)"

    line_kr = []
    for k, l in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥"), ("USD/KRW", "원/달러")]:
        r = _fmt(k, l)
        if r: line_kr.append(r)
        
    line_us = []
    for k, l in [("Dow Jones", "다우"), ("NASDAQ", "나스닥"), ("S&P 500", "S&P500")]:
        r = _fmt(k, l)
        if r: line_us.append(r)
        
    line_macro = []
    for k, l in [("WTI", "WTI유"), ("BTC", "비트코인")]:
        r = _fmt(k, l)
        if r: line_macro.append(r)

    lines = [f"# {title}", ""]
    if kicker:
        lines.append(f"_{kicker}_")
        lines.append("")
    
    lines.append("## 오늘의 시장 한눈에 보기")
    lines.append(f"기준일: {ref_date}")
    lines.append("")
    
    if market_summary:
        lines.append(market_summary)
        lines.append("")
    
    if line_kr: lines.append(f"**한국**: " + " │ ".join(line_kr)); lines.append("")
    if line_us: lines.append(f"**미국**: " + " │ ".join(line_us)); lines.append("")
    if line_macro: lines.append(f"**기타**: " + " │ ".join(line_macro)); lines.append("")

    if not line_kr and not line_us:
        lines.append("> _시장 지표 데이터 로딩에 실패했습니다. 외부 API 서비스 점검이 필요할 수 있습니다._")
        lines.append("")

    return "\n".join(lines)

def _select_signalist_today_rows(ref: _date) -> List[SignalRow]:
    try: all_rows = detect_signals_from_volume_anomaly_v2(ref)
    except Exception: all_rows = []
    if not all_rows: return []

    def _vol(r):
        try: return abs(float(getattr(r, "vol_sigma", 0.0)))
        except Exception: return 0.0
    candidates = sorted(all_rows, key=_vol, reverse=True)

    def _sector(r):
        val = getattr(r, "sector", "") or getattr(r, "theme", "")
        return str(val).strip()

    pos_rows = [r for r in candidates if getattr(r, "vol_sigma", 0.0) > 0]
    neg_rows = [r for r in candidates if getattr(r, "vol_sigma", 0.0) < 0]
    TOP_N = 5; PER_SECTOR_LIMIT = 2
    selected: list = []; seen: set[tuple] = set(); sector_counts: dict[str, int] = {}

    def _can_add(r) -> bool:
        k = (r.name, getattr(r, "vol_sigma", 0.0))
        if k in seen: return False
        sec = _sector(r)
        if not sec: return True
        if sector_counts.get(sec, 0) >= PER_SECTOR_LIMIT: return False
        return True

    def _add(r):
        if not _can_add(r): return
        k = (r.name, getattr(r, "vol_sigma", 0.0))
        seen.add(k); selected.append(r)
        sec = _sector(r)
        if sec: sector_counts[sec] = sector_counts.get(sec, 0) + 1

    if pos_rows: _add(pos_rows[0])
    if neg_rows: _add(neg_rows[0])
    for r in candidates:
        if len(selected) >= TOP_N: break
        _add(r)
    if len(selected) < TOP_N:
        for r in candidates:
            if len(selected) >= TOP_N: break
            k = (r.name, getattr(r, "vol_sigma", 0.0))
            if k in seen: continue
            seen.add(k); selected.append(r)
    return selected

# ---------------------------------------------------------
# [핵심] 이슈 키워드 추출기 V2 (정규식 강화 버전 유지)
# ---------------------------------------------------------
def _extract_keyword_from_title(title: str, stock_name: str) -> str:
    if not title: return "-"
    
    # 1. 괄호 및 대괄호 안의 내용 제거 (예: [특징주], (속보))
    title = re.sub(r"\[.*?\]", " ", title)
    title = re.sub(r"\(.*?\)", " ", title)
    
    # 2. 종목명 제거 (정확도 향상을 위해 공백으로 치환)
    title = title.replace(stock_name, " ")
    
    # 3. 특수문자 제거 (따옴표, 점, 쉼표, 줄표 등 -> 공백)
    # 한글, 영문, 숫자 빼고 다 지움
    title = re.sub(r"[^가-힣a-zA-Z0-9\s]", " ", title)
    
    # 4. 불용어(Stopwords) 제거 - 뉴스 상투어
    stop_words = [
        "특징주", "급등", "상승", "하락", "약세", "강세", "주가", "전망", "이슈", 
        "공시", "체결", "규모", "종목", "관련주", "테마", "분석", "속보", "단독",
        "영향", "주목", "최고", "최저", "경신", "돌파", "마감", "출발", "오전", "오후",
        "포착", "체크", "주의", "비상", "기대", "우려", "쇼크", "서프라이즈", "실적",
        "발표", "공개", "개시", "성공", "체결", "확정", "진입", "확대", "축소", "상한가", "하한가"
    ]
    for w in stop_words:
        title = title.replace(w, " ")
        
    # 5. 숫자 제거 및 1글자 제거
    words = title.split()
    cleaned_words = []
    for w in words:
        if re.search(r"\d", w): continue
        if len(w) < 2: continue
        
        # 끝에 붙은 조사 제거 (간단한 규칙 기반)
        if len(w) >= 3 and w[-1] in ['에', '로', '을', '를', '가', '이', '은', '는', '의']:
             w = w[:-1]
        cleaned_words.append(w)
        
    if not cleaned_words: return "-"
        
    # 6. 가장 긴 단어 선택
    return max(cleaned_words, key=len)

def _get_internal_events(ref_date: str) -> dict[str, str]:
    news_path = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_stock_event_news_{ref_date}.jsonl"
    event_map = {}
    if not news_path.exists(): return {}
    
    with news_path.open(encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                name = item.get("stock_name")
                title = item.get("title")
                if name and title:
                    if name not in event_map:
                        keyword = _extract_keyword_from_title(title, name)
                        if keyword and keyword != "-":
                            event_map[name] = keyword
            except: continue
    return event_map

def section_market_thermometer(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    try:
        snap = get_market_overview_safe(ref)
        indices = snap.get("indices", {})
        changes = []
        if "KOSPI" in indices: changes.append(indices["KOSPI"][1])
        if "KOSDAQ" in indices: changes.append(indices["KOSDAQ"][1])
        if not changes: return ""
        avg_chg = sum(changes) / len(changes)
    except: return ""

    if avg_chg >= 1.5:
        status = "🔥 과열 (Extreme Greed)"; gauge = "[🟥🟥🟥🟥🟥]"; comment = "시장이 뜨겁습니다. 추격 매수보다는 차익 실현을 고려할 구간입니다."
    elif avg_chg >= 0.5:
        status = "☀️ 맑음 (Greed)"; gauge = "[🟥🟥🟥⬜⬜]"; comment = "투자 심리가 살아났습니다. 주도주 위주의 접근이 유효합니다."
    elif avg_chg >= -0.5:
        status = "☁️ 흐림 (Neutral)"; gauge = "[⬜⬜🟩⬜⬜]"; comment = "방향성 탐색 구간입니다. 개별 종목 이슈에 집중하세요."
    elif avg_chg >= -1.5:
        status = "☔ 비 (Fear)"; gauge = "[🟦🟦🟦⬜⬜]"; comment = "투심이 위축되었습니다. 보수적인 관점이 필요합니다."
    else:
        status = "❄️ 혹한 (Extreme Fear)"; gauge = "[🟦🟦🟦🟦🟦]"; comment = "공포 구간입니다. 투매 동참보다는 '패닉 바잉' 기회를 노리세요."

    return dedent(f"""
    ### 🌡️ 오늘의 시장 온도: {status}
    **{gauge}**
    > *"{comment}"*
    """).strip()

def section_signalist_today(ref_date: str) -> str:
    try:
        from iceage.src.pipelines.final_strategy_selector import StrategySelector
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        
        candidates = []
        for r in results.get('panic_buying', []) + results.get('fallen_angel', []) + results.get('kings_shadow', []):
            r['_sentiment'] = '📈 매수 우위'
            bucket = r.get('size_bucket', '')
            if bucket == 'large': r['_tone'] = "🔵 대형주 수급"
            elif bucket == 'mid': r['_tone'] = "🟡 중형주 반등"
            else: r['_tone'] = "🟢 소형주 급등"
            candidates.append(r)
            
        shorts = results.get('overheat_short', [])
        if shorts:
            shorts = sorted(shorts, key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)[:1]
            for r in shorts:
                r['_sentiment'] = '📉 매도 우위'
                r['_tone'] = "🚨 과열 경보"
                candidates.append(r)
            
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        rows = candidates[:5]
        
    except Exception as e:
        rows = []
        logging.error(f"Signalist Today 섹션 생성 중 오류 발생: {e}")

    if not rows:
        return "## 오늘의 레이더 포착 (The Signalist Radar)\n\n포착된 종목이 없습니다."

    event_map = _get_internal_events(ref_date)

    intro = dedent("""
    ## 오늘의 레이더 포착 (The Signalist Radar)
    **"데이터가 발견한 수급의 변곡점"**
    Signalist레이더는 시총별 특성과 거래대금 괴리율을 입체적 분석하여, **유의미한 흐름이 포착된 종목**을 선별합니다.
    단순한 가격 등락이 아닌, **평소 대비 비정상적인 거래 강도**를 기반으로 가능성이 높은 구간을 탐지했습니다.
    """).strip()

    header = "| 종목명 | 종가 | 등락 (폭) | 괴리율 | 수급 방향 | 이슈 키워드 |"
    sep = "|---|---|---|---|---|---|"
    
    body = []
    for r in rows:
        name = r.get('name', '')
        close_val = int(r.get('close', 0))
        close_str = f"{close_val:,}"
        chg_pct = float(r.get('chg', 0))
        prev_close = close_val / (1 + chg_pct/100)
        chg_won = int(close_val - prev_close)
        
        if chg_pct > 0: chg_str = f"**+{chg_pct:.2f}%**<br><small>(▲{chg_won:,})</small>"
        elif chg_pct < 0: chg_str = f"{chg_pct:.2f}%<br><small>(▼{abs(chg_won):,})</small>"
        else: chg_str = "0.00%"
            
        sigma = f"{float(r.get('tv_z', 0)):+.1f}σ"
        display_tone = f"{r.get('_sentiment', '-')}<br>({r.get('_tone', '')})"
        event_key = event_map.get(name, "-")

        body.append(f"| {name} | {close_str} | {chg_str} | {sigma} | {display_tone} | {event_key} |")

    table = "\n".join([header, sep] + body)
    
    memo_lines = ["\n### 🧐 종목별 관찰 메모"]
    bundle = _ensure_llm_bundle(ref_date)
    sig_comments = bundle.get("signal_comments") or {}
    
    for r in rows:
        name = r.get('name')
        comment = sig_comments.get(name) or "특이 수급 포착"
        memo_lines.append(f"- **{name}**: {comment}")

    memo_md = "\n".join(memo_lines)
    
    return f"{intro}\n\n기준일: {ref_date}\n\n{table}\n{memo_md}\n\n_위 리스트는 알고리즘 추출 결과이며, 투자 권유가 아닙니다._"

def section_signalist_history(ref_date: str, window_days: int = 90) -> str:
    ref = _date.fromisoformat(ref_date)
    return build_signalist_history_markdown(ref, lookback_days=window_days)

def load_kr_news_cleaned(ref_date: str, limit: int = 5) -> list[dict]:
    path = Path("iceage") / "data" / "processed" / f"kr_news_cleaned_{ref_date}.jsonl"
    if not path.exists(): return []
    items = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            try: items.append(json.loads(line))
            except: continue
            if len(items) >= limit: break
    return items

def load_global_news(ref_date: str, limit: int = 3) -> list[dict]:
    path = Path("iceage") / "data" / "processed" / f"global_news_{ref_date}.jsonl"
    if not path.exists(): return []
    items = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            try: items.append(json.loads(line))
            except: continue
            if len(items) >= limit: break
    return items

def section_news_digest(ref_date: str) -> str:
    kr_items = load_kr_news_cleaned(ref_date, limit=5)
    global_items = load_global_news(ref_date, limit=3)
    lines = ["## Today’s Top News"]

    if kr_items:
        lines.append("\n### 국내 주요 뉴스\n")
        for i, item in enumerate(kr_items, 1):
            lines.append(f"{i}. [{item.get('title')}]({item.get('link')}) ({item.get('source')})")
        lines.append("")

    if global_items:
        bundle = _ensure_llm_bundle(ref_date)
        llm_summary = bundle.get("global_summary")
        lines.append("\n### 해외 주요 뉴스\n")
        if isinstance(llm_summary, dict):
            if llm_summary.get("headline"): lines.append(f"**{llm_summary['headline']}**\n")
            if llm_summary.get("summary"): lines.append(f"{llm_summary['summary']}\n")
            for b in llm_summary.get("bullets", []): lines.append(f"- {b}")
            lines.append("")
        for i, item in enumerate(global_items, 1):
            t = item.get("title_en") or item.get("title")
            lines.append(f"{i}. [{t}]({item.get('link')}) ({item.get('source')})")

    return "\n".join(lines).strip()

def section_global_minute(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    snap = get_market_overview_safe(ref)
    indices = snap.get("indices", {})
    fx = snap.get("fx", {})
    commodities = snap.get("commodities", {})

    sp_level, sp_pct = indices.get("S&P 500", (None, None))
    ndq_level, ndq_pct = indices.get("NASDAQ", (None, None))
    dxy_level, dxy_pct = fx.get("DXY", (None, None))
    wti_level, wti_pct = commodities.get("WTI", commodities.get("WTI Crude", (None, None)))

    lines = ["## Global Minute", f"기준일: {ref_date}", ""]
    lines.append("### US")
    if sp_pct is not None:
        lines.append(f"- 이슈: S&P 500 {sp_level:,.2f} ({sp_pct:+.2f}%), NASDAQ {ndq_pct:+.2f}%")
        if sp_pct > 0.4: impact = "성장주·기술주 중심으로 위험선호가 강화된 흐름입니다."
        elif sp_pct < -0.4: impact = "금리·실적 부담으로 위험자산 회피 심리가 나타난 구간입니다."
        else: impact = "실적·매크로 이벤트를 소화하며 방향성을 탐색하는 조정 구간입니다."
        lines.append(f"- 해석: {impact}")
    else:
        lines.append("- 이슈: 데이터 부족")
        lines.append("- 해석: 미국 증시 데이터를 가져오지 못했습니다.")
    lines.append("")

    lines.append("### 달러/환율")
    if dxy_pct is not None:
        lines.append(f"- 이슈: 달러 인덱스(DXY) {dxy_level:,.2f} ({dxy_pct:+.2f}%)")
        if dxy_pct < -0.3: impact = "달러 약세 구간으로, 신흥국 자산과 위험자산에 상대적으로 우호적인 환경입니다."
        elif dxy_pct > 0.3: impact = "달러 강세로, 안전자산 선호 및 유동성 경계 심리가 반영된 흐름입니다."
        else: impact = "달러가 뚜렷한 방향성 없이 등락하며 단기 이벤트를 관망하는 구간입니다."
        lines.append(f"- 해석: {impact}")
    else:
        lines.append("- 이슈: 데이터 부족")
        lines.append("- 해석: 달러 인덱스 데이터를 가져오지 못했습니다.")
    lines.append("")

    lines.append("### 원자재/에너지")
    if wti_pct is not None:
        lines.append(f"- 이슈: WTI {wti_level:,.2f}달러 ({wti_pct:+.2f}%)")
        if wti_pct > 0.5: impact = "유가 상승으로 인플레이션·원가 부담에 대한 경계가 다시 부각될 수 있는 구간입니다."
        elif wti_pct < -0.5: impact = "유가 하락으로 물가 부담 완화 기대가 커지며 위험자산에 우호적인 환경입니다."
        else: impact = "유가가 박스권 등락을 이어가며 공급·수요 이슈를 소화하는 구간입니다."
        lines.append(f"- 해석: {impact}")
    else:
        lines.append("- 이슈: 데이터 부족")
        lines.append("- 해석: 유가 데이터를 가져오지 못했습니다.")
    lines.append("")

    return "\n".join(lines)

def section_investors_mind(topic: str, body: str) -> str:
    if not topic or not body: return ""
    return dedent(f"""
    ## 🧘 Investor's Mind: {topic}
    {body}
    """).strip()

def _find_col(columns, candidates):
    cols = list(columns)
    for c in candidates:
        if c in columns: return c
    for c in candidates:
        for col in cols:
            if c in str(col): return col
    return None

def _load_turnover_by_market(ref: _date) -> dict[str, float]:
    """
    [수정] 거래대금 컬럼이 있어도 데이터가 0이면, 
    종가*거래량으로 강제 복구하여 반환하는 안전장치 추가
    """
    raw_path = Path("iceage") / "data" / "raw" / f"kr_prices_{ref.isoformat()}.csv"
    if not raw_path.exists(): return {}
    
    try: 
        df = pd.read_csv(raw_path)
    except: return {}
    
    # 숫자 변환 헬퍼
    def _clean(x):
        try: return float(str(x).replace(",", ""))
        except: return 0.0
        
    cols = set(df.columns)
    
    # 1. 시장 구분 컬럼 찾기
    market_col = _find_col(cols, ["market", "시장구분", "시장", "Market"])
    if not market_col: return {}
    
    # 2. 거래대금 우선 시도
    value_col = _find_col(cols, ["trading_value", "거래대금"])
    if value_col:
        df["_turnover_"] = df[value_col].apply(_clean)
    else:
        df["_turnover_"] = 0.0
        
    # 3. [핵심] 거래대금이 비어있거나 합계가 0이면 강제 계산 (심폐소생술)
    if df["_turnover_"].sum() == 0:
        close_col = _find_col(cols, ["close", "종가", "현재가"])
        vol_col = _find_col(cols, ["volume", "거래량"])
        
        if close_col and vol_col:
            df["_turnover_"] = df[close_col].apply(_clean) * df[vol_col].apply(_clean)

    return df.groupby(market_col)["_turnover_"].sum().to_dict()

def section_numbers_that_matter(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    lines = ["## Numbers that Matter", f"기준일: {ref_date}", ""]
    
    by_market = _load_turnover_by_market(ref)
    if by_market:
        lines.append("### 오늘 국내 주식 거래대금 (조원 단위, 추정)")
        total = 0.0
        for market_name, v in by_market.items():
            total += float(v)
            trillions = float(v) / 1_000_000_000_000
            lines.append(f"- {market_name}: {trillions:,.1f}조")
        lines.append(f"- 합계: {total / 1_000_000_000_000:,.1f}조")
        lines.append("")
        
    fx_series = []
    for back in range(4, -1, -1):
        d = ref - timedelta(days=back)
        try:
            snap = get_market_overview_safe(d)
            fx = snap.get("fx", {})
            if "USD/KRW" in fx: fx_series.append((d, fx["USD/KRW"][0]))
        except: continue
        
    if fx_series:
        lines.append("### USD/KRW 환율 (최근 일자)")
        prev = None
        for d, level in fx_series:
            diff = f"({level-prev:+.2f})" if prev else ""
            lines.append(f"- {d.isoformat()}: {level:,.2f} {diff}")
            prev = level
        lines.append("")
        
    if ENABLE_INVESTOR_FLOW_SECTION:
        flow_map = load_investor_flow(ref)
        if flow_map:
            lines.append("### 투자자별 매매 동향 (단위: 억원, 순매수 기준)")
            lines.append("| 시장 | 개인 | 외국인 | 기관 |")
            lines.append("|------|------|--------|------|")
            for m in ["KOSPI", "KOSDAQ"]:
                s = flow_map.get(m)
                if s:
                    p = s.net_by_investor.get("개인", 0)
                    f = s.net_by_investor.get("외국인", 0)
                    i = s.net_by_investor.get("기관", 0)
                    lines.append(f"| {m} | {p:,.1f} | {f:,.1f} | {i:,.1f} |")
            lines.append("")
            
    return "\n".join(lines)

def extract_first_sentence(text: str) -> str:
    if not text: return ""
    cleaned = " ".join(text.split())
    sentences = re.split(r'(?<=[\.!?])\s+', cleaned)
    return sentences[0].strip() if sentences else cleaned.strip()

def section_morning_quote(quote: str) -> str:
    return dedent(f"""
    ## Morning Quote
    > {quote}
    """).strip()

def section_footer() -> str:
    return dedent(f"""
    ---
    본 콘텐츠는 투자 권유 목적이 아닌 정보 제공용입니다.  
    The Signalist © 2025 All Rights Reserved.  [구독해지]  [의견보내기]
    """).strip()

MIND_TOPICS = ["확신보다 유연함", "손실을 대하는 태도", "과잉 확신의 함정", "복리와 기다림", "포지션 사이징"]
def pick_topic_and_body(ref_date: str) -> tuple[str, str]:
    import random
    fallback_topic = random.choice(MIND_TOPICS)
    fallback_body = "평정심을 유지하세요. 시장은 언제나 기회를 줍니다."
    try:
        bundle = _ensure_llm_bundle(ref_date)
        im = bundle.get("investor_mind") or {}
        return im.get("topic", fallback_topic), im.get("body", fallback_body)
    except: return fallback_topic, fallback_body

def render_newsletter(ref_date: str) -> str:
    topic, body = pick_topic_and_body(ref_date)
    parts = [
        section_header_intro(ref_date),
        section_market_thermometer(ref_date),
        section_signalist_today(ref_date),
        section_signalist_history(ref_date),
        section_themes(ref_date),
        # IPO 섹션 제거
        section_global_minute(ref_date),
        section_news_digest(ref_date),
        section_investors_mind(topic, body),
        section_numbers_that_matter(ref_date),
        section_footer()
    ]
    return "\n\n".join([p for p in parts if p])

def log_signalist_today(ref_date: str, rows: list, force: bool = True) -> None:
    if not rows: return
    out_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "signalist_today_log.csv"
    
    new_records = []
    for r in rows:
        if hasattr(r, '__dict__'):
            d = {
                "signal_date": ref_date, "code": getattr(r, "code", ""), "name": r.name,
                "close": r.close, "vol_sigma": float(getattr(r, "vol_sigma", 0.0)),
                "sentiment": getattr(r, "sentiment", ""), "insight": getattr(r, "insight", "")
            }
        else:
            d = {
                "signal_date": ref_date, "code": str(r.get('code', '')).zfill(6),
                "name": r.get('name', ''), "close": r.get('close', 0),
                "vol_sigma": float(r.get('tv_z', 0) or r.get('vol_sigma', 0)),
                "sentiment": r.get('_sentiment') or r.get('sentiment', ''),
                "insight": r.get('_insight') or r.get('insight', '')
            }
        new_records.append(d)
        
    new_records.sort(key=lambda x: abs(x['vol_sigma']), reverse=True)
    new_records = new_records[:5]
    df_new = pd.DataFrame(new_records)
    
    if out_path.exists():
        try:
            df_old = pd.read_csv(out_path, encoding="utf-8-sig")
            
            # [Fix] 컬럼 호환성 체크 (과거 백필 데이터 호환)
            if "date" in df_old.columns and "signal_date" not in df_old.columns:
                df_old.rename(columns={"date": "signal_date"}, inplace=True)
            
            if "tv_z" in df_old.columns and "vol_sigma" not in df_old.columns:
                df_old.rename(columns={"tv_z": "vol_sigma"}, inplace=True)
                
            if "signal_date" not in df_old.columns:
                print("[ERROR] 기존 로그 파일에 'signal_date' 컬럼이 없습니다. 덮어쓰기를 방지하기 위해 병합을 중단합니다.")
                return 

            df_old = df_old[df_old["signal_date"] != ref_date]
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception as e:
            print(f"[ERROR] 기존 로그 파일 병합 실패: {e}")
            print("   -> 기존 데이터를 보존하기 위해 새 데이터를 추가하지 않습니다.")
            return # 안전장치: 에러나면 그냥 리턴 (덮어쓰기 방지)
    else: 
        df_all = df_new
    
    df_all = df_all.sort_values("signal_date")
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ [Log Saved] {ref_date} 시그널 {len(new_records)}개 저장 완료!")

def main():
    cal = TradingCalendar(CalendarConfig())
    if len(sys.argv) >= 2: ref_date = sys.argv[1]
    else: 
        # [수정] 서버의 기본 시간대(UTC) 대신 한국 시간(KST)을 명시적으로 사용
        now_kst = datetime.now(ZoneInfo('Asia/Seoul'))
        ref = compute_reference_date(cal, now_kst)
        ref_date = ref.isoformat()

    print(f"\n📅 Newsletter ref_date: {ref_date}")

    try:
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        candidates = []
        for r in results.get('panic_buying', []) + results.get('fallen_angel', []) + results.get('kings_shadow', []):
            r['_sentiment'] = '📈 매수 우위'
            b = r.get('size_bucket')
            if b == 'small': r['_insight'] = "소형주 수급 변곡점 포착"
            elif b == 'large': r['_insight'] = "대형주 추세 눌림목 포착"
            else: r['_insight'] = "중형주 낙폭 과대 포착"
            candidates.append(r)
        for r in results.get('overheat_short', []):
            r['_sentiment'] = '📉 매도 우위'
            r['_insight'] = "단기 과열권 도달 (고점 경고)"
            candidates.append(r)
            
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        final_rows = candidates[:5]
        
        if final_rows:
            log_signalist_today(ref_date, final_rows)

    except Exception as e:
        print(f"[ERROR] 시그널 생성 중 오류: {e}")

    md = render_newsletter(ref_date)
    
    out_dir = PROJECT_ROOT / "iceage" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = _get_newsletter_env_suffix()
    filename = f"Signalist_Daily_{ref_date}{suffix}.md"
    out_path = out_dir / filename
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"✅ 생성 완료: {out_path}")

if __name__ == "__main__":
    main()
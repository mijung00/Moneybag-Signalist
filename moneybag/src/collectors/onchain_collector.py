import requests
import pandas as pd
from datetime import datetime

class OnChainCollector:
    def __init__(self):
        # [수정] limit=31로 설정하여 한 달 전 데이터까지 확보
        self.url = "https://api.alternative.me/fng/?limit=31"

    def get_whale_ammo(self):
        """
        [고래 심리 데이터 심층 수집]
        현재 값뿐만 아니라 어제, 지난주, 지난달 데이터를 비교 분석
        """
        try:
            # 1. 공포탐욕지수 (Fear & Greed)
            fng_res = requests.get(self.url, timeout=10)
            fng_data = fng_res.json().get('data', [])
            
            if not fng_data: return None

            # 데이터 파싱 (0:오늘, 1:어제, 7:지난주, 30:지난달)
            today = fng_data[0]
            yesterday = fng_data[1] if len(fng_data) > 1 else today
            last_week = fng_data[7] if len(fng_data) > 7 else today
            last_month = fng_data[30] if len(fng_data) > 30 else today

            # 2. 스테이블코인 (기존 로직 유지 - 일단 0으로 처리하거나 필요시 DefiLlama 호출)
            # (여기서는 심리 지표 강화에 집중하므로 생략)
            
            return {
                "current": {
                    "value": int(today['value']),
                    "status": today['value_classification']
                },
                "history": {
                    "yesterday": int(yesterday['value']),
                    "last_week": int(last_week['value']),
                    "last_month": int(last_month['value'])
                },
                "status": "Success"
            }

        except Exception as e:
            print(f"온체인 데이터 수집 실패: {e}")
            return None

if __name__ == "__main__":
    collector = OnChainCollector()
    print(collector.get_whale_ammo())
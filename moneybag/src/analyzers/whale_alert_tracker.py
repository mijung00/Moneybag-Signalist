import os
import pymysql
from datetime import datetime, timedelta

class WhaleAlertTracker:
    def __init__(self):
        # DB 연결 정보는 환경변수에서 로드됩니다.
        pass

    def _get_db_connection(self):
        """
        DB 연결 객체를 반환합니다.
        moralis_listener.py와 동일한 로직을 사용하여 중앙 DB에 접속합니다.
        """
        try:
            return pymysql.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        except Exception as e:
            print(f"❌ [WhaleAlertTracker] DB 연결 실패: {e}")
            return None

    def analyze_volume_anomaly(self, pair_future: str, hours: int = 24):
        """
        [수정] 로컬 파일이나 ccxt 대신, 중앙 DB에서 지난 24시간 거래량을 집계하여 분석합니다.
        """
        symbol = pair_future.replace('/USDT', '')
        conn = self._get_db_connection()
        if not conn:
            return None # DB 연결 실패 시 None 반환

        try:
            with conn.cursor() as cursor:
                now = datetime.now()
                time_threshold = now - timedelta(hours=hours)
                
                # 지난 24시간(현재 구간) 거래량 합계 조회
                sql = """
                SELECT SUM(amount_usd) as total_volume
                FROM whale_transactions
                WHERE symbol = %s AND timestamp >= %s
                """
                cursor.execute(sql, (symbol, time_threshold))
                result = cursor.fetchone()
                current_volume = result['total_volume'] if result and result['total_volume'] else 0

                # 그 이전 24시간(비교 구간) 거래량 합계 조회
                prev_time_threshold = time_threshold - timedelta(hours=hours)
                sql_prev = """
                SELECT SUM(amount_usd) as total_volume
                FROM whale_transactions
                WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                """
                cursor.execute(sql_prev, (symbol, prev_time_threshold, time_threshold))
                result_prev = cursor.fetchone()
                previous_volume = result_prev['total_volume'] if result_prev and result_prev['total_volume'] else 0
        except Exception as e:
            print(f"❌ [WhaleAlertTracker] DB 쿼리 실패: {e}")
            return None
        finally:
            if conn.open:
                conn.close()

        # 거래량 급증 비율 계산
        if previous_volume == 0:
            vol_spike_ratio = 5.0 if current_volume > 0 else 1.0
        else:
            vol_spike_ratio = current_volume / previous_volume

        return {
            'symbol': symbol,
            'vol_spike_ratio': vol_spike_ratio
        }

# --- 테스트 실행용 ---
if __name__ == "__main__":
    tracker = WhaleAlertTracker()
    targets = ["BTC/USDT", "ETH/USDT", "DOGE/USDT", "XRP/USDT"]
    
    print("🐳 고래 추적 레이더 가동 중...")
    for t in targets:
        res = tracker.analyze_volume_anomaly(t.replace("USDT", "/USDT"))
        if res:
            print(f"[{t}] 거래량 스파이크 비율: {res['vol_spike_ratio']:.2f}x")
        else:
            print(f"[{t}] 데이터 분석 실패")
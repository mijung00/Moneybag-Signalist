import os
import json
from flask import Flask, request, abort
from datetime import datetime
import pymysql

# --- 설정 ---
# 이 파일은 Moralis Stream Webhook이 호출할 때마다 거래 내역을 기록합니다.
# 이 서버는 카드뉴스 생성기와 별도로, 항상 실행되어야 합니다.
#
# 실행 방법:
# (venv) PS C:\ubuntu> python -m moneybag.src.webhooks.moralis_listener
#
# Moralis 설정:
# 1. Moralis Admin > Streams 에서 새로운 Stream 생성
# 2. Webhook URL: http://<사장님_서버_IP>:5001/moralis-webhook
# 3. Monitored Address: 0xdac17f958d2ee523a2206206994597c13d831ec7 (USDT)
# 4. Topic0: Transfer(address,address,uint256)
# 5. Advanced Options (필터):
#    { "gte": ["value", "1000000000000"] }
# 6. 'Listen to all addresses' 옵션은 현재 플랜에서 지원하는지 확인 필요

app = Flask(__name__)

# 데이터 저장 경로
# DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'out')
# WHALE_LOG_FILE = os.path.join(DATA_DIR, 'whale_transactions.jsonl')

# Moralis Stream 설정에서 복사한 API 키 (Webhook 서명 검증용)
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY")

def verify_signature(request):
    """Webhook 요청의 서명을 검증합니다."""
    signature = request.headers.get('x-signature')
    if not signature:
        return False
    
    # Moralis Python SDK의 서명 검증 로직을 직접 구현하거나,
    # 간단한 검증을 위해 API 키 존재 여부만 확인할 수도 있습니다.
    # 여기서는 SDK 없이 간단히 처리합니다.
    # 실제 프로덕션에서는 Moralis SDK의 `verify_signature` 사용을 권장합니다.
    # from moralis.streams.helpers import verify_signature
    # is_valid = verify_signature(body=request.data.decode(), signature=signature, api_key=MORALIS_API_KEY)
    
    # 지금은 헤더 존재 여부만 체크
    return True

def get_db_connection():
    """DB 연결 객체 반환"""
    # 환경변수에서 DB 접속 정보 로드
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route('/moralis-webhook', methods=['POST'])
def moralis_webhook():
    if not verify_signature(request):
        print("❌ [Webhook] 서명 검증 실패. 요청을 거부합니다.")
        abort(401)

    payload = request.json
    print(f"🔔 [Webhook] Moralis로부터 데이터 수신!")

    # 실제 거래 내역(logs) 처리
    for tx in payload.get('erc20Transfers', []):
        try:
            token_symbol = tx.get('tokenSymbol', 'UNKNOWN')
            token_decimals = int(tx.get('tokenDecimals', '6'))
            value_raw = int(tx.get('value', '0'))
            value_usd = value_raw / (10**token_decimals)

            # 데이터 포맷을 기존 MoralisTracker와 유사하게 맞춤
            whale_tx = {
                'symbol': token_symbol,
                'amount_usd': value_usd,
                'from': {'owner': tx.get('from'), 'owner_type': 'wallet'}, # label 정보는 스트림에 없음
                'to': {'owner': tx.get('to'), 'owner_type': 'wallet'},
                'timestamp': payload.get('block', {}).get('timestamp'),
                'transaction_hash': tx.get('transactionHash')
            }

            # [수정] 파일 대신 DB에 데이터 저장
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    sql = """
                    INSERT IGNORE INTO whale_transactions 
                    (symbol, amount_usd, from_address, to_address, transaction_hash, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    # Moralis 타임스탬프(ISO 8601)를 DB DATETIME 형식으로 변환
                    ts_obj = datetime.fromisoformat(whale_tx['timestamp'].replace('Z', '+00:00'))
                    ts_str = ts_obj.strftime('%Y-%m-%d %H:%M:%S')
                    
                    cursor.execute(sql, (
                        whale_tx['symbol'],
                        whale_tx['amount_usd'],
                        whale_tx['from']['owner'],
                        whale_tx['to']['owner'],
                        whale_tx['transaction_hash'],
                        ts_str
                    ))
                conn.commit()
                print(f"  -> 💾 DB에 저장: {whale_tx['symbol']} ${whale_tx['amount_usd']:,.0f}")
            finally:
                conn.close()

        except Exception as e:
            print(f"  -> ⚠️ 로그 처리 중 오류: {e}")

    return {"status": "ok"}, 200

if __name__ == '__main__':
    print(f"🐋 Moralis 고래 추적 리스너(Webhook 서버)를 시작합니다.")
    print(f"   - 데이터 저장소: 중앙 DB (whale_transactions 테이블)")
    print(f"   - 수신 주소: http://0.0.0.0:5001/moralis-webhook")
    app.run(host='0.0.0.0', port=5001)
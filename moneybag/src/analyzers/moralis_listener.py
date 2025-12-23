import os
import json
from flask import Flask, request, abort
from datetime import datetime, timezone

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
# [수정] 서버 환경에서는 배포 시 삭제되지 않는 영구 경로에 로그를 저장합니다.
if os.path.exists('/var/log'): # Linux 서버 환경인지 확인
    PERSISTENT_LOG_DIR = '/var/log/moneybag'
else: # 로컬 Windows 환경일 경우 기존 경로 사용
    PERSISTENT_LOG_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'out')

WHALE_LOG_FILE = os.path.join(PERSISTENT_LOG_DIR, 'whale_transactions.jsonl')

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

            # [수정] 파일에 쓰기 전에 영구 로그 디렉터리가 존재하는지 확인하고 없으면 생성합니다.
            os.makedirs(PERSISTENT_LOG_DIR, exist_ok=True)

            # 파일에 한 줄씩 추가 (JSON Lines 형식)
            with open(WHALE_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(whale_tx) + '\n')
            
            print(f"  -> 📝 로그 기록: {whale_tx['symbol']} ${whale_tx['amount_usd']:,.0f}")

        except Exception as e:
            print(f"  -> ⚠️ 로그 처리 중 오류: {e}")

    return {"status": "ok"}, 200

if __name__ == '__main__':
    # 로그 파일 경로 확인 및 생성
    # 로컬에서 직접 실행할 때도 디렉터리가 생성되도록 보장합니다.
    os.makedirs(PERSISTENT_LOG_DIR, exist_ok=True)
    print(f"🐋 Moralis 고래 추적 리스너(Webhook 서버)를 시작합니다.")
    print(f"   - 로그 파일: {WHALE_LOG_FILE}")
    print(f"   - 수신 주소: http://0.0.0.0:5001/moralis-webhook")
    app.run(host='0.0.0.0', port=5001)
import os
import json
from datetime import datetime, timedelta, timezone

class MoralisTracker:
    """
    [구조 변경] Moralis API를 직접 호출하는 대신,
    Webhook 리스너가 기록한 로그 파일을 읽어 대규모 트랜잭션을 분석합니다.
    """
    def __init__(self):
        # 이제 API 키는 리스너(listener)만 사용합니다.
        self.log_file_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'data', 'out', 'whale_transactions.jsonl'
        )
        if not os.path.exists(self.log_file_path):
            print(f"⚠️ [MoralisTracker] 로그 파일이 없습니다: {self.log_file_path}")
            print("   -> Webhook 리스너(moralis_listener.py)가 먼저 실행되어야 합니다.")

    def get_large_transactions(self, limit=5, min_value_usd=1000000):
        """
        기록된 로그 파일에서 최근 12시간 내의 대규모 트랜잭션을 읽어옵니다.
        """
        if not os.path.exists(self.log_file_path):
            return {"error": "로그 파일 없음", "transactions": []}

        all_txs = []
        twelve_hours_ago = datetime.now(timezone.utc) - timedelta(hours=12)

        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        tx = json.loads(line)
                        
                        # 타임스탬프 형식 변환 및 시간대 정보 추가
                        tx_time_str = tx.get('timestamp')
                        if not tx_time_str: continue
                        
                        # ISO 8601 형식 (예: '2025-12-22T10:30:00.000Z') 처리
                        tx_time = datetime.fromisoformat(tx_time_str.replace('Z', '+00:00'))
                        
                        # 최근 12시간 데이터 & 최소 금액 조건 확인
                        if tx_time >= twelve_hours_ago and tx.get('amount_usd', 0) >= min_value_usd:
                            # from/to 주소에 대한 label 정보는 스트림에 없으므로, 'Unknown Wallet'으로 통일
                            tx['from']['owner'] = tx['from'].get('owner', 'Unknown Wallet')
                            tx['to']['owner'] = tx['to'].get('owner', 'Unknown Wallet')
                            all_txs.append(tx)
                            
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        print(f"⚠️ 로그 라인 파싱 실패: {line.strip()} | 오류: {e}")
                        continue

            # 최신순, 금액순으로 정렬
            sorted_txs = sorted(all_txs, key=lambda x: (x.get('timestamp', ''), x.get('amount_usd', 0)), reverse=True)
            
            print(f"✅ [MoralisTracker] 로그 파일에서 최근 12시간 내 거래 {len(sorted_txs)}건 발견.")
            return {"transactions": sorted_txs[:limit]}

        except Exception as e:
            import traceback
            print(f"❌ [MoralisTracker] 로그 파일 처리 중 오류 발생: {e}")
            traceback.print_exc()
            return {"error": str(e), "transactions": []}
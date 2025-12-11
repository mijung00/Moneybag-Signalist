import pandas as pd
# 방금 만든 파일 import
from .strategy_selector import select_top_strategies 

# ... (기존 import들) ...

def generate_final_verdict(market_data):
    """
    기존: 1개만 리턴
    변경: Top 3 리스트를 리턴
    """
    # ... (전략 계산하는 로직들은 그대로 둠) ...
    # (strategy_list 변수에 여러 전략들이 담겨 있다고 가정)
    
    # [수정] Top 3 전략 선정
    # 기존 코드: best_strategy = select_best_strategy(strategy_list)
    top_strategies = select_top_strategies(strategy_list, top_n=3)
    
    # 리턴값을 리스트로 변경!
    return {
        "market_regime": market_regime, # (기존 변수)
        "top_strategies": top_strategies, # [중요] 여기가 핵심!
        "msg": "Analysis Complete"
    }
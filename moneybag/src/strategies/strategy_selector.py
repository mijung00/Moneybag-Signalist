"""
Module: strategy_selector.py
Description: 여러 전략 중에서 점수가 가장 높은 Top 3를 선정합니다.
"""

def select_top_strategies(strategies, top_n=3):
    """
    분석된 전략 리스트에서 상위 N개를 뽑아서 리턴함.
    """
    if not strategies:
        return []

    # 1. 점수(total_score) 기준으로 내림차순 정렬 (점수 높은 게 1등)
    # (만약 딕셔너리에 total_score가 없으면 0점으로 처리)
    sorted_strats = sorted(strategies, key=lambda x: x.get('total_score', 0), reverse=True)
    
    # 2. 상위 N개 자르기
    return sorted_strats[:top_n]
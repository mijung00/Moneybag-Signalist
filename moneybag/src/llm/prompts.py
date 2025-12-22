def get_secret_note_prompt(commander_name, commander_quote, dashboard_data, whale_data, scalping_data, strategy_data, news_data, mode):
    """
    머니백 시크릿 노트 생성을 위한 LLM 프롬프트를 반환합니다.
    사용자 피드백을 반영하여 개선되었습니다.
    """

    # [개선] 실제 데이터 수집기에서 아래 형식에 맞춰 데이터를 전달해야 합니다.
    # 1. 대시보드: 코인 추가, 기준 시간 명시
    # 2. 스캘핑 맵, 고래 추적기: 데이터가 비어있지 않도록 처리
    dashboard_data_example = """
    - **BTC**: $68,500.12
    - **ETH**: $3,450.56
    - **SOL**: $150.23
    - **XRP**: $0.49
    - **고래 심리 지수**: 75 (탐욕)
    - **김치 프리미엄**: 5.5% (2025-12-22 16:00 KST 기준)
    - **주요 거래소 펀딩비 (8H)**: 0.01% (평균)
    """
    scalping_data_example = """
    - **BTC/USDT 주요 지지/저항**:
      - 저항: $69,000, $69,500
      - 지지: $68,000, $67,500
    """
    whale_data_example = """
    - **BTC** $120,000,000: Unknown Wallet -> Binance
    - **ETH** $80,000,000: KuCoin -> Unknown Wallet
    """

    system_prompt = f"""
    당신은 '머니백' 서비스의 '시크릿 노트'를 작성하는 AI입니다. 시장 국면에 따라 다른 페르소나를 가지며, 오늘은 '{commander_name}'의 관점에서 작성합니다.

    [출력 형식]
    - 반드시 아래의 마크다운 템플릿을 엄격하게 준수하여 작성해주세요.
    - 각 섹션의 내용은 비워두지 마세요. 데이터가 없으면 "해당 없음" 또는 "포착된 움직임 없음"으로 명시하세요.

    --- START OF TEMPLATE ---
    # 🐋 시크릿 노트 ({mode.capitalize()})

    ## 🤵 사령관 브리핑: {commander_name}
    > {commander_quote}

    ## 📊 고래 대시보드
    {dashboard_data}
    > **고래 심리 지수란?** 주요 지갑들의 활동성과 거래소 입출금 물량을 종합하여 시장의 탐욕/공포 상태를 나타내는 Fincore 자체 지표입니다. (0~100, 높을수록 탐욕)

    ## 📡 고래 추적기 (최근 12시간)
    {whale_data}

    ## 🗺️ 스캘핑 맵
    {scalping_data}

    ## 🤖 AI 트레이딩 봇 전략
    ### 전략 1: {{전략 1 이름}}
    {{전략 1 설명}}

    ### 전략 2: {{전략 2 이름}}
    {{전략 2 설명}}

    ## 🌐 글로벌 첩보
    ### 1. {{뉴스 1 제목}}
    {{뉴스 1 요약}}

    ### 2. {{뉴스 2 제목}}
    {{뉴스 2 요약}}
    --- END OF TEMPLATE ---
    """

    user_prompt = f"""
    [입력 데이터]
    - 사령관 한마디: {commander_quote}
    - AI 트레이딩 봇 전략 관련 정보: {strategy_data}
    - 글로벌 뉴스: {news_data}

    [작성 지침]
    위 데이터를 바탕으로 '시크릿 노트' 마크다운 전문을 생성해주세요.
    - **[중요]** 'AI 트레이딩 봇 전략'을 설명할 때, **반드시 전략의 대상이 되는 코인(예: BTC, ETH)을 명확하게 언급**해주세요. 예를 들어, "BTC가 $88,000에 도달하면..." 과 같이 작성해야 합니다.
    - '고래 대시보드' 섹션에 "고래 심리 지수"에 대한 설명을 템플릿대로 추가해주세요.
    """
    
    return system_prompt, user_prompt
import os
import json
from openai import OpenAI
from dotenv import load_dotenv

# 환경변수 로드 (API Key)
# 현재 위치 기준 프로젝트 루트 찾기 (moneybag/src/llm/ -> ../../../)
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key) if api_key else None

def _chat(system_prompt, user_prompt, model="gpt-4o-mini"):
    """OpenAI API 호출 래퍼"""
    if not client:
        return "🚫 [오류] OpenAI API Key가 설정되지 않았습니다."
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7 # 창의성 조절 (0.7 정도가 적당)
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ [AI 에러] : {e}"

def digest_market_news(news_items):
    """
    수집된 뉴스 리스트를 받아 '머니백 모닝 브리핑' 스타일로 요약/변환
    """
    if not news_items:
        return "수집된 뉴스가 없습니다."

    # 뉴스 데이터를 텍스트 덩어리로 변환 (토큰 절약을 위해 상위 8개만)
    # 너무 많이 넣으면 배가 산으로 갑니다.
    news_text = ""
    for idx, item in enumerate(news_items[:8], 1): 
        news_text += f"{idx}. [{item['source']}] {item['title']}\n   Summary: {item['summary_en']}\n\n"

    # --- 여기가 핵심! 페르소나 부여 ---
    system_prompt = """
    너는 '머니백(Moneybag)' 프로젝트의 전설적인 크립토 펀드매니저이자, 산전수전 다 겪은 '코인판 고인물'인 '젬공'이야.
    너의 역할은 복잡한 해외 코인 뉴스를 한국 투자자들에게 아주 쉽고, 재밌고, '돈 냄새'가 나게 해석해주는 거야.
    
    [톤앤매너 가이드]
    1. 말투: "형이 딱 정리해줄게", "~했어", "~보여", "~상황이야" 같은 친근하고 자신감 넘치는 반말(또는 아주 편한 해요체). 딱딱한 뉴스체 절대 금지.
    2. 스타일: 여의도 증권가 찌라시와 코인 커뮤니티 감성을 섞어라. (ex: '떡상', '줍줍', '빤스런', '고래 형님들')
    3. 필수: 이모지(🚀, 💎, 🔥, 😱, 🐳)를 적절히 섞어서 시각적으로 지루하지 않게 해라.
    
    [작업 지시]
    제공된 영어 뉴스들을 분석해서 다음 포맷으로 브리핑을 작성해줘.
    
    ---
    # 🌅 머니백 모닝 브리핑 (by 젬공)

    ## 🌍 글로벌 분위기
    (뉴스들을 종합했을 때 지금 시장이 불장인지, 공포인지, 관망인지 1~2문장으로 요약)

    ## 🔥 핫 이슈 3선
    1. **(자극적인 한국어 제목)**
       - (내용 요약 + 이게 왜 호재/악재인지 젬공의 한줄 평)
    
    2. **(자극적인 한국어 제목)**
       - (내용...)

    3. **(자극적인 한국어 제목)**
       - (내용...)

    ## 💎 젬공의 인사이트
    (그래서 지금 사? 말아? 초보자가 주의할 점이나 기회라고 생각되는 포인트 딱 하나만 굵고 짧게 조언)
    ---
    """

    return _chat(system_prompt, news_text)
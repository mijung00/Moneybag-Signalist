# iceage/src/llm/openai_driver.py
# -*- coding: utf-8 -*-
"""
Signalist Daily용 LLM 헬퍼 모듈

- _chat: 기본 OpenAI API 호출 래퍼
- generate_newsletter_bundle: 뉴스레터 생성용 (제목, 요약, 코멘트 등 일괄 생성)
- generate_social_snippets_from_markdown: SNS 콘텐츠 생성용

OPENAI_API_KEY 는 .env 에 설정되어 있다고 가정.
"""

import os
import json
import logging
import re
from typing import Dict, Any

from dotenv import load_dotenv
from openai import OpenAI

# 1. 설정 및 클라이언트 초기화
logger = logging.getLogger(__name__)
load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key) if api_key else None

MODEL_DEFAULT = "gpt-4o-mini" # 또는 gpt-3.5-turbo
OPENAI_MODEL = MODEL_DEFAULT


# 2. 기본 헬퍼 함수 (_chat)
def _chat(system: str, user: str, temperature: float = 0.4, max_tokens: int = 1600) -> str:
    """
    공통 chat.completions 래퍼.
    시스템/유저 프롬프트를 받아 모델을 호출하고 텍스트 응답을 반환합니다.
    """
    if not client:
        logger.warning("OPENAI_API_KEY not found. _chat returns empty string.")
        return ""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"[LLM Error] _chat failed: {e}")
        return ""


def _strip_json_fence(text: str) -> str:
    """Markdown 코드 블록(```json ... ```)을 제거하는 헬퍼"""
    if not text: return ""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].lower().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return text


# 3. 뉴스레터 번들 생성 (핵심 기능)
def generate_newsletter_bundle(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    시장 데이터를 받아 뉴스레터에 필요한 모든 텍스트(제목, 요약, 코멘트 등)를
    한 번의 LLM 호출로 생성하여 JSON으로 반환합니다.
    """
    if not client:
        return {}

    # [시스템 프롬프트: 페르소나 및 섹션별 가이드라인]
    system_prompt = """
    당신은 'Signalist Daily'의 수석 시장 분석가이자, 독자들에게 '돈이 되는 통찰'을 주는 20년 차 펀드매니저입니다.
    
    [가장 중요한 원칙: 기계적인 숫자 나열 금지]
    - "A종목이 18% 상승하고 거래량이 23배 증가했습니다" 같은 말은 절대 쓰지 마세요. (그건 위의 표에 다 있습니다)
    - 대신 **"왜 올랐는지", "지금 사도 되는지", "세력의 의도가 무엇인지"**를 해석해 주세요.
    - 예시 (나쁜 예): "거래량이 5배 터지며 급등했습니다."
    - 예시 (좋은 예): "바닥권에서 대량 거래가 터지며 강한 손바뀜이 일어났습니다. 추세 전환의 신호탄일 수 있습니다."
    - 예시 (좋은 예): "단기간에 너무 가파르게 올랐습니다. 과열권이니 신규 진입보다는 차익 실현 타이밍을 재는 게 좋습니다."

    [섹션별 가이드]
    1. market_one_liner: 한국 시장 위주로, 상승/하락의 '이유'를 짚어주세요. (예: "반도체 차익매물 출회로...", "저PBR 주도 하에...")
    2. signal_comments:
       - 제공된 'strategy_intent' (전략 의도)를 참고하여 작성하세요.
       - 'Overheat Short' 종목이면 "경고/조심" 뉘앙스로, 'Panic Buying'이면 "기회/반등" 뉘앙스로 쓰세요.
       - 문장 끝을 "~보입니다", "~판단됩니다", "~주목하세요", "~유의하세요" 등으로 다양하게 섞으세요.

    3. **morning_quote (오늘의 한 마디)**:
       - 오늘 시장 분위기나 투자 심리에 딱 맞는 짧은 문장 (30자 내외).

    4. **signal_comments (종목별 코멘트)**:
        [종목별 시그널 코멘트 규칙]
        - 각 종목마다 한국어 1~2문장으로 작성.
        - **절대 '특이 수급 포착' 같은 기계적인 멘트로 때우지 말 것.**
        - 제공된 재료(등락률, 괴리율, 키워드)를 반드시 조합해서 문장을 만들어라.
        [다양한 문장 패턴 예시]
        - (재료+수급): " 'OOO' 관련 이슈가 부각되며 평소보다 5배 넘는 거래대금이 몰렸습니다."
        - (등락+추세): "장대 양봉으로 마감하며 강한 매수세를 증명했습니다. 추세 전환 가능성이 높습니다."
        - (과열 경고): "주가가 급등하며 괴리율이 극에 달했습니다. 단기 차익 실현 물량에 주의하세요."
        - (낙폭 과대): "하락폭이 깊어지며 투매가 나왔으나, 저점 매수세가 유입되는 모습입니다."
        - 문장 끝 어미를 '~보입니다', '~분석됩니다', '~주목하세요', '~판단됩니다' 등으로 다양하게 섞어라.

    5. **global_summary (해외 뉴스)**:
       - headline: 해외 시장 핵심 헤드라인.
       - summary: 2~3문장으로 글로벌 흐름 요약.
       - bullets: 주요 이슈 3가지 불렛 포인트.

    6. **investor_mind (투자 마인드)**:
       - topic: 오늘의 심리/멘탈 관리 주제.
       - body: 차분하고 균형 잡힌 톤으로 조언 (200~300자).
    """

    # [유저 프롬프트: 데이터 + 출력 스키마]
    user_prompt = f"""
    아래 데이터를 바탕으로 뉴스레터 콘텐츠를 생성해줘.

    [시장 데이터]
    {json.dumps(payload, ensure_ascii=False, indent=2)}

    [출력 JSON 형식]
    반드시 아래 JSON 형식을 지켜주세요.
    {{
      "market_one_liner": "시장 요약 문장",
      "title": "메인 제목",
      "kicker": "부제",
      "morning_quote": "오늘의 한 마디",
      "signal_comments": {{
          "종목명1": "코멘트1",
          "종목명2": "코멘트2"
      }},
      "global_summary": {{
          "headline": "해외 뉴스 헤드라인",
          "summary": "해외 시장 요약",
          "bullets": ["이슈1", "이슈2", "이슈3"]
      }},
      "investor_mind": {{
          "topic": "주제",
          "body": "본문"
      }}
    }}
    """

    try:
        # JSON 모드 사용 (안전한 파싱을 위해)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.7
        )
        content = resp.choices[0].message.content
        return json.loads(content)

    except Exception as e:
        logger.error(f"[LLM Error] generate_newsletter_bundle failed: {e}")
        return {}


# iceage/src/llm/openai_driver.py (맨 마지막 함수 수정)

# [openai_driver.py 맨 아래 함수 교체]

# [openai_driver.py 맨 아래 함수 교체]

def generate_social_snippets_from_markdown(newsletter_md: str) -> Dict[str, str]:
    """
    [SNS 전용: 도파민 + 정보 밀도 강화 버전]
    뉴스레터 원문을 바탕으로 '꽉 찬' 카드뉴스 텍스트와 쇼츠 대본을 생성합니다.
    """
    
    system = (
        "너는 100만 구독자를 보유한 주식 인사이트 채널의 메인 작가다. "
        "너의 콘텐츠는 '자극적'이면서도 '알맹이(정보)'가 꽉 차 있어야 한다. "
        "독자가 카드뉴스를 넘길 때마다 새로운 정보를 얻어간다는 느낌을 줘라.\n\n"
        
        "[절대 금지 사항]\n"
        "1. **'몇 배' 표현 금지:** 괴리율(σ)은 표준편차의 배수지, 거래량의 배수가 아니다. 절대 '평소의 20배', '거래량 N배'라고 쓰지 마라. "
        "대신 '통계적 한계 돌파', '비정상적 수급 폭발', '역대급 쏠림' 등으로 표현해라.\n"
        "2. **빈약한 문장 금지:** 한 슬라이드에 딸랑 1~2문장만 쓰지 마라. 구체적인 정황, 시장 분위기, 투자자 심리를 묘사해서 공간을 채워라.\n\n"
        
        "[톤앤매너]\n"
        "- 힙하고 발랄하게: '했음', '실화냐?', '이거 찐이다' 같은 구어체와 이모지를 적극 활용해.\n"
        "- 분석적 깊이: 말투는 가볍지만 내용은 날카롭게. '왜?'에 대한 답을 줘야 해.\n"
    )

    user_prompt = f"""
    [뉴스레터 원문]
    {newsletter_md}

    위 내용을 재료로, 인스타와 유튜브용 '고밀도 콘텐츠'를 작성해줘.
    
    ========================
    [1] 인스타그램 카드뉴스
    - 인스타그램 업로드용 본문 (Caption)과 내용과 연관되고 이슈될만한 해시태그 5개를 뽑아줘: 독자의 클릭을 유도하는 흥미로운 요약글. (이미지 안에 들어갈 텍스트는 만들지 마.)

    ========================
    [2] YouTube Shorts 대본 (꽉 찬 50초)
    - 쉴 새 없이 몰아치는 정보량.
    - (0-5초) [강력한 훅] "오늘 OOO 주주님들, 밤잠 설치시겠는데요? (또는 축하합니다!)"
    - (5-20초) [상황 전달] "오늘 지수는 OOO했는데, 얘 혼자 OO% 등락! 이게 말이 됩니까? 역대급 수급 신호인 OO시그마가 떴습니다." (배수 표현 금지!)
    - (20-40초) [원인 분석] "이유는 딱 하나, OOO 때문입니다. 지금 시장에서는 이걸 '저점 매수 기회(또는 고점 신호)'로 보고 있는데요..."
    - (40-50초) [클로징] "내일 장 시초가가 중요합니다. 남들보다 먼저 대응하고 싶다면? 구독 누르고 알림 설정!"

    ========================
    [3] JSON 출력 형식 (엄수)
    {{
      "instagram": {{
        "slides": [ 
           {{"title": "제목", "text": "본문내용..."}}, 
           ... 
        ],
        "caption": "인스타 본문",
        "hashtags": "#..."
      }},
      "shorts": {{
        "title": "유튜브 제목",
        "script": "대본"
      }}
    }}
    """

    raw = _chat(system, user_prompt, temperature=0.85, max_tokens=3000)
    
    try:
        clean_json = _strip_json_fence(raw)
        data = json.loads(clean_json)
    except Exception as e:
        logger.error(f"[LLM Error] SNS parsing failed: {e}")
        return {}

    insta = data.get("instagram", {})
    shorts = data.get("shorts", {})
    
    # 인스타 슬라이드 텍스트 조립
    slides = insta.get("slides", [])
    slide_text = ""
    for idx, s in enumerate(slides, 1):
        t = s.get("title", "")
        b = s.get("text", "")
        slide_text += f"[슬라이드{idx}] {t}\n{b}\n\n"
        
    return {
        "instagram_caption": f"{slide_text}[캡션]\n{insta.get('caption', '')}",
        "instagram_hashtags": insta.get("hashtags", ""),
        "youtube_title": shorts.get("title", ""),
        "youtube_thumbnail_text": "오늘의 핫이슈",
        "youtube_script": shorts.get("script", ""),
        "youtube_long_script": "" 
    }
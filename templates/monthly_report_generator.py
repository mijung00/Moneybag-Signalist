# iceage/src/pipelines/monthly_report_generator.py
import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
import boto3

# 경로 설정
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from common.s3_manager import S3Manager
from iceage.src.llm.openai_driver import _chat

def generate_monthly_commentary(weekly_summaries: str, ref_date: str) -> dict:
    """LLM을 사용하여 월간 리포트 코멘터리를 생성합니다."""
    if not _chat or not weekly_summaries:
        return {"title": f"Signalist Monthly ({ref_date[:7]})", "summary": "데이터 분석 중..."}

    prompt = f"""
    당신은 'The Signalist'의 수석 애널리스트입니다. 아래는 지난 4주간 발행된 주간 리포트들의 요약본입니다. 
    이 자료들을 종합하여, 지난 한 달간의 시장 동향을 거시적인 관점에서 분석하는 월간 리포트를 작성해주세요.

    **지난 4주간의 리포트 요약:**
    {weekly_summaries}

    **요청 사항 (JSON 형식으로 응답):**
    1.  `title`: "시그널로 돌아본 O월" 과 같이, 한 달을 요약하는 창의적이고 멋진 리포트 제목.
    2.  `summary`: 주간 리포트들을 관통하는 핵심 테마, 시장 심리의 변화, 주요 이벤트의 영향 등을 종합하여 3~4문단의 깊이 있는 'Monthly Analyst's View'를 작성.

    개별 종목 언급보다는 시장 전체의 흐름과 거시적인 관점에 집중해주세요.
    """
    system_prompt = "당신은 전문 금융 데이터 분석가이며, 요청된 JSON 형식에 맞춰 응답합니다."
    
    try:
        response_str = _chat(system_prompt, prompt)
        match = re.search(r'```json\n({.*?})\n```', response_str, re.DOTALL)
        if match:
            response_str = match.group(1)
        return json.loads(response_str)
    except Exception as e:
        print(f"❌ LLM 월간 코멘터리 생성 실패: {e}")
        return {"title": f"Signalist Monthly ({ref_date[:7]})", "summary": "AI 코멘트 생성에 실패했습니다."}

def main():
    """스크립트 메인 실행 함수"""
    ref_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 월간 리포트 생성 시작 (기준일: {ref_date})")

    s3 = S3Manager(bucket_name="fincore-output-storage")
    
    # 지난 4주간의 주간 리포트 내용 수집
    weekly_summaries = []
    for i in range(4):
        d = datetime.strptime(ref_date, "%Y-%m-%d") - timedelta(weeks=i)
        # 해당 주의 금요일을 기준으로 파일명 생성 (토요일에 생성되므로)
        friday_of_week = d - timedelta(days=d.weekday()) + timedelta(days=4)
        weekly_key = f"iceage/out/weekly/Signalist_Weekly_{friday_of_week.strftime('%Y-%m-%d')}.md"
        content = s3.get_text_content(weekly_key)
        if content:
            weekly_summaries.append(f"--- {friday_of_week.strftime('%Y-%m-%d')} 주차 ---\n{content[:1000]}...")

    llm_content = generate_monthly_commentary("\n\n".join(weekly_summaries), ref_date)
    
    title = llm_content.get("title", f"Signalist Monthly ({ref_date[:7]})")
    summary = llm_content.get("summary", "데이터가 부족하여 월간 분석을 생성할 수 없습니다.")
    
    md_content = f"# {title}\n\n{summary}"
    
    out_key = f"iceage/out/monthly/Signalist_Monthly_{ref_date[:7]}.md"
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket="fincore-output-storage", Key=out_key, Body=md_content.encode('utf-8'), ContentType="text/markdown; charset=utf-8")
    print(f"✅ 월간 리포트 생성 및 S3 업로드 완료: {out_key}")

if __name__ == "__main__":
    main()
# iceage/src/pipelines/weekly_report_generator.py
import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
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

def analyze_weekly_signals(df: pd.DataFrame, end_date: datetime) -> dict:
    """지난 7일간의 시그널을 분석합니다."""
    start_date = end_date - timedelta(days=7)
    df['signal_date'] = pd.to_datetime(df['signal_date'])
    
    weekly_df = df[(df['signal_date'] >= start_date) & (df['signal_date'] <= end_date)]
    
    if weekly_df.empty:
        return {"error": "지난 7일간의 데이터가 없습니다."}

    # 1. 가장 자주 포착된 종목 (Top 5)
    top_stocks = weekly_df['name'].value_counts().nlargest(5)
    
    # 2. 매수/매도 시그널 비율
    sentiment_counts = weekly_df['sentiment'].value_counts(normalize=True) * 100
    
    return {
        "top_stocks": top_stocks.to_dict(),
        "sentiment_ratio": sentiment_counts.to_dict()
    }

def generate_llm_commentary(analysis: dict, ref_date: str) -> dict:
    """LLM을 사용하여 주간 리포트 코멘터리를 생성합니다."""
    if not _chat or "error" in analysis:
        return {"title": f"Signalist Weekly ({ref_date})", "summary": "데이터 분석 중...", "stock_comments": {}}

    top_stocks_str = "\n".join([f"- {name}: {count}회" for name, count in analysis.get("top_stocks", {}).items()])
    sentiment_str = "\n".join([f"- {sent}: {ratio:.1f}%" for sent, ratio in analysis.get("sentiment_ratio", {}).items()])

    prompt = f"""
    당신은 'The Signalist'의 수석 애널리스트입니다. 아래 주간 시그널 분석 데이터를 바탕으로, 전문적이면서도 흥미로운 주간 리포트를 작성해주세요.

    **분석 데이터 (기간: 지난 7일):**
    - 가장 자주 포착된 종목 TOP 5:
    {top_stocks_str}
    - 매수/매도 시그널 비율:
    {sentiment_str}

    **요청 사항 (JSON 형식으로 응답):**
    1.  `title`: "시그널로 돌아본 한 주" 와 같이, 한 주를 요약하는 창의적이고 멋진 리포트 제목.
    2.  `summary`: 위 데이터를 종합하여 지난 한 주간의 시장 특징을 2~3문단으로 분석하는 'Analyst's View'. (예: 특정 테마의 반복적인 등장, 시장 심리의 변화 등)
    3.  `stock_comments`: 가장 자주 포착된 각 종목({', '.join(analysis.get("top_stocks", {}).keys())})에 대해, 왜 자주 포착되었을지 추측하며 1~2줄의 짧은 코멘트. (key: 종목명, value: 코멘트)

    투자 추천은 절대 금지입니다. 데이터 기반의 관찰과 분석에 집중해주세요.
    """
    
    system_prompt = "당신은 전문 금융 데이터 분석가이며, 요청된 JSON 형식에 맞춰 응답합니다."
    
    try:
        response_str = _chat(system_prompt, prompt)
        match = re.search(r'```json\n({.*?})\n```', response_str, re.DOTALL)
        if match:
            response_str = match.group(1)
        return json.loads(response_str)
    except Exception as e:
        print(f"❌ LLM 코멘터리 생성 실패: {e}")
        return {"title": f"Signalist Weekly ({ref_date})", "summary": "AI 코멘트 생성에 실패했습니다.", "stock_comments": {}}

def create_weekly_report_md(ref_date: str, s3: S3Manager) -> str:
    """주간 리포트 마크다운을 생성합니다."""
    log_key = "iceage/data/processed/signalist_today_log.csv"
    try:
        csv_content = s3.get_text_content(log_key)
        if not csv_content: raise FileNotFoundError("Log file is empty.")
        df = pd.read_csv(StringIO(csv_content))
    except Exception as e:
        return f"# 에러\n\n시그널 로그 파일({log_key})을 S3에서 불러오는 데 실패했습니다: {e}"

    analysis = analyze_weekly_signals(df, datetime.strptime(ref_date, "%Y-%m-%d"))
    llm_content = generate_llm_commentary(analysis, ref_date)

    title = llm_content.get("title", f"Signalist Weekly ({ref_date})")
    summary = llm_content.get("summary", "")
    stock_comments = llm_content.get("stock_comments", {})

    lines = [f"# {title}", f"_{ref_date} 기준 지난 7일간의 기록_"]
    if summary: lines.extend(["\n## 🔍 Analyst's View", summary])

    if "top_stocks" in analysis:
        lines.append("\n## 📡 이번 주 레이더에 가장 많이 잡힌 종목")
        lines.append("| 순위 | 종목명 | 포착 횟수 | AI 코멘트 |")
        lines.append("|:---:|:---|:---:|:---|")
        for i, (name, count) in enumerate(analysis["top_stocks"].items(), 1):
            comment = stock_comments.get(name, "특이 수급 반복 포착.")
            lines.append(f"| {i} | **{name}** | {count}회 | {comment} |")

    lines.append("\n---\n_본 리포트는 과거 데이터의 통계이며, 미래 수익을 보장하지 않습니다._")
    return "\n".join(lines)

def main():
    """스크립트 메인 실행 함수"""
    ref_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 주간 리포트 생성 시작 (기준일: {ref_date})")

    s3 = S3Manager(bucket_name="fincore-output-storage")
    md_content = create_weekly_report_md(ref_date, s3)
    
    out_key = f"iceage/out/weekly/Signalist_Weekly_{ref_date}.md"
    
    # S3Manager에 put_text_content가 없으므로 boto3 직접 사용
    s3_client = boto3.client("s3", region_name="ap-northeast-2")
    s3_client.put_object(
        Bucket="fincore-output-storage",
        Key=out_key,
        Body=md_content.encode('utf-8'),
        ContentType="text/markdown; charset=utf-8"
    )
    print(f"✅ 주간 리포트 생성 및 S3 업로드 완료: {out_key}")

if __name__ == "__main__":
    main()
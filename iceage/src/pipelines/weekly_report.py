import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import re

# 경로 설정 (iceage 프로젝트에 맞게 조정)
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BASE_DIR))

from common.env_loader import load_env
load_env(BASE_DIR)

# LLM 드라이버 및 공통 모듈 임포트 (moneybag 프로젝트와 공유한다고 가정)
try:
    from moneybag.src.llm.openai_driver import _chat
except ImportError:
    print("⚠️ [LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다.")
    _chat = None

from moneybag.src.pipelines.send_email import EmailSender
from moneybag.src.utils.slack_notifier import SlackNotifier

class WeeklyReport:
    def __init__(self):
        # iceage 데일리 리포트가 저장되는 경로
        self.daily_report_dir = BASE_DIR / "iceage" / "data" / "out"
        self.output_dir = self.daily_report_dir # 같은 곳에 저장
        self.service_name = "시그널리스트"

    def find_daily_reports(self, start_date, end_date):
        """지정한 기간의 데일리 리포트 파일들을 찾습니다."""
        print(f"🔍 {start_date.date()} ~ {end_date.date()} 기간의 데일리 리포트를 검색합니다.")
        found_files = []
        delta_days = (end_date - start_date).days
        for i in range(delta_days + 1):
            current_date = start_date + timedelta(days=i)
            # 데일리 리포트 파일명 형식(Signalist_Briefing_YYYY.MM.DD.md)을 가정합니다.
            filename = f"Signalist_Briefing_{current_date.strftime('%Y.%m.%d')}.md"
            filepath = self.daily_report_dir / filename
            if filepath.exists():
                print(f"  - 발견: {filename}")
                found_files.append(filepath)
        return found_files

    def generate_report(self):
        """주간 리포트를 생성하고 파일로 저장합니다."""
        if not _chat:
            raise ConnectionError("LLM 드라이버가 로드되지 않았습니다.")

        # 1. 날짜 범위 설정 (크론이 토요일에 실행되므로, 지난 주 월~금을 대상으로 함)
        today = datetime.now(ZoneInfo("Asia/Seoul"))
        end_date = today - timedelta(days=1)   # 금요일
        start_date = end_date - timedelta(days=4) # 월요일
        date_range_str = f"{start_date.strftime('%Y.%m.%d')} ~ {end_date.strftime('%Y.%m.%d')}"
        
        # 2. 해당 기간의 데일리 리포트 파일 찾기 및 내용 취합
        daily_files = self.find_daily_reports(start_date, end_date)
        if not daily_files:
            print("❌ 요약할 데일리 리포트가 없습니다. 주간 리포트 생성을 중단합니다.")
            return None

        full_summary = ""
        for f_path in daily_files:
            date_str = f_path.stem.split('_')[-1]
            full_summary += f"\n\n--- [ {date_str} 브리핑 내용 ] ---\n"
            with open(f_path, 'r', encoding='utf-8') as f:
                full_summary += f.read()

        # 3. LLM을 이용해 주간 리포트 초안 생성
        print("🧠 LLM이 주간 리포트를 작성 중입니다...")
        system_prompt = f"""
        당신은 "{self.service_name}"의 수석 애널리스트입니다. 지난 한 주간의 데일리 브리핑 내용을 종합하여, 인사이트가 담긴 주간 리포트를 작성하는 임무를 받았습니다. 단순 요약이 아닌, 한 주간의 시장 흐름을 관통하는 스토리를 만들어내야 합니다.

        [작성 지침]
        1. **헤드라인**: 한 주간의 시장을 가장 잘 표현하는 매력적인 제목을 만드세요.
        2. **주간 시장 요약 (Executive Summary)**: 지난 주 시장(KOSPI, KOSDAQ)의 주요 움직임과 핵심 이벤트를 3~4 문장으로 요약하세요.
        3. **금주의 핵심 테마 (Key Themes of the Week)**: 데일리 리포트에서 반복적으로 언급된 주요 테마(예: AI, 반도체, 2차전지, 정부 정책 등)를 2~3개 선정하고, 각 테마가 시장에 어떤 영향을 미쳤는지 설명하세요.
        4. **주간 전략 평가 및 복기 (Strategy Review)**: 데일리 리포트의 '최종 결론'들을 바탕으로, 지난 한 주간 제시했던 전략들이 어땠는지 평가하세요. 성공적인 예측이나 아쉬웠던 점을 솔직하게 복기하며 신뢰를 주세요.
        5. **다음 주 전망 및 전략 (Outlook for Next Week)**: 분석한 내용을 바탕으로 다음 주 시장을 어떻게 전망하는지, 그리고 투자자들이 어떤 점에 주목해야 할지 구체적인 전략을 제시하세요.

        [출력 양식]
        # 💎 [주간 리포트] (여기에 헤드라인 작성)

        **기간:** {date_range_str}

        ## 1. 주간 시장 요약 (Executive Summary)
        (내용)

        ## 2. 금주의 핵심 테마 (Key Themes of the Week)
        ### 테마 1: (예: AI 반도체의 귀환)
        (설명)
        ### 테마 2: (예: 정책 수혜주 강세)
        (설명)

        ## 3. 주간 전략 평가 및 복기 (Strategy Review)
        (내용)

        ## 4. 다음 주 전망 및 전략 (Outlook for Next Week)
        (내용)
        """
        user_prompt = f"아래는 지난 한 주간의 데일리 브리핑 내용입니다. 이 내용을 바탕으로 주간 리포트를 작성해주세요.\n\n{full_summary}"
        report_content = _chat(system_prompt, user_prompt)

        # 5. 파일로 저장
        output_filename = f"Weekly_Report_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.md"
        output_filepath = self.output_dir / output_filename
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"✅ [저장 완료] 주간 리포트가 '{output_filepath}'에 저장되었습니다.")
        return str(output_filepath)

def run_weekly_routine():
    """주간 리포트 생성 및 발송 전체 프로세스"""
    print(f"\n🏃 [Weekly Runner] 주간 리포트 루틴을 시작합니다...")
    notifier = SlackNotifier()
    try:
        reporter = WeeklyReport()
        if report_path := reporter.generate_report():
            print("\n📧 이메일 발송 중...")
            EmailSender().send(report_path, mode="weekly")
            notifier.send_message("✅ [Iceage] 주간 리포트 발송 완료!")
    except Exception as e:
        error_msg = f"🚨 [Iceage 긴급] 주간 리포트 생성/발송 실패!\n에러: {e}"
        print(error_msg)
        notifier.send_message(error_msg)
    print(f"\n🏃 [Weekly Runner] 주간 리포트 루틴을 종료합니다.")

def main(*args, **kwargs):
    """runner.py에서 호출하기 위한 표준 진입점."""
    run_weekly_routine()

if __name__ == "__main__":
    run_weekly_routine()
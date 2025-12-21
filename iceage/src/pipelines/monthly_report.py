import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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

class MonthlyReport:
    def __init__(self):
        self.report_dir = BASE_DIR / "iceage" / "data" / "out"
        self.service_name = "시그널리스트"

    def find_weekly_reports(self, year, month):
        """지정한 월의 주간 리포트 파일들을 찾습니다."""
        print(f"🔍 {year}년 {month}월의 주간 리포트를 검색합니다.")
        found_files = []
        # 파일명 형식: Weekly_Report_YYYYMMDD-YYYYMMDD.md
        for file in self.report_dir.glob("Weekly_Report_*.md"):
            try:
                date_part = file.stem.split('_')[-1]
                start_date_str = date_part.split('-')[0]
                report_date = datetime.strptime(start_date_str, '%Y%m%d')
                if report_date.year == year and report_date.month == month:
                    print(f"  - 발견: {file.name}")
                    found_files.append(file)
            except (IndexError, ValueError):
                continue
        return sorted(found_files)

    def generate_report(self):
        """월간 리포트를 생성하고 파일로 저장합니다."""
        if not _chat:
            raise ConnectionError("LLM 드라이버가 로드되지 않았습니다.")

        # 1. 날짜 범위 설정 (크론이 매월 1일에 실행되므로, '지난 달'을 기준으로 함)
        today = datetime.now(ZoneInfo("Asia/Seoul"))
        last_day_of_last_month = today.replace(day=1) - timedelta(days=1)
        last_month = last_day_of_last_month.month
        last_month_year = last_day_of_last_month.year
        date_range_str = f"{last_month_year}년 {last_month}월"
        
        # 2. 해당 월의 주간 리포트 파일 찾기 및 내용 취합
        weekly_files = self.find_weekly_reports(last_month_year, last_month)
        if not weekly_files:
            print("❌ 요약할 주간 리포트가 없습니다. 월간 리포트 생성을 중단합니다.")
            return None

        full_summary = ""
        for i, f_path in enumerate(weekly_files, 1):
            full_summary += f"\n\n--- [ {last_month}월 {i}주차 리포트 내용 ] ---\n"
            with open(f_path, 'r', encoding='utf-8') as f:
                full_summary += f.read()

        # 3. LLM을 이용해 월간 리포트 초안 생성
        print("🧠 LLM이 월간 리포트를 작성 중입니다...")
        system_prompt = f"""
        당신은 "{self.service_name}"의 최고 투자 전략가(Chief Investment Officer)입니다. 지난 한 달간 발행된 주간 리포트들을 바탕으로, 거시적인 관점의 월간 투자 전략 리포트를 작성하는 임무를 받았습니다. 단순 요약을 넘어, 한 달간의 시장 동향을 종합하고 다음 달을 위한 장기적인 투자 방향을 제시해야 합니다.

        [작성 지침]
        1. **헤드라인**: 지난 한 달의 시장을 정의하고 다음 달의 기대를 암시하는 강력한 제목을 만드세요.
        2. **월간 시장 리뷰 (Monthly Market Review)**: 지난 한 달간의 주요 지수(KOSPI, KOSDAQ) 변화, 주요 경제 지표(금리, 환율 등)를 종합하여 시장을 리뷰하세요.
        3. **월간 핵심 동인 분석 (Key Drivers of the Month)**: 주간 리포트들의 '핵심 테마'를 종합하여, 월 전체를 관통한 가장 중요한 시장 동인(Market Driver)이 무엇이었는지 분석하세요.
        4. **월간 전략 성과 리뷰 (Monthly Performance Review)**: 주간 리포트에서 제시된 전략들의 월간 성과를 종합적으로 평가하고, 성공/실패 요인을 분석하여 다음 전략의 기반으로 삼으세요.
        5. **장기 전망 및 투자 테제 (Long-term Outlook & Thesis)**: 분석한 내용을 바탕으로, 다음 분기까지 이어질 수 있는 장기적인 시장 전망과 투자 테제(Thesis)를 제시하세요. 어떤 섹터에 주목해야 하는지, 어떤 리스크를 관리해야 하는지 명확히 하세요.

        [출력 양식]
        # 🏆 [월간 리포트] (여기에 헤드라인 작성)

        **리포트 기간:** {date_range_str}

        ## 1. 월간 시장 리뷰 (Monthly Market Review)
        (내용)

        ## 2. 월간 핵심 동인 분석 (Key Drivers of the Month)
        (내용)

        ## 3. 월간 전략 성과 리뷰 (Monthly Performance Review)
        (내용)

        ## 4. 장기 전망 및 투자 테제 (Long-term Outlook & Thesis)
        (내용)

        ---
        <div style="text-align: center; font-size: 12px; color: #888888; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eeeeee;">
        본 메일은 -email- 주소로 발송된 Fincore 뉴스레터입니다.<br>
        더 이상 수신을 원하지 않으시면 <a href="-unsubscribe_url-" style="color: #555555; text-decoration: underline;">여기</a>를 눌러 구독을 취소해주세요.<br><br>
        (주)비제이유앤아이 | <a href="https://www.fincore.trade/privacy" style="color: #555555;">개인정보 처리방침</a>
        </div>
        """
        user_prompt = f"아래는 지난 한 달간의 주간 리포트 모음입니다. 이 내용을 바탕으로 월간 리포트를 작성해주세요.\n\n{full_summary}"
        report_content = _chat(system_prompt, user_prompt)

        # 4. 파일로 저장
        output_filename = f"Monthly_Report_{last_month_year}{last_month:02d}.md"
        output_filepath = self.report_dir / output_filename
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"✅ [저장 완료] 월간 리포트가 '{output_filepath}'에 저장되었습니다.")
        return str(output_filepath)

def run_monthly_routine():
    """월간 리포트 생성 및 발송 전체 프로세스"""
    print(f"\n🏃 [Monthly Runner] 월간 리포트 루틴을 시작합니다...")
    notifier = SlackNotifier()
    try:
        reporter = MonthlyReport()
        if report_path := reporter.generate_report():
            print("\n📧 이메일 발송 중...")
            EmailSender().send(report_path, mode="monthly")
            notifier.send_message("✅ [Iceage] 월간 리포트 발송 완료!")
    except Exception as e:
        error_msg = f"🚨 [Iceage 긴급] 월간 리포트 생성/발송 실패!\n에러: {e}"
        print(error_msg)
        notifier.send_message(error_msg)
    print(f"\n🏃 [Monthly Runner] 월간 리포트 루틴을 종료합니다.")

if __name__ == "__main__":
    run_monthly_routine()
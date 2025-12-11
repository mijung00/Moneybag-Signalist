# iceage/src/tools/run_newsletter_only.py
import sys
import os
from pathlib import Path

# 프로젝트 루트 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.pipelines.morning_newsletter import main as newsletter_main

def run_newsletter_only():
    # 원하는 날짜 지정 (오늘 날짜 또는 테스트하고 싶은 날짜)
    target_date = "2025-12-05" 
    
    print(f"📰 [Signalist] 뉴스레터 재생성 모듈 가동 ({target_date})")
    print("   - 기존 데이터(Price, Log)를 바탕으로 마크다운만 다시 만듭니다.")
    
    # 뉴스레터 메인 함수 호출
    # sys.argv를 조작하여 인자 전달
    sys.argv = ["morning_newsletter.py", target_date]
    newsletter_main()
    
    print(f"\n✅ 재생성 완료! data/reports/Signalist_Daily_{target_date}.md 확인 바람.")

if __name__ == "__main__":
    run_newsletter_only()
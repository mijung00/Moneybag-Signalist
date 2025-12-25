from datetime import date, timedelta
import logging

# application.py에 정의된 통합 설정 로더를 가져옵니다.
from common.config import config

# 기존 모듈 임포트 (경로가 실제와 다를 경우 수정이 필요할 수 있습니다)
import iceage.src.pipelines.daily_runner as iceage_runner
import moneybag.src.pipelines.daily_runner as moneybag_runner
import iceage.src.pipelines.weekly_report as iceage_weekly_runner
import iceage.src.pipelines.monthly_report as iceage_monthly_runner
from iceage.src.collectors import krx_listing_collector, krx_index_collector, krx_daily_price_collector

def run_iceage_task(arg=None):
    """run_iceage.sh를 대체하는 파이썬 함수"""
    # 이 함수가 실행되기 전에 필요한 모든 시크릿을 로드합니다.
    for k in ["KRX_AUTH_KEY", "SERPAPI_KEY", "OPENAI_API_KEY", "DB_PASSWORD", "SENDGRID_API_KEY"]:
        config.ensure_secret(k)
    
    # 셸 스크립트 대신, 직접 파이썬 모듈의 main 함수를 호출합니다.
    logging.info(f"Starting IceAge task with arg: {arg}")
    return iceage_runner.main(arg) if hasattr(iceage_runner, 'main') else "Module Error: iceage_runner.main not found"

def run_moneybag_task(mode="morning"):
    """run_moneybag.sh를 대체하는 파이썬 함수"""
    for k in ["OPENAI_API_KEY", "SENDGRID_API_KEY", "TELEGRAM_BOT_TOKEN_MONEYBAG", "DB_PASSWORD"]:
        config.ensure_secret(k)

    logging.info(f"Starting Moneybag task with mode: {mode}")
    return moneybag_runner.main(mode) if hasattr(moneybag_runner, 'main') else "Module Error: moneybag_runner.main not found"

def run_krx_batch_task(days=3):
    """run_krx_batch.sh의 3일치 데이터 수집 로직을 대체하는 파이썬 함수"""
    config.ensure_secret("KRX_AUTH_KEY")
    config.ensure_secret("DB_PASSWORD") # DB 접속에 필요
    
    today = date.today()
    results = []
    
    logging.info(f"Starting KRX Batch task for {days} days...")
    for i in range(days):
        target_date_str = (today - timedelta(days=i)).strftime("%Y%m%d")
        logging.info(f"  -> Processing KRX data for {target_date_str}")
        
        # 셸 스크립트처럼 각 콜렉터를 순차적으로 직접 호출합니다.
        krx_listing_collector.main(target_date_str)
        krx_index_collector.main(target_date_str)
        krx_daily_price_collector.main(target_date_str)
        results.append(target_date_str)
    
    return f"KRX Batch Completed for dates: {', '.join(results)}"

def run_iceage_weekly_task():
    """run_iceage_weekly.sh를 대체하는 파이썬 함수"""
    for k in ["DB_PASSWORD", "SENDGRID_API_KEY"]: # 주간 리포트에 필요한 시크릿
        config.ensure_secret(k)
    
    logging.info("Starting IceAge Weekly Report task...")
    return iceage_weekly_runner.main() if hasattr(iceage_weekly_runner, 'main') else "Module Error: iceage_weekly_runner.main not found"

def run_iceage_monthly_task():
    """run_iceage_monthly.sh를 대체하는 파이썬 함수"""
    for k in ["DB_PASSWORD", "SENDGRID_API_KEY"]: # 월간 리포트에 필요한 시크릿
        config.ensure_secret(k)

    logging.info("Starting IceAge Monthly Report task...")
    return iceage_monthly_runner.main() if hasattr(iceage_monthly_runner, 'main') else "Module Error: iceage_monthly_runner.main not found"
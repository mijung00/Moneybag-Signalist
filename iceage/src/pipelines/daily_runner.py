# iceage/src/pipelines/daily_runner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import subprocess
import sys
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from zoneinfo import ZoneInfo
now = datetime.now(ZoneInfo("Asia/Seoul"))


from iceage.src.utils.trading_days import (
    TradingCalendar,
    CalendarConfig,
    compute_reference_date,
    may_run_today,
)

from common.s3_manager import S3Manager  # <--- 이거 추가!

# ---- 데이터 경로 & 과거 데이터 체크용 헬퍼 ----
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../iceage
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW = DATA_DIR / "raw"
DATA_PROCESSED = DATA_DIR / "processed"
DATA_REF = DATA_DIR / "reference"


REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from common.env_loader import load_env
load_env(REPO_ROOT)

# 에러 메시지 모아두는 전역 리스트
ERRORS: list[str] = []


def _has_core_kr_data(ref_str: str) -> bool:
    """
    ref_date 기준으로 이미 수집된 '핵심 데이터 파일들'이 모두 존재하는지 확인.
    - 존재하면 과거 날짜 재실행 시 수집 단계를 스킵해도 안전하다고 판단.
    """
    required_paths = [
        DATA_REF / f"kr_listing_{ref_str}.csv",
        DATA_RAW / f"kr_prices_{ref_str}.csv",
        DATA_RAW / f"kr_news_{ref_str}.jsonl",
        DATA_RAW / f"naver_themes_{ref_str}.csv",
        DATA_PROCESSED / f"kr_news_cleaned_{ref_str}.jsonl",
        DATA_PROCESSED / f"global_news_{ref_str}.jsonl",
        DATA_PROCESSED / f"kr_sector_themes_{ref_str}.json",
    ]
    return all(p.exists() for p in required_paths)


def _run(cmd: list[str]) -> None:
    """
    [개선] 하위 모듈을 서브프로세스로 실행하고, 오류 발생 시 상세 내용을 출력합니다.
    - 항상 현재 인터프리터(sys.executable)를 사용해서 venv / 패키지 환경이 그대로 유지되도록 합니다.
    - check_call 대신 Popen을 사용하여 stdout/stderr를 캡처하고, 오류 발생 시 상세히 로깅합니다.
    """
    if cmd and cmd[0] == "python":
        cmd = [sys.executable] + cmd[1:]

    # [추가] Windows 인코딩 문제를 해결하기 위해 모든 자식 프로세스를 UTF-8 모드로 실행
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"

    print(f"\n$ {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        env=env
    )
    stdout, stderr = process.communicate()

    # 성공한 경우에도 표준 출력을 보여주어 진행 상황 파악 용이
    print(stdout)

    if process.returncode != 0:
        print("--- SUBPROCESS STDERR ---")
        print(stderr)
        print("-------------------------")
        raise subprocess.CalledProcessError(process.returncode, cmd, output=stdout, stderr=stderr)


def run_step(name: str, cmd: list[str], critical: bool = False) -> None:
    """
    공통 스텝 실행 헬퍼.
    - 성공: 로그만 찍고 통과
    - 실패: ERRORS에 기록하고, critical=False 이면 계속 진행
    """
    global ERRORS

    print(f"\n[STEP] {name}")
    try:
        _run(cmd)
        print(f"[OK] {name}")
    except Exception as e:
        msg = f"[ERROR] {name} 실패: {e}"
        print(msg)
        ERRORS.append(msg)
        if critical:
            # 치명적인 스텝이면 전체 파이프라인 중단
            raise


def main() -> None:
    """
    일일 파이프라인 실행 엔트리포인트.
    """
    global ERRORS
    ERRORS = []

    cal = TradingCalendar(CalendarConfig())
    
    # ---------------------------------------------------------
    # [젬공의 책략 1] 실행 가능 여부 먼저 판단
    # ---------------------------------------------------------
    # [디버깅] 실제 환경변수 값을 출력해서 확인 (로그에서 확인 가능)
    raw_env_val = os.environ.get("ALLOW_RUN_NON_BUSINESS", "0")
    print(f"[DEBUG] 현재 ALLOW_RUN_NON_BUSINESS 값: '{raw_env_val}'")
    
    allow_non_business = raw_env_val.strip() == "1"

    # 인자 없이 실행된 경우(=자동 스케줄러), 오늘이 영업일인지 먼저 체크
    if len(sys.argv) < 2:
        if not may_run_today(cal, now):
            if allow_non_business:
                print("[WARN] 비영업일이지만 ALLOW_RUN_NON_BUSINESS=1 로 강제 실행합니다.")
            else:
                print(f"[INFO] 오늘은 영업일/실행시간이 아니므로 종료합니다. (Time: {now})")
                sys.exit(0)  # 여기서 깔끔하게 종료!

    # ---------------------------------------------------------
    # [젬공의 책략 2] 기준일 설정
    # ---------------------------------------------------------
    if len(sys.argv) >= 2:
        arg1 = sys.argv[1]
        # 'newsletter' 인자는 무시하고 기본 날짜 계산 로직을 따르도록 수정
        if arg1 == 'newsletter':
            ref = compute_reference_date(cal, now)
        else:
            ref = date.fromisoformat(arg1)  # YYYY-MM-DD (수동 지정)
    else:
        # 오늘 실행하면 "전 영업일"을 기준일로 사용
        ref = compute_reference_date(cal, now)

    ref_str = ref.isoformat()
    print(f"\n📅 기준일(ref_date): {ref_str}")

    # ====================================================
    # [수정] S3에서 필수 과거 데이터 불러오기 (출근 준비)
    # ====================================================
    s3 = S3Manager()
    
    print("\n📥 [S3 Sync] 필수 과거 데이터 다운로드 중...")

    # 1. 누적 로그 파일
    # [수정] 로컬 경로는 'iceage/'를 빼야 함 (PROJECT_ROOT가 이미 iceage 폴더임)
    log_file_local = "data/processed/signalist_today_log.csv" 
    
    log_file_s3 = "iceage/data/processed/signalist_today_log.csv"
    
    # 로컬 폴더 생성 (data/processed)
    (PROJECT_ROOT / "data/processed").mkdir(parents=True, exist_ok=True)
    
    full_log_path = PROJECT_ROOT / log_file_local
    s3.download_file(log_file_s3, str(full_log_path)) # 실패해도 괜찮음

    # 2. 괴리율 분석(volume_anomaly)을 위한 과거 시세 데이터 (최근 60일치)
    local_raw_dir = PROJECT_ROOT / "data/raw"
    local_raw_dir.mkdir(parents=True, exist_ok=True)
    s3_raw_dir = f"s3://{s3.bucket_name}/iceage/data/raw/"

    # [개선] 전체 동기화 대신, 필요한 최근 60일치 파일만 특정하여 동기화
    LOOKBACK_DAYS = 60 # volume_anomaly_v2.py에서 사용하는 window_days
    print(f"   👉 과거 {LOOKBACK_DAYS}일치 시세 데이터 동기화 (aws s3 sync --include)...")
    try:
        sync_cmd = ["aws", "s3", "sync", s3_raw_dir, str(local_raw_dir), "--exclude", "*", "--quiet"]
        
        # 필요한 파일 목록을 --include 옵션으로 추가
        for i in range(LOOKBACK_DAYS + 1): # ref_date 당일 포함 ~ 60일 전
            past_date = ref - timedelta(days=i)
            filename = f"kr_prices_{past_date.isoformat()}.csv"
            sync_cmd.extend(["--include", filename])

        subprocess.run(sync_cmd, check=True, timeout=300) # 5분 타임아웃
    except Exception as e:
        print(f"⚠️ [S3 Sync] 'aws s3 sync' 실패. runner 환경에 aws-cli가 필요합니다. 에러: {e}")

    print(f"✅ [S3 Sync] 완료")
    # ====================================================

    freeze_hist = os.getenv("FREEZE_HISTORICAL_KR", "1") == "1"
    enable_investor_flow = os.getenv("ENABLE_INVESTOR_FLOW", "0") == "1"
    run_social_output = os.getenv("RUN_SOCIAL_OUTPUT", "1") == "1"
    run_cardnews_output = os.getenv("RUN_CARDNEWS_OUTPUT", "1") == "1"
    run_tts_output = os.getenv("RUN_TTS_OUTPUT", "1") == "1"
    run_video_output = os.getenv("RUN_VIDEO_OUTPUT", "1") == "1"
    run_summary_image_output = os.getenv("RUN_SUMMARY_IMAGE_OUTPUT", "1") == "1"
    
    # 과거 ref_date 에 대한 "수집 스킵" 여부 결정
    skip_collection = False
    if freeze_hist and ref < date.today() and _has_core_kr_data(ref_str):
        skip_collection = True
        print(
            "[INFO] 과거 ref_date이고, 핵심 데이터 파일이 이미 존재합니다.\n"
            "       → 1)~5) 수집/정제 단계는 건너뛰고, "
            "뉴스레터/HTML/SNS/메일만 실행합니다."
        )

    # -----------------------
    # 1)~5) 데이터 수집/정제
    # -----------------------
    if not skip_collection:
        # 1) 상장법인 목록 수집 (KRX OPEN API 사용)
        try:
            run_step(
                "상장법인 목록 수집 (KRX API)",
                [
                    "python",
                    "-m",
                    "iceage.src.collectors.krx_listing_collector",
                    ref_str,
                ],
                critical=True,  # 실패 시 예외를 발생시켜 폴백 로직으로 넘어가도록 함
            )
        except Exception as e:
            print(f"[WARN] KRX 상장 목록 수집 실패, 전일 데이터 복사로 폴백합니다: {e}")
            try:
                print("\n[STEP] 상장법인 목록 수집 (폴백: 전일 데이터 복사)")
                listing_files = sorted(DATA_REF.glob("kr_listing_*.csv"), reverse=True)
                if not listing_files:
                    raise FileNotFoundError("폴백할 과거 상장 목록 파일이 없습니다.")
                
                latest_listing_file = listing_files[0]
                fallback_target_path = DATA_REF / f"kr_listing_{ref_str}.csv"
                shutil.copy2(latest_listing_file, fallback_target_path)
                print(f"[OK] 상장법인 목록 수집 (폴백: 전일 데이터 복사)")
                print(f"   - 복사 완료: {latest_listing_file.name} -> {fallback_target_path.name}")
            except Exception as fallback_e:
                msg = f"[ERROR] 상장 목록 폴백 실패: {fallback_e}"
                print(msg)
                ERRORS.append(msg)
                raise  # 폴백마저 실패하면 파이프라인 중단

        # 1-1) KRX 지수(코스피/코스닥) 수집
        run_step(
            "KRX 시장 지수 수집",
            ["python", "-m", "iceage.src.collectors.krx_index_collector", ref_str],
        )

        # 2) 일별 시세 수집 (KRX -> 네이버 폴백)
        try:
            _run(
                [
                    "python",
                    "-m",
                    "iceage.src.collectors.krx_daily_price_collector",
                    ref_str,
                ]
            )
        except Exception as e:
            print(f"[WARN] KRX 일별 시세 수집 실패, 네이버 시세로 폴백합니다: {e}")
            _run(
                [
                    "python",
                    "-m",
                    "iceage.src.collectors.kr_stock_price_collector",
                    ref_str,
                ]
            )

        # 2-1) 괴리율 v2 분석
        try:
            _run(
                [
                    "python",
                    "-m",
                    "iceage.src.analyzers.volume_anomaly_v2",
                    ref_str,
                ]
            )
        except Exception as e:
            print(f"[WARN] volume_anomaly_v2 실패: {e}")

        # 2-2) 투자자별 매매 동향 수집 (옵션)
        if enable_investor_flow:
            run_step(
                "투자자별 매매 동향 수집",
                [
                    "python",
                    "-m",
                    "iceage.src.collectors.kr_investor_flow_collector",
                    ref_str,
                ],
            )
        
        # 3) 국내/해외 뉴스 수집 + 클린
        run_step(
            "국내 시장 뉴스 수집",
            ["python", "-m", "iceage.src.collectors.kr_news_serpapi", ref_str],
        )

        run_step(
            "종목 이벤트 뉴스 수집",
            ["python", "-m", "iceage.src.collectors.kr_stock_event_serpapi", ref_str],
        )

        run_step(
            "국내 뉴스 클렌징",
            ["python", "-m", "iceage.src.processors.kr_news_cleaner", ref_str],
        )

        run_step(
            "해외 뉴스 수집",
            ["python", "-m", "iceage.src.collectors.global_news_serpapi", ref_str],
        )

        # 4) 네이버 테마 맵 업데이트
        run_step(
            "네이버 테마맵 수집",
            ["python", "-m", "iceage.src.collectors.naver_theme_collector", ref_str],
        )

        # 5) 섹터/테마 집계
        run_step(
            "섹터/테마 집계",
            ["python", "-m", "iceage.src.processors.kr_sector_aggregator", ref_str],
        )
    else:
        print("[INFO] 수집 스킵 모드: 1)~5) 단계는 건너뜁니다. (기존 파일 그대로 사용)")

    # -----------------------
    # 6) 모닝 뉴스레터 생성
    # -----------------------
    # 뉴스레터는 비영업일에도 강제로 렌더할 수 있도록 ALLOW_RUN_NON_BUSINESS 기본값 1
    os.environ["ALLOW_RUN_NON_BUSINESS"] = os.environ.get(
        "ALLOW_RUN_NON_BUSINESS", "1"
    )

    run_step(
        "모닝 뉴스레터 생성",
        ["python", "-m", "iceage.src.pipelines.morning_newsletter", ref_str],
        critical=True,
    )

    # -----------------------
    # 7) 뉴스레터 HTML 렌더링
    # -----------------------
    run_step(
        "뉴스레터 HTML 렌더링",
        ["python", "-m", "iceage.src.pipelines.render_newsletter_html", ref_str],
    )

    # -----------------------
    # 8) SNS용 콘텐츠 생성 (인스타/숏폼/데일리) ★ -> LLM 쓰는 거라 일단 끔
    # -----------------------
    # if run_social_output:
    #     run_step(
    #         "SNS용 콘텐츠 생성",
    #         ["python", "-m", "iceage.src.pipelines.social_contents", ref_str],
    #     )
    # else:
    #     print("[INFO] SNS 콘텐츠 생성(LLM)은 주석 처리되어 스킵합니다.")
        
    # -----------------------
    # 9) SNS 카드뉴스 이미지 생성 (인스타 카드) ★
    # -----------------------
    if run_cardnews_output:
        run_step(
            "SNS 카드뉴스 이미지 생성",
            ["python", "-m", "iceage.src.pipelines.generate_cardnews_assets", ref_str],
        )
    else:
        print("[INFO] RUN_CARDNEWS_OUTPUT!=1 이므로 카드뉴스 생성은 스킵합니다.")
        
    # -----------------------
    # [NEW] 커뮤니티용 요약 이미지 생성
    # -----------------------
    if run_summary_image_output:
        run_step(
            "커뮤니티용 요약 이미지 생성",
            ["python", "-m", "iceage.src.pipelines.generate_summary_image", ref_str],
        )
    else:
        print("[INFO] RUN_SUMMARY_IMAGE_OUTPUT!=1 이므로 요약 이미지 생성은 스킵합니다.")

# -----------------------
    # 10) TTS 오디오 생성 (쇼츠 / 데일리) -> [주석 처리: 사용 안 함]
    # -----------------------
    # if run_tts_output:
    #     run_step(
    #         "TTS 오디오 생성",
    #         ["python", "-m", "iceage.src.pipelines.generate_tts_assets", ref_str],
    #     )
    # else:
    #     print("[INFO] RUN_TTS_OUTPUT!=1 이므로 TTS 생성은 스킵합니다.")

    # -----------------------
    # 11) SNS 영상 생성 (쇼츠 / 데일리) -> [주석 처리: 사용 안 함]
    # -----------------------
    # if run_video_output:
    #     run_step(
    #         "SNS 영상 생성",
    #         ["python", "-m", "iceage.src.pipelines.generate_video_assets", ref_str],
    #     )
    # else:
    #     print("[INFO] RUN_VIDEO_OUTPUT!=1 이므로 SNS 영상 생성은 스킵합니다.")
    
    
    # -----------------------
    # 12) 이메일 발송 (뉴스레터 + SNS 관리자) ★
    # -----------------------
    
    
    if os.getenv("NEWSLETTER_AUTO_SEND", "0") == "1":
        print("[INFO] NEWSLETTER_AUTO_SEND=1 이므로 이메일 발송 실행")
        run_step(
            "뉴스레터 / SNS 관리자 메일 발송",
            ["python", "-m", "iceage.src.pipelines.send_newsletter", ref_str],
        )
    else:
        print("[INFO] NEWSLETTER_AUTO_SEND!=1 이므로 이메일 발송은 스킵합니다.")

    # -----------------------
    # 슬랙 알림 (에러 / 성공)
    # -----------------------
    enable_slack = os.getenv("ENABLE_SLACK_ALERTS", "0") == "1"
    notify_on_success = os.getenv("SLACK_NOTIFY_ON_SUCCESS", "0") == "1"

    if enable_slack:
        slack_notifier = None
        try:
            # [개선] 임포트를 먼저 시도하여 모듈 존재 여부 확인
            from iceage.src.utils.slack_notifier import send_slack_message
            slack_notifier = send_slack_message

            if ERRORS:
                # 에러 요약
                summary = "\n".join(ERRORS[:5])
                msg = f"[Signalist Daily] ❌ 에러 발생 ({ref_str})\n{summary}"
            elif notify_on_success:
                # 성공 알림
                msg = f"[Signalist Daily] ✅ 정상 완료 ({ref_str})"
            else:
                msg = None

            if msg:
                slack_notifier(msg)
                print("[INFO] 슬랙 알림 전송 완료")

        except ImportError:
            print("[WARN] 슬랙 알림 전송 실패: 'iceage.src.utils.slack_notifier' 모듈을 찾을 수 없습니다.")
        except Exception as e: # 네트워크 오류 등 기타 예외 처리
            print(f"[WARN] 슬랙 알림 전송 실패: {e}")
# ... (위쪽 코드는 그대로 유지) ...

    # ====================================================
    # [수정] 폴더 단위 통째로 S3 백업 (퇴근)
    # ====================================================
    
    s3 = S3Manager()

    print("\n☁️ [S3 Sync] 데이터 및 결과물 전체 백업 중...")
    
    # [테스트용] recent_days=0 (오늘 파일만)
    # [실사용] recent_days=0
    BACKUP_DAYS = 0
    
    # 1. iceage/data 폴더 (raw, processed, reference 포함)
    s3.upload_directory(str(DATA_DIR), "iceage/data", recent_days=BACKUP_DAYS)

    # 2. iceage/out 폴더 (뉴스레터 마크다운 등)
    out_dir = PROJECT_ROOT / "out"
    if out_dir.exists():
        s3.upload_directory(str(out_dir), "iceage/out", recent_days=BACKUP_DAYS)

    print("\n✅ daily_runner 완료")

if __name__ == "__main__":
    main()
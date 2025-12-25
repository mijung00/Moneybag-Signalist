import sys
import time
import os
from datetime import datetime, timedelta
from threading import Thread

# watchdogs.py 또는 moralis_listener.py 의 최상단
from common.config import config

WATCHDOGS = [
    # (모듈 경로, 하트비트 파일 환경변수, 임계 시간(초), 필요한 시크릿 목록)
    ("iceage.src.pipelines.stock_watchdog", "ICEAGE_HEARTBEAT_PATH", 180, ["TELEGRAM_BOT_TOKEN_SIGNALIST", "SLACK_WEBHOOK_URL"]),
    ("moneybag.src.pipelines.market_watchdog", "MONEYBAG_HEARTBEAT_PATH", 180, ["TELEGRAM_BOT_TOKEN_MONEYBAG", "SLACK_WEBHOOK_URL"]),
]

MAX_RESTARTS_10MIN = int(os.getenv("WATCHDOG_MAX_RESTARTS_10MIN", "5"))
RESTART_BACKOFF_SEC = int(os.getenv("WATCHDOG_RESTART_BACKOFF_SEC", "15"))

threads = {}        # module -> Thread
restart_times = {}  # module -> [timestamps]


def _now():
    return datetime.utcnow()


def _too_many_restarts(module: str) -> bool:
    ts = restart_times.get(module, [])
    cutoff = _now() - timedelta(minutes=10)
    ts = [t for t in ts if t >= cutoff]
    restart_times[module] = ts
    return len(ts) >= MAX_RESTARTS_10MIN


def _record_restart(module: str):
    restart_times.setdefault(module, []).append(_now())


def _run_task_in_thread(module_name: str, secrets_to_load: list):
    """스레드에서 실행될 실제 작업 함수"""
    try:
        # 1. 이 스레드에 필요한 시크릿을 로드합니다.
        for secret in secrets_to_load:
            config.ensure_secret(secret)

        # 2. 모듈을 동적으로 임포트하고 main 함수를 실행합니다.
        print(f"  -> [{module_name}] 스레드 시작...")
        module = __import__(module_name, fromlist=['main'])
        module.main()
    except Exception as e:
        print(f"❌ [{module_name}] 스레드 실행 중 치명적 오류: {e}", file=sys.stderr)

def _start(module: str, secrets: list):
    print(f"🚀 [Manager] Thread Start: {module}")
    thread = Thread(target=_run_task_in_thread, args=(module, secrets), daemon=True)
    thread.start()
    threads[module] = thread
    _record_restart(module)


def _heartbeat_stale(path: str, stale_sec: int) -> bool:
    if not path:
        return False
    try:
        if not os.path.exists(path):
            return True
        mtime = os.path.getmtime(path)
        age = time.time() - mtime
        return age > stale_sec
    except Exception:
        return False


def run_watchdogs():
    print("🦅 [Manager] 통합 왓치독 매니저 시작")
    print(f"🔧 [Manager] python: {sys.executable}")

    # 최초 기동
    for module, _, _, secrets in WATCHDOGS:
        _start(module, secrets)

    while True:
        time.sleep(5)

        for module, hb_env, stale_sec, secrets in WATCHDOGS:
            thread = threads.get(module)

            # 1) 스레드가 죽었거나, 2) 살아있는데 heartbeat가 멈췄으면 재시작
            hb_path = os.getenv(hb_env, "")
            is_stale = hb_path and _heartbeat_stale(hb_path, stale_sec)

            if (thread and not thread.is_alive()) or is_stale:
                if is_stale:
                    print(f"⚠️ [Manager] {module} heartbeat stale 감지 → 재시작 ({hb_path})")
                else:
                    print(f"⚠️ [Manager] {module} 스레드 종료 감지 → 재시작")

                if _too_many_restarts(module):
                    print(f"⛔ [Manager] {module} 재시작 과다(10분 {MAX_RESTARTS_10MIN}회). 잠깐 대기")
                    continue

                time.sleep(RESTART_BACKOFF_SEC)
                _start(module, secrets)


if __name__ == "__main__":
    run_watchdogs()

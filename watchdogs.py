import subprocess
import sys
import time
import os
from datetime import datetime, timedelta

WATCHDOGS = [
    ("iceage.src.pipelines.stock_watchdog", "ICEAGE_HEARTBEAT_PATH", 180),   # 3분
    ("moneybag.src.pipelines.market_watchdog", "MONEYBAG_HEARTBEAT_PATH", 180),
]

MAX_RESTARTS_10MIN = int(os.getenv("WATCHDOG_MAX_RESTARTS_10MIN", "5"))
RESTART_BACKOFF_SEC = int(os.getenv("WATCHDOG_RESTART_BACKOFF_SEC", "15"))

procs = {}          # module -> Popen
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


def _start(module: str, env: dict):
    python_executable = sys.executable
    print(f"🚀 [Manager] Start: {module}")
    p = subprocess.Popen([python_executable, "-u", "-m", module], env=env)
    procs[module] = p
    _record_restart(module)


def _stop(module: str):
    p = procs.get(module)
    if not p:
        return
    try:
        p.terminate()
        p.wait(timeout=10)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass


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
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    print("🦅 [Manager] 통합 왓치독 매니저 시작")
    print(f"🔧 [Manager] python: {sys.executable}")

    # 최초 기동
    for module, hb_env, _stale in WATCHDOGS:
        _start(module, env)

    while True:
        time.sleep(5)

        for module, hb_env, stale_sec in WATCHDOGS:
            p = procs.get(module)

            # 1) 프로세스가 죽었으면 재시작
            if p and p.poll() is not None:
                print(f"⚠️ [Manager] {module} 종료 감지 (exit={p.returncode})")
                if _too_many_restarts(module):
                    print(f"⛔ [Manager] {module} 재시작 과다(10분 {MAX_RESTARTS_10MIN}회). 잠깐 대기")
                    continue
                time.sleep(RESTART_BACKOFF_SEC)
                _start(module, env)
                continue

            # 2) 살아있는데 heartbeat가 멈췄으면(먹통) 재시작
            hb_path = os.getenv(hb_env, "")
            if hb_path and _heartbeat_stale(hb_path, stale_sec):
                print(f"⚠️ [Manager] {module} heartbeat stale 감지 → 재시작 ({hb_path})")
                if _too_many_restarts(module):
                    print(f"⛔ [Manager] {module} 재시작 과다(10분 {MAX_RESTARTS_10MIN}회). 잠깐 대기")
                    continue
                _stop(module)
                time.sleep(RESTART_BACKOFF_SEC)
                _start(module, env)


if __name__ == "__main__":
    run_watchdogs()

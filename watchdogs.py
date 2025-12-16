import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass

# 어떤 모듈을 감시할지
MODULES = [
    "iceage.src.pipelines.stock_watchdog",
    "moneybag.src.pipelines.market_watchdog",
]

# 각 모듈이 "살아있다"는 표시로 업데이트하는 파일(각 watchdog 파일에서도 같은 경로로 씀)
HEARTBEAT_FILES = {
    "iceage.src.pipelines.stock_watchdog": os.getenv("ICEAGE_HEARTBEAT_PATH", "/tmp/iceage_stock_watchdog.heartbeat"),
    "moneybag.src.pipelines.market_watchdog": os.getenv("MONEYBAG_HEARTBEAT_PATH", "/tmp/moneybag_market_watchdog.heartbeat"),
}

CHECK_INTERVAL_SEC = int(os.getenv("WATCHDOG_MANAGER_CHECK_INTERVAL_SEC", "5"))
HEARTBEAT_STALE_SEC = int(os.getenv("WATCHDOG_HEARTBEAT_STALE_SEC", "120"))
HEARTBEAT_GRACE_SEC = int(os.getenv("WATCHDOG_HEARTBEAT_GRACE_SEC", "60"))  # 시작 후 이 시간까지는 heartbeat 없어도 봐줌

RESTART_DELAY_SEC = int(os.getenv("WATCHDOG_RESTART_DELAY_SEC", "3"))
MAX_RESTARTS_PER_10MIN = int(os.getenv("WATCHDOG_MAX_RESTARTS_PER_10MIN", "30"))

_stop_requested = False


def _handle_stop(signum, frame):
    global _stop_requested
    _stop_requested = True
    print(f"\n🛑 [Manager] stop signal received ({signum}). stopping...", flush=True)


@dataclass
class ProcState:
    module: str
    proc: subprocess.Popen
    started_at: float
    restart_times: list  # timestamps


def _start_module(python_executable: str, module: str, env: dict) -> ProcState:
    print(f"🚀 [Manager] Start: {module}", flush=True)
    p = subprocess.Popen(
        [python_executable, "-u", "-m", module],
        env=env,
        start_new_session=True,  # 프로세스 그룹 단위로 종료하기 위함
    )
    return ProcState(module=module, proc=p, started_at=time.time(), restart_times=[])


def _terminate(state: ProcState, timeout_sec: int = 15):
    p = state.proc
    if p.poll() is not None:
        return
    try:
        os.killpg(p.pid, signal.SIGTERM)
    except Exception as e:
        print(f"⚠️ [Manager] SIGTERM failed ({state.module}): {e}", flush=True)

    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        if p.poll() is not None:
            return
        time.sleep(0.5)

    try:
        os.killpg(p.pid, signal.SIGKILL)
        print(f"💥 [Manager] SIGKILL sent ({state.module})", flush=True)
    except Exception as e:
        print(f"⚠️ [Manager] SIGKILL failed ({state.module}): {e}", flush=True)


def _heartbeat_is_stale(module: str, started_at: float) -> bool:
    hb = HEARTBEAT_FILES.get(module)
    if not hb:
        return False  # heartbeat 경로가 없으면 이 기능은 스킵
    try:
        st = os.stat(hb)
        age = time.time() - st.st_mtime
        return age > HEARTBEAT_STALE_SEC
    except FileNotFoundError:
        # 시작 직후에는 파일이 아직 없을 수 있으니 grace 기간은 봐준다
        if time.time() - started_at < HEARTBEAT_GRACE_SEC:
            return False
        return True
    except Exception:
        return False


def _rate_limit_ok(state: ProcState) -> bool:
    now = time.time()
    state.restart_times = [t for t in state.restart_times if now - t < 600]
    return len(state.restart_times) < MAX_RESTARTS_PER_10MIN


def main():
    python_executable = sys.executable
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    # 배포/재시작 뒤 "예전 heartbeat 파일" 때문에 오판하지 않도록 제거 시도
    for f in HEARTBEAT_FILES.values():
        try:
            os.remove(f)
        except FileNotFoundError:
            pass
        except Exception:
            pass

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print("🦅 [Manager] 통합 왓치독 매니저 시작", flush=True)
    print(f"🔧 [Manager] python: {python_executable}", flush=True)

    states = {m: _start_module(python_executable, m, env) for m in MODULES}

    while not _stop_requested:
        time.sleep(CHECK_INTERVAL_SEC)

        for m in list(states.keys()):
            state = states[m]
            rc = state.proc.poll()

            # 1) 프로세스가 종료된 경우: 재시작
            if rc is not None:
                print(f"⚠️ [Manager] Dead: {m} exited (code={rc}).", flush=True)

                if not _rate_limit_ok(state):
                    print(f"🧯 [Manager] Too many restarts for {m}. wait 60s then retry.", flush=True)
                    time.sleep(60)

                time.sleep(RESTART_DELAY_SEC)
                state.restart_times.append(time.time())
                states[m] = _start_module(python_executable, m, env)
                continue

            # 2) 프로세스는 살아있는데 heartbeat가 오래됨 = '멈춘 것' 가능성
            if _heartbeat_is_stale(m, state.started_at):
                print(f"🧊 [Manager] Stale heartbeat detected. Restarting: {m}", flush=True)
                _terminate(state)
                time.sleep(RESTART_DELAY_SEC)
                state.restart_times.append(time.time())
                states[m] = _start_module(python_executable, m, env)

    print("🧹 [Manager] stopping children...", flush=True)
    for state in states.values():
        _terminate(state)

    print("✅ [Manager] stopped.", flush=True)


if __name__ == "__main__":
    main()

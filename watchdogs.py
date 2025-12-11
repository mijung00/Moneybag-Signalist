import sys
import time
import subprocess
from datetime import datetime

def run_process(cmd, cwd):
    """프로세스를 백그라운드로 실행하는 함수"""
    print(f"🚀 [Start Watchdog] {cmd} in {cwd}")
    return subprocess.Popen(cmd, cwd=cwd, shell=False)

if __name__ == "__main__":
    print("🦅 통합 왓치독 매니저 시작...")
    
    # 1. 시그널리스트 왓치독 (stock_watchdog.py)
    p1 = run_process(
        [sys.executable, "-m", "iceage.src.pipelines.stock_watchdog"], 
        cwd="iceage" # iceage 폴더 안에서 실행
    )
    
    # 2. 머니백 왓치독 (market_watchdog.py)
    p2 = run_process(
        [sys.executable, "-m", "moneybag.src.pipelines.market_watchdog"], 
        cwd="moneybag" # moneybag 폴더 안에서 실행
    )

    # 3. 무한 루프로 감시 (죽으면 다시 살리는 로직은 AWS가 담당하지만, 여기서도 일단 대기)
    try:
        while True:
            time.sleep(60)
            # 살아있는지 체크 (필요하면 여기서 죽은 놈 다시 살리는 로직 추가 가능)
            if p1.poll() is not None:
                print("⚠️ 시그널리스트 왓치독이 죽었습니다. 재시작합니다...")
                p1 = run_process([sys.executable, "-m", "iceage.src.pipelines.stock_watchdog"], cwd="iceage")
                
            if p2.poll() is not None:
                print("⚠️ 머니백 왓치독이 죽었습니다. 재시작합니다...")
                p2 = run_process([sys.executable, "-m", "moneybag.src.pipelines.market_watchdog"], cwd="moneybag")
                
    except KeyboardInterrupt:
        print("🛑 왓치독 종료 요청받음.")
        p1.terminate()
        p2.terminate()
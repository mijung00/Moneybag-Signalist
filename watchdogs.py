import sys
import time
import subprocess
import os

def run_process(cmd):
    """프로세스를 백그라운드로 실행하는 함수 (현재 위치에서 실행)"""
    # 현재 watchdogs.py가 있는 폴더(루트)를 기준으로 잡음
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    print(f"🚀 [Start Watchdog] {cmd} in {base_dir}")
    
    # cwd 설정을 없애거나 base_dir로 설정해야 'iceage.src...'를 찾을 수 있음!
    return subprocess.Popen(cmd, cwd=base_dir, shell=False)

if __name__ == "__main__":
    print("🦅 통합 왓치독 매니저 시작 (Fixed Path Version)...")
    
    # 1. 시그널리스트 왓치독
    # cwd="iceage" 제거함! -> 루트에서 실행해야 iceage 패키지를 인식함
    p1 = run_process([sys.executable, "-m", "iceage.src.pipelines.stock_watchdog"])
    
    # 2. 머니백 왓치독
    # cwd="moneybag" 제거함!
    p2 = run_process([sys.executable, "-m", "moneybag.src.pipelines.market_watchdog"])

    # 3. 무한 루프로 감시
    try:
        while True:
            time.sleep(60)
            
            # 프로세스 죽었는지 체크
            if p1.poll() is not None:
                print("⚠️ 시그널리스트 왓치독 사망. 심폐소생술 실시...")
                p1 = run_process([sys.executable, "-m", "iceage.src.pipelines.stock_watchdog"])
                
            if p2.poll() is not None:
                print("⚠️ 머니백 왓치독 사망. 심폐소생술 실시...")
                p2 = run_process([sys.executable, "-m", "moneybag.src.pipelines.market_watchdog"])
                
    except KeyboardInterrupt:
        print("🛑 왓치독 종료 요청받음.")
        p1.terminate()
        p2.terminate()
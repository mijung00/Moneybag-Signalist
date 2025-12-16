import subprocess
import sys
import time
import os
import signal

# 프로세스 목록 관리
processes = []

def run_watchdogs():
    # 현재 실행 중인 파이썬(가상환경)의 경로를 절대경로로 가져옴
    python_executable = sys.executable 
    
    print(f"🦅 통합 왓치독 매니저 시작...")
    print(f"🔧 실행 환경: {python_executable}")

    # 실행할 모듈 목록
    watchdogs = [
        "iceage.src.pipelines.stock_watchdog",
        "moneybag.src.pipelines.market_watchdog"
    ]

    # 환경변수 복사 (토큰 전달용)
    env = os.environ.copy()
    # 파이썬 출력 버퍼링 끄기 (로그 즉시 출력)
    env["PYTHONUNBUFFERED"] = "1"

    for module in watchdogs:
        print(f"🚀 [Start] {module} 실행 중...")
        
        # subprocess.Popen을 사용할 때 sys.executable을 써야 가상환경이 유지됨
        p = subprocess.Popen(
            [python_executable, "-m", module],
            env=env,
            stdout=sys.stdout, # 자식의 출력을 부모의 출력으로 연결
            stderr=sys.stderr  # 에러도 연결
        )
        processes.append(p)

    print("✅ 모든 왓치독이 배치되었습니다. 감시를 시작합니다.")

    # 메인 프로세스가 죽지 않고 계속 살아있으면서 자식들을 지켜봄
    try:
        while True:
            time.sleep(10)
            # 혹시 죽은 프로세스가 있는지 체크
            for i, p in enumerate(processes):
                if p.poll() is not None: # 죽었으면
                    print(f"⚠️ [Warning] 프로세스 {watchdogs[i]} 가 종료되었습니다 (Exit Code: {p.returncode})")
                    # 여기서 재시작 로직을 넣을 수도 있음 (지금은 일단 로그만)
    except KeyboardInterrupt:
        print("\n🛑 왓치독 매니저 종료 중...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    run_watchdogs()
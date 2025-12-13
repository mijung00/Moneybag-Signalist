import os
import sys
import subprocess
from datetime import datetime, timedelta
from flask import Flask

application = Flask(__name__)

# ==========================================
# 🛠️ 공통 함수: 스크립트 실행기 (단발성 뉴스레터용)
# ==========================================
def run_script(folder_name, module_path, args=[]):
    """특정 폴더의 모듈을 프로젝트 루트에서 실행하는 함수"""
    # 1. 루트 폴더(Moneybag-Signalist-main)를 기준점으로 잡음
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. 모듈 경로를 '폴더명.모듈명' 형태로 조합 (예: iceage.src.pipelines.daily_runner)
    full_module_path = f"{folder_name}.{module_path}"
    
    cmd = [sys.executable, "-m", full_module_path] + args
    
    print(f"🚀 [Start Task] {full_module_path}")
    
    try:
        # 3. cwd(실행 위치)를 폴더 안이 아니라 'base_dir(루트)'로 설정!
        result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True, encoding='utf-8')
        print(f"✅ Output:\n{result.stdout}")
        if result.stderr:
            print(f"⚠️ Error Log:\n{result.stderr}")
        return "SUCCESS" if result.returncode == 0 else f"FAIL: {result.stderr}"
    except Exception as e:
        print(f"❌ Exception: {e}")
        return f"EXCEPTION: {str(e)}"

# ==========================================
# 🦅 왓치독 실행기 (경비 대장 깨우기)
# ==========================================
# def kickstart_watchdog_manager():
#     """
#     서버 옆에 있는 watchdogs.py 파일을 백그라운드에서 실행합니다.
#     """
#     try:
#         # 현재 폴더(C:\ubuntu)에 있는 watchdogs.py를 찾음
#         current_dir = os.path.dirname(os.path.abspath(__file__))
#         script_path = os.path.join(current_dir, "watchdogs.py")
#
#         print(f"🦅 [System] 왓치독 매니저를 실행합니다... ({script_path})")
#         
#         # Popen을 써야 웹서버가 멈추지 않고 계속 돌아감 (Non-blocking)
#         # 로그는 웹서버 로그랑 같이 찍히도록 설정
#         subprocess.Popen([sys.executable, script_path], cwd=current_dir)
#         
#     except Exception as e:
#         print(f"❌ [Critical] 왓치독 실행 실패: {e}")

# 🔥 서버가 켜질 때 왓치독 매니저도 같이 실행!
# (로컬 개발 환경에서 저장할 때마다 두 번 실행되는 것 방지)
# if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
#     kickstart_watchdog_manager()



# ==========================================
# 🌐 플라스크 라우트 (뉴스레터 및 헬스체크)
# ==========================================
@application.route('/run_moneybag_morning', methods=['GET', 'POST'])
def moneybag_morning():
    return run_script("moneybag", "src.pipelines.daily_runner", ["morning"]), 200

@application.route('/run_moneybag_night', methods=['GET', 'POST'])
def moneybag_night():
    return run_script("moneybag", "src.pipelines.daily_runner", ["night"]), 200

@application.route('/run_signalist', methods=['GET', 'POST'])
def signalist_morning():
    return run_script("iceage", "src.pipelines.daily_runner"), 200

@application.route('/update_stock_data', methods=['GET', 'POST'])
def update_stock_data():
    today = datetime.now()
    logs = []
    collectors = [
        "src.collectors.krx_listing_collector",
        "src.collectors.krx_index_collector",
        "src.collectors.krx_daily_price_collector"
    ]
    for i in range(3, 0, -1):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        logs.append(f"Date: {date_str}")
        for module in collectors:
            msg = run_script("iceage", module, [date_str])
            logs.append(f" - {module}: {msg}")
    return "\n".join(logs), 200

@application.route('/', methods=['GET'])
def health_check():
    return "OK", 200

if __name__ == "__main__":
    application.run(port=5000)
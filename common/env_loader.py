# common/env_loader.py
import os
from pathlib import Path

def load_env(repo_root: Path | None = None) -> None:
    """
    - 로컬 개발환경: repo_root/.env 있으면 로드
    - EB(서버): 환경변수는 EB가 프로세스에 주입해주므로 os.environ을 그대로 쓰는 게 정석
      (단, root로 도는 배치/크론에서만 필요하면 EB env 파일을 '읽을 수 있을 때만' 로드)
    """
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    if repo_root:
        local_env = Path(repo_root) / ".env"
        if local_env.exists():
            load_dotenv(local_env, override=False)

    eb_env = Path("/opt/elasticbeanstalk/deployment/env")
    if eb_env.exists() and os.access(eb_env, os.R_OK):
        # 읽을 수 있을 때만 (대부분 cron/root에서만 true)
        load_dotenv(eb_env, override=False)

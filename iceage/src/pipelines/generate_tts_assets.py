# iceage/src/pipelines/generate_tts_assets.py
import asyncio
import sys
import os
import platform  # OS 확인용 추가
from pathlib import Path
import edge_tts

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOCIAL_DIR = PROJECT_ROOT / "iceage" / "out" / "social"
AUDIO_DIR = SOCIAL_DIR / "audio"

# [설정] 목소리 (SunHi가 안 될 경우 InJoon으로 변경해볼 것)
VOICE = "ko-KR-SunHiNeural" 

async def _synthesize_edge_tts(text: str, out_path: Path) -> bool:
    """
    Edge TTS로 음성 생성 (Retry 로직 포함)
    """
    # 텍스트가 비어있는지 확인
    if not text or not text.strip():
        print("   ❌ 오류: 변환할 텍스트가 비어있습니다.")
        return False

    for attempt in range(3): # 최대 3회 재시도
        try:
            # rate 옵션을 제거하여 기본 속도로 설정 (오류 최소화)
            communicate = edge_tts.Communicate(text, VOICE)
            await communicate.save(str(out_path))
            
            # 파일 생성 확인
            if out_path.exists() and out_path.stat().st_size > 100:
                return True
        except Exception as e:
            print(f"   [Retry {attempt+1}/3] TTS 생성 실패: {e}")
            await asyncio.sleep(2) # 대기 후 재시도
            
    return False

async def run_async_tts(ref_date: str):
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Shorts Script
    shorts_md = SOCIAL_DIR / f"Signalist_YouTubeShorts_{ref_date}.md"
    if shorts_md.exists():
        text = shorts_md.read_text(encoding='utf-8')
        # 마크다운 헤더 등 제거하고 순수 텍스트만 추출
        clean_text = text.replace("#", "").replace("*", "").replace("-", "")
        clean_text = clean_text[:1000] 
        
        out_path = AUDIO_DIR / f"shorts_{ref_date}.mp3"
        print(f"🎙️ [TTS] Shorts 오디오 생성 시도: {out_path.name}")
        
        success = await _synthesize_edge_tts(clean_text, out_path)
        if success:
            print("   ✅ 생성 성공")
        else:
            print("   ❌ 생성 실패 (3회 시도 모두 실패)")

    # 2. Daily Script (Optional)
    daily_md = SOCIAL_DIR / f"Signalist_YouTubeDaily_{ref_date}.md"
    if daily_md.exists():
        text = daily_md.read_text(encoding='utf-8')
        clean_text = text.replace("#", "").replace("*", "")
        
        out_path = AUDIO_DIR / f"daily_{ref_date}.mp3"
        print(f"🎙️ [TTS] Daily 오디오 생성 시도: {out_path.name}")
        
        success = await _synthesize_edge_tts(clean_text, out_path)
        if success:
            print("   ✅ 생성 성공")

def generate_tts_for_date(ref_date: str):
    # Windows 환경에서 asyncio RuntimeError 방지
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_async_tts(ref_date))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        generate_tts_for_date(sys.argv[1])
    else:
        print("Usage: python -m ... YYYY-MM-DD")
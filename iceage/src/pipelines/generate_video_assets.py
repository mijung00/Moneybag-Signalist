# iceage/src/pipelines/generate_video_assets.py
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from moviepy.editor import (
    AudioFileClip, ImageClip, TextClip, CompositeVideoClip, 
    concatenate_videoclips, ColorClip, vfx
)
import math

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

SOCIAL_DIR = PROJECT_ROOT / "iceage" / "out" / "social"
CARD_DIR = SOCIAL_DIR / "cardnews"
AUDIO_DIR = SOCIAL_DIR / "audio"
VIDEO_DIR = SOCIAL_DIR / "video"

def _load_audio(kind: str, ref_date: str):
    """
    오디오 파일 로드 (파일이 없거나 깨졌으면 None 반환)
    """
    fname = f"{kind}_{ref_date}.mp3"
    audio_path = AUDIO_DIR / fname
    
    # [Safety Check] 파일 존재 및 크기 확인 (1KB 미만이면 실패로 간주)
    if not audio_path.exists() or audio_path.stat().st_size < 1000:
        print(f"[WARN] 오디오 파일이 없거나 손상되었습니다: {audio_path}")
        return None
        
    try:
        return AudioFileClip(str(audio_path))
    except Exception as e:
        print(f"[ERROR] 오디오 로드 실패: {e}")
        return None

def _make_video(kind: str, ref_date: str):
    """
    카드뉴스 이미지 + TTS 오디오 -> 영상 합성
    """
    # 1. 이미지 경로 확인
    img_dir = CARD_DIR / ref_date
    if not img_dir.exists():
        print(f"[SKIP] 카드뉴스 이미지가 없습니다: {img_dir}")
        return

    images = sorted(list(img_dir.glob("*.png")))
    if not images:
        print(f"[SKIP] 이미지가 0장입니다.")
        return

    # 2. 오디오 로드 (실패 시 중단)
    audio_clip = _load_audio(kind, ref_date)
    if audio_clip is None:
        print(f"[SKIP] 오디오 문제로 영상 생성을 건너뜁니다.")
        return

    print(f"[INFO] 영상 생성 시작 ({kind}): {ref_date}")
    
    # 3. 컷당 지속 시간 계산
    total_duration = audio_clip.duration
    # 앞뒤 여유 1초씩 둠
    total_duration += 2.0 
    
    clip_duration = total_duration / len(images)
    
    # 4. 클립 생성
    clips = []
    for img_path in images:
        clip = ImageClip(str(img_path)).set_duration(clip_duration)
        # 줌인 효과 (Zoom-in) - 10% 확대
        clip = clip.resize(lambda t: 1 + 0.05 * (t / clip_duration)) 
        clips.append(clip)
        
    video = concatenate_videoclips(clips, method="compose")
    video = video.set_audio(audio_clip)
    
    # 5. 내보내기
    VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    out_path = VIDEO_DIR / f"{kind}_{ref_date}.mp4"
    
    # 유튜브 쇼츠 포맷 (1080x1920)
    video.write_videofile(
        str(out_path),
        fps=24,
        codec="libx264",
        audio_codec="aac",
        threads=4,
        logger=None # 지저분한 로그 숨기기
    )
    print(f"✅ 영상 생성 완료: {out_path}")

def generate_videos_for_date(ref_date: str):
    # 쇼츠 영상만 생성 (데일리는 필요시 추가)
    _make_video("shorts", ref_date)

def main():
    if len(sys.argv) > 1:
        ref_date = sys.argv[1]
    else:
        import datetime
        ref_date = datetime.date.today().isoformat()
        
    generate_videos_for_date(ref_date)

if __name__ == "__main__":
    main()
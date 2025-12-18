import boto3
import os
import time
import re
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, ClientError

class S3Manager:
    def __init__(self, bucket_name="fincore-output-storage"):
        """
        AWS S3 연결 관리자 (Moneybag & Signalist 공용)
        """
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', region_name='ap-northeast-2')

    def upload_file(self, local_file_path, s3_file_path):
        """단일 파일 업로드"""
        if not os.path.exists(local_file_path):
            print(f"❌ [Fail] 파일 없음: {local_file_path}")
            return False
        try:
            s3_key = s3_file_path.replace("\\", "/") 
            print(f"☁️ [Upload] {local_file_path} -> {s3_key}")
            self.s3.upload_file(local_file_path, self.bucket_name, s3_key)
            return True
        except Exception as e:
            print(f"❌ [Error] {e}")
            return False
        
    def get_text_content(self, s3_key):
        """
        [NEW] S3에 있는 파일을 텍스트(String)로 읽어옵니다. (웹 뷰어용)
        """
        try:
            s3_key = s3_key.replace("\\", "/")
            response = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read().decode('utf-8')
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"❌ [Read Error] {e}")
            return None

    def download_file(self, s3_file_path, local_file_path):
        """단일 파일 다운로드"""
        try:
            s3_key = s3_file_path.replace("\\", "/")
            local_dir = os.path.dirname(local_file_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir)
            
            print(f"📥 [Download] {s3_key} -> {local_file_path}")
            self.s3.download_file(self.bucket_name, s3_key, local_file_path)
            return True
        except ClientError:
            return False
        except Exception as e:
            print(f"❌ [Error] {e}")
            return False

    def get_latest_file_in_prefix(self, prefix):
        """
        [NEW] 특정 경로(prefix)에 있는 파일 중 가장 최신(파일명 정렬 기준) 파일을 찾습니다.
        """
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            all_files = []
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith("/"):
                            all_files.append(key)
            
            if not all_files:
                return None
            
            # [수정] 단순 문자열 정렬이 아닌, '날짜'를 추출해서 정렬
            def _extract_date(fname):
                # YYYY-MM-DD 패턴 찾기
                match = re.search(r'(\d{4}-\d{2}-\d{2})', fname)
                if match:
                    return match.group(1)
                return "0000-00-00" # 날짜 없으면 맨 뒤로

            # 날짜 기준 오름차순 정렬 -> 같은 날짜면 파일명(Morning/Night) 순
            sorted_files = sorted(all_files, key=lambda x: (_extract_date(x), x))
            return sorted_files[-1]
        except Exception as e:
            print(f"❌ [S3 List Error] {e}")
            return None

    def get_latest_file_in_prefix(self, prefix):
        """
        [NEW] 특정 경로(prefix)에 있는 파일 중 가장 최신(파일명 정렬 기준) 파일을 찾습니다.
        """
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            all_files = []
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith("/"):
                            all_files.append(key)
            
            return sorted(all_files)[-1] if all_files else None
        except Exception as e:
            print(f"❌ [S3 List Error] {e}")
            return None

    def upload_directory(self, local_dir, s3_prefix, recent_days=2):
        """
        📁 [스마트 동기화] 하위 폴더 포함, 날짜 기준 업로드
        :param recent_days: 0=당일(자정 이후), N=최근 N일, None=전체
        """
        if not os.path.exists(local_dir):
            print(f"⚠️ [Skip] 로컬 폴더 없음: {local_dir}")
            return

        print(f"\n📦 [Sync Start] {local_dir} (하위 폴더 포함) -> {s3_prefix}")
        
        # 기준 시간 설정
        if recent_days is not None:
            if recent_days == 0:
                cutoff_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                print(f"   👉 옵션: [당일] {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} 이후 파일만 업로드")
            else:
                cutoff_time = datetime.now() - timedelta(days=recent_days)
                print(f"   👉 옵션: [최근 {recent_days}일] {cutoff_time.strftime('%Y-%m-%d')} 이후 파일만 업로드")
        else:
            cutoff_time = None
            print("   👉 옵션: 모든 파일 업로드")
        
        count = 0
        skip_count = 0
        
        # os.walk로 모든 하위 폴더 재귀 탐색
        for root, dirs, files in os.walk(local_dir):
            # 불필요한 시스템 폴더 제외
            if 'venv' in root or '.git' in root or '__pycache__' in root:
                continue

            for filename in files:
                local_path = os.path.join(root, filename)
                
                # 날짜 필터링
                if cutoff_time:
                    mtime = datetime.fromtimestamp(os.path.getmtime(local_path))
                    if mtime < cutoff_time:
                        skip_count += 1
                        continue 

                # S3 경로 계산 (상대 경로 유지)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                
                if self.upload_file(local_path, s3_path):
                    count += 1
        
        print(f"✅ [Sync Done] 업로드: {count}개 / 건너뜀(구형): {skip_count}개")


# --- 👇 로컬 테스트 실행 영역 ---
if __name__ == "__main__":
    manager = S3Manager()
    
    # 테스트할 폴더 목록 (하위 폴더까지 싹 다 뒤짐)
    target_folders = [
        "iceage/data",
        "iceage/out",
        "moneybag/data"
    ]
    
    print("\n🚀 [테스트 시작] 당일(오늘 0시 이후) 생성된 파일만 업로드합니다.\n")
    
    for folder in target_folders:
        # recent_days=0 : 오늘 만든 것만!
        manager.upload_directory(folder, folder, recent_days=0)
import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

class S3Manager:
    def __init__(self, bucket_name="fincore-output-storage"):
        """
        AWS S3 연결 관리자 (Moneybag & Signalist 공용)
        """
        self.bucket_name = bucket_name
        # AWS 서버(EC2)에서는 권한을 자동으로 가져오므로 키 입력 불필요
        self.s3 = boto3.client('s3', region_name='ap-northeast-2')

    def upload_file(self, local_file_path, s3_file_path):
        """단일 파일 업로드"""
        if not os.path.exists(local_file_path):
            return False
        try:
            s3_key = s3_file_path.replace("\\", "/") # 윈도우 경로 호환
            print(f"☁️ [S3 Upload] {local_file_path} -> {s3_key}")
            self.s3.upload_file(local_file_path, self.bucket_name, s3_key)
            return True
        except Exception as e:
            print(f"❌ [S3 Upload Error] {e}")
            return False

    def download_file(self, s3_file_path, local_file_path):
        """단일 파일 다운로드"""
        try:
            s3_key = s3_file_path.replace("\\", "/")
            local_dir = os.path.dirname(local_file_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir)
            
            print(f"📥 [S3 Download] {s3_key} -> {local_file_path}")
            self.s3.download_file(self.bucket_name, s3_key, local_file_path)
            return True
        except ClientError:
            # 파일이 없는 건 에러가 아님 (첫 실행 등)
            return False
        except Exception as e:
            print(f"❌ [S3 Download Error] {e}")
            return False

    def upload_directory(self, local_dir, s3_prefix):
        """
        📁 [신규 기능] 폴더 통째로 업로드 (하위 폴더 포함)
        :param local_dir: 로컬 폴더 경로 (예: iceage/data)
        :param s3_prefix: S3에 저장될 앞부분 경로 (예: iceage/data)
        """
        if not os.path.exists(local_dir):
            print(f"⚠️ [S3 Sync] 로컬에 폴더가 없습니다: {local_dir}")
            return

        print(f"\n📦 [S3 Directory Sync] 폴더 동기화 시작: {local_dir} -> {s3_prefix}")
        
        count = 0
        # os.walk로 모든 하위 폴더/파일을 탐색
        for root, dirs, files in os.walk(local_dir):
            for filename in files:
                # 로컬 파일의 절대 경로
                local_path = os.path.join(root, filename)
                
                # 폴더 구조를 유지하기 위해 상대 경로 계산
                # 예: iceage/data/raw/prices.csv -> raw/prices.csv
                relative_path = os.path.relpath(local_path, local_dir)
                
                # S3 경로 = 접두어 + 상대 경로
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                
                if self.upload_file(local_path, s3_path):
                    count += 1
        
        print(f"✅ [S3 Directory Sync] 총 {count}개 파일 업로드 완료!\n")
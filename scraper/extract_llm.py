import json
import os
import requests
import google.generativeai as genai
from dotenv import load_dotenv

# .env 로드 (GitHub 추적 제외된 파일)
load_dotenv()

# Google API Key 설정
api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    # ⚠️ 경고: 키 없음
    print("❌ GOOGLE_API_KEY가 .env 파일이나 환경 변수에 없습니다.")
    print("  1. .env 파일 생성: GOOGLE_API_KEY=AIzaSy...")
    print("  2. export GOOGLE_API_KEY=AIzaSy...")
    sys.exit(1)

genai.configure(api_key=api_key)

# ... (기존 로직 유지) ...

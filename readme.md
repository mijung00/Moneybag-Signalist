# ✨ Acknowledgements

This project was architected and implemented by **Google's Gemini Code Assist** under the direction of the project owner.

As the project owner, I provided the vision, requirements, and direction. Gemini Code Assist was responsible for the architectural design, code implementation, debugging, and deployment configurations based on my conversational prompts. This repository stands as a testament to the power of collaborative development between a human director and an AI coding partner.

---

# 💰 Moneybag & Signalist (Fincore Engine)

**Moneybag & Signalist**는 한국 주식 시장(KRX)과 글로벌 암호화폐 시장을 실시간으로 분석하여, 투자 인사이트를 제공하는 **자동화된 퀀트/AI 분석 엔진**입니다.

AWS Elastic Beanstalk 환경에서 운용되며, 데이터 수집부터 분석, 콘텐츠 생성(뉴스레터, 카드뉴스), 배포(이메일, 슬랙)까지 전 과정이 자동화되어 있습니다.

---

## 🚀 Key Features

### 🧊 Iceage (Signalist) - 국내 주식 분석 파이프라인
* **Market Data**: KRX 전 종목 시세, 지수, 투자자별 매매동향 수집 (KRX API & Naver Finance Fallback)
* **Data Analysis**:
    * **Volume Anomaly**: 거래량 폭증/건조 등 특이 패턴 탐지
    * **Theme Detector**: 네이버 금융 기반 실시간 주도 테마/섹터 분석
    * **Strategy Selector**: 투매, 낙폭과대, 눌림목 등 다양한 퀀트 전략에 기반한 타겟 종목 선정
* **Content Generation**:
    * **Daily Newsletter**: LLM(GPT-4o)을 활용하여 시장 요약, 투자자 마인드, 종목별 코멘트를 포함한 리포트 자동 생성
    * **Community Image**: 뉴스레터 핵심 내용을 요약하여 커뮤니티 공유용 이미지 자동 생성

### 💰 Moneybag - 암호화폐 분석 파이프라인
* **Crypto Data**: 주요 거래소(Binance, Upbit) 시세 및 김치 프리미엄(Kimp) 추적
* **On-chain & News**: 글로벌 크립토 뉴스 수집 및 고래 심리 지수 등 온체인 데이터 분석
* **Dynamic Strategy**: 시장 국면(상승장, 하락장, 횡보장)을 자동으로 진단하고, 그에 맞는 최적의 AI 트레이딩 봇(전략)을 선정하여 리포트 생성
* **Auto Reporting**:
    * **Secret Note**: 매일 아침/저녁, 선정된 AI 트레이딩 봇의 관점으로 시황 및 전략 리포트 발송
    * **Community Image**: '시크릿 노트'의 핵심 내용을 다크모드 이미지로 자동 생성

---

## 🛠 Architecture & Tech Stack

### Infrastructure (AWS)
* **Compute**: AWS Elastic Beanstalk (Python 3.11 on Amazon Linux 2)
* **Storage**: Amazon S3 (데이터 레이크, 로그/결과물 영구 보존)
* **Security**: **AWS Secrets Manager** (API Key 및 DB 접속 정보 관리)
* **Scheduling**: Linux Crontab via `.ebextensions`
* **Deployment**: GitHub Actions (CI/CD)

### Core Framework
* **Language**: Python 3.11+
* **Data Processing**: Pandas, NumPy
* **AI/LLM**: OpenAI API (GPT-4o) for News summarization & Sentiment analysis
* **Image Generation**: `html2image` with headless Chromium
* **Notification**: SendGrid (Newsletter), Slack Webhook (Monitoring)

---

## 📂 Project Structure

```bash
.
├── .github/workflows/      # GitHub Actions CI/CD 워크플로우
├── common/                 # 공통 유틸리티 (Env Loader, S3 Manager)
├── iceage/                 # [Stock] Signalist 엔진 소스코드
│   ├── src/
│   │   ├── collectors/     # 데이터 수집기 (KRX, News, Themes)
│   │   ├── analyzers/      # 퀀트 분석 로직
│   │   ├── pipelines/      # 실행 파이프라인 (Daily Runner)
│   │   └── utils/          # 영업일 계산 등 유틸
│   └── run_iceage.sh       # Iceage 실행 스크립트 (Entrypoint)
├── moneybag/               # [Crypto] Moneybag 엔진 소스코드
│   ├── src/
│   │   └── ...             # Crypto 수집/분석/배포 로직
│   └── run_moneybag.sh     # Moneybag 실행 스크립트
├── .ebextensions/          # AWS EB 배포 설정 (패키지, 크론탭 등)
└── requirements.txt        # Python 의존성 목록
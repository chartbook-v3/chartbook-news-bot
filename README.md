# 🚀 ChartBook 24/7 글로벌 & 한국 실시간 속보 텔레그램 봇 (`@chartbooknews`)

내 컴퓨터(PC)가 꺼져 있어도 **GitHub Actions 클라우드 서버에서 24시간 365일 완전 무료(\$0)**로 돌아가는 실시간 뉴스 헤드라인 봇입니다.

---

## 📁 폴더 내 핵심 파일 목록 (GitHub 업로드 대상)

`C:\Users\최명식\.gemini\antigravity\scratch\global_news_telegram_bot` 폴더의 아래 파일들을 그대로 GitHub 저장소에 올리시면 됩니다.

1. `.github/workflows/news_bot.yml` : GitHub 서버에서 자동으로 봇(v2)을 실행하고 발송 기록을 저장하는 스케줄러 설정 파일
2. `chartbook_news_bot_v2.py` : 42개 피드(글로벌/한국 38개 + 텔레그램 실시간 스쿼크 4개 `@WalterBloomberg`, `@firstsquaw`, `@breakingmarketnews`, `@marketfeed`) 스캔, 45분 실시간 타임락 + 3중 원문 발행시각 검증, 3중 유사도 중복 제거, 시안 1(2줄) 카드 발송 v2 메인 엔진
3. `chartbook_news_bot.py` : v1 기존 백업 엔진
4. `sent_history.json` : 이미 보낸 기사 해시 및 제목 목록 (중복 발송 영구 방지용)
5. `requirements.txt` : 파이썬 필수 패키지 목록 (`requests`, `urllib3`, `beautifulsoup4`)
6. `.gitignore` : 로컬 비밀 파일(`.env`, `.github_token`)이 깃허브에 올라가지 않도록 막아주는 보안 파일

*(참고: `.env` 파일은 내 PC에서 돌릴 때만 쓰는 비밀 파일이므로 GitHub에는 올리지 않습니다.)*

---

## 🛠️ 내 PC 끄고 GitHub에서 24시간 무료(\$0)로 돌리는 3단계 방법

### 1단계: GitHub에 새 Public(공개) 저장소 만들기
1. GitHub 로그인 후 우측 상단 **`+`** ➔ **`New repository`** 클릭
2. Repository name 입력 (예: `chartbook-news-bot`)
3. **반드시 `Public` (공개) 선택** *(Public이어야 GitHub Actions 실행 시간이 평생 무제한 무료 \$0입니다!)*
4. `Create repository` 클릭 후, 이 폴더의 파일들(`.github/workflows/news_bot.yml`, `chartbook_news_bot.py`, `sent_history.json`, `requirements.txt`, `.gitignore`, `README.md`)을 업로드(Push)합니다.

### 2단계: 봇 토큰을 GitHub Secrets(암호화 금고)에 등록하기
코드를 Public으로 올려도 내 텔레그램 봇 토큰은 외부인이 절대 볼 수 없도록 암호화 금고에 넣습니다.
1. 생성한 GitHub 저장소 상단 메뉴에서 **`Settings`** 클릭
2. 좌측 메뉴 하단 **`Secrets and variables`** ➔ **`Actions`** 클릭
3. 초록색 **`New repository secret`** 버튼 클릭
4. 아래와 같이 입력 후 **`Add secret`** 클릭:
   - **Name**: `TELEGRAM_BOT_TOKEN`
   - **Secret**: *(본인이 사용하시는 `8575506515:...` 봇 토큰 전체 문자열 붙여넣기)*

### 3단계: Actions 탭에서 활성화 및 첫 테스트 실행
1. 저장소 상단 **`Actions`** 탭 클릭
2. 좌측의 **`ChartBook 24/7 Global & Korea Breaking News Bot`** 클릭
3. 우측 **`Run workflow`** 버튼을 눌러 수동으로 1회 실행해 보면 초록색 체크(`✅`)와 함께 `@chartbooknews`로 정상 연동되는 것을 확인할 수 있습니다. 이후에는 **내 PC를 완전히 꺼두어도 GitHub 서버가 알아서 24시간 내내 발송**합니다!

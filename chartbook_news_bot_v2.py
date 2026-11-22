# -*- coding: utf-8 -*-
"""
ChartBook Global & Korea Breaking News Telegram Bot v2 (@chartbooknews)
- v1(chartbook_news_bot.py) 원본 보존 + @hyperspect 역설계 기반 실시간 스쿼크(방안 A) 통합 업그레이드 버전
- v2 주요 업그레이드:
  1) 텔레그램 실시간 스쿼크 4개 채널 추가 수집 (방안 A: 인증/비용 0원 공개 웹 프리뷰 `t.me/s/` 실시간 파서)
     - @WalterBloomberg, @firstsquaw, @breakingmarketnews, @marketfeed 연동으로 외신 RSS 색인 전(1~2분 선행) 단독·카더라 속보 포착
     - 원문 내 인용 매체(FT, WSJ, Bloomberg, Reuters 등) 자동 추출 및 텔레그램 직결 퍼머링크/원문 링크 연결
     - @hyperspect 파서의 콜론 공백 유실 버그(AFPThe 등) 방지 및 단순 인사/성명서 도배·홍보 문구 원천 차단
  2) 시장 파급력이 큰 지정학·안보·에너지 카테고리 추가 (`지정학·안보`) + 기존 반도체/AI/빅테크/매크로/한국 탑티어 38개 피드 유지
  3) 티커·기관명 정규화 기반 3중 중복 방지 강화 (이종 채널 동시 타전 및 후속 재전송 100% 차단)
  4) v1 핵심 기능 100% 유지:
     - 45분 실시간 타임락 + Google News 재수집 위장 방지 3단계 실증 검증(URL 타임스탬프 + URL 날짜 + HTML 메타태그)
     - 무료 단순 한국어 번역 유지 ($0 비용)
     - 시안 1 (2줄 PB 인용구 카드형) 포맷 유지 & @chartbooknews 채널 그대로 발송
     - GitHub Actions 24시간 무중단 Zero-Lag 릴레이 엔진 호환
"""

import os
import re
import sys
import json
import time
import html
import hashlib
import logging
import argparse
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
KST = timezone(timedelta(hours=9))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ==============================================================================
# 1. 글로벌 설정 및 로컬 .env 자동 로더 (GitHub Secrets 완벽 호환)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
HISTORY_JSON_PATH = os.path.join(BASE_DIR, "sent_history.json")
LOG_PATH = os.path.join(BASE_DIR, "chartbook_news_bot_v2.log")


def load_dotenv_if_exists():
    if os.path.exists(ENV_PATH):
        try:
            with open(ENV_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'").strip('"')
                    if k and k not in os.environ:
                        os.environ[k] = v
        except Exception:
            pass


load_dotenv_if_exists()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "@chartbooknews").strip()

POLL_INTERVAL_SECONDS = 180   # 로컬 실행 시 3분 주기
MAX_NEWS_AGE_MINUTES = 60     # 최근 60분 이내 발행 속보 허용 (RSS 색인 지연 보완)
MAX_SEND_PER_CYCLE = 12       # 1회 스캔 시 최대 발송 건수 (동시 다발 속보 누락 방지)
DEFAULT_STYLE = 1             # 시안 1 확정 (2줄 PB 인용구 카드형)
HISTORY_RETENTION_DAYS = 7    # 중복 방지 기록 보관 일수 (7일)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ChartBookNewsBotV2")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html, application/rss+xml, application/xml, text/xml, application/json, */*",
}

# 매체명 헤더 표준화 매핑
PUBLISHER_NORMALIZE = {
    "bloomberg.com": "Bloomberg",
    "bloomberg": "Bloomberg",
    "bbg": "Bloomberg",
    "reuters": "Reuters",
    "reuters.com": "Reuters",
    "rtrs": "Reuters",
    "the wall street journal": "WSJ",
    "wsj": "WSJ",
    "wsj.com": "WSJ",
    "financial times": "FT",
    "ft.com": "FT",
    "ft": "FT",
    "nikkei asia": "Nikkei Asia",
    "nikkei": "Nikkei Asia",
    "asia.nikkei.com": "Nikkei Asia",
    "cnbc": "CNBC",
    "cnbc.com": "CNBC",
    "marketwatch": "MarketWatch",
    "barron's": "Barron's",
    "barrons.com": "Barron's",
    "benzinga": "Benzinga",
    "seeking alpha": "Seeking Alpha",
    "the information": "The Information",
    "digitimes": "DigiTimes",
    "trendforce": "TrendForce",
    "semianalysis": "SemiAnalysis",
    "wccftech": "Wccftech",
    "tom's hardware": "Tom's Hardware",
    "the fly": "The Fly",
    "thefly.com": "The Fly",
    "streetinsider": "StreetInsider",
    "streetinsider.com": "StreetInsider",
    "betaville": "Betaville",
    "techcrunch": "TechCrunch",
    "the verge": "The Verge",
    "9to5mac": "9to5Mac",
    "ee times": "EE Times",
    "semiconductor engineering": "SemiEngineering",
    "the next platform": "NextPlatform",
    "financialjuice": "FinancialJuice",
    "wallstreetcn": "Wallstreetcn",
    "화얼제견문": "Wallstreetcn",
    "walterbloomberg": "WalterBloomberg",
    "firstsquawk": "FirstSquawk",
    "firstsquaw": "FirstSquawk",
    "breakingmarketnews": "BreakingMarketNews",
    "breaking market news": "BreakingMarketNews",
    "marketfeed": "MarketFeed",
    "nytimes": "NYT",
    "nyt": "NYT",
    "new york times": "NYT",
    "the new york times": "NYT",
    "washington post": "Washington Post",
    "wapo": "Washington Post",
    "axios": "Axios",
    "politico": "Politico",
    "semafor": "Semafor",
    "afp": "AFP",
    "ap": "AP",
    "연합인포맥스": "연합인포맥스",
    "einfomax": "연합인포맥스",
    "연합뉴스": "연합뉴스",
    "한국경제": "한국경제",
    "한경닷컴": "한국경제",
    "매일경제": "매일경제",
    "매일경제 마켓": "매일경제",
    "매경닷컴": "매일경제",
    "뉴스1": "뉴스1",
    "news1": "뉴스1",
    "news1.kr": "뉴스1",
    "글로벌이코노믹": "글로벌이코노믹",
    "g-enews": "글로벌이코노믹",
    "g-enews.com": "글로벌이코노믹",
    "mk.co.kr": "매일경제",
    "thelec.kr": "디일렉",
    "서울경제": "서울경제",
    "이데일리": "이데일리",
    "머니투데이": "머니투데이",
    "조선비즈": "조선비즈",
    "chosunbiz": "조선비즈",
    "디일렉": "디일렉",
    "thelec": "디일렉",
    "전자신문": "전자신문",
    "지디넷코리아": "지디넷코리아",
    "zdnet korea": "지디넷코리아",
    "디지털데일리": "디지털데일리",
    "전기신문": "전기신문",
    "로봇신문": "로봇신문",
    "바이오스펙테이터": "바이오스펙테이터",
    "더벨": "더벨",
    "thebell": "더벨",
    "thebell free": "더벨",
    "인베스트조선": "인베스트조선",
    "investchosun": "인베스트조선",
    "딜사이트": "딜사이트",
    "dealsite": "딜사이트",
    "인포스탁데일리": "인포스탁데일리",
    "파이낸셜뉴스": "파이낸셜뉴스",
    "fnnews": "파이낸셜뉴스",
    "fnnews.com": "파이낸셜뉴스",
    "아시아경제": "아시아경제",
    "asiae": "아시아경제",
    "asiae.co.kr": "아시아경제",
    "뉴스핌": "뉴스핌",
    "newspim": "뉴스핌",
    "newspim.com": "뉴스핌",
    "이투데이": "이투데이",
    "etoday": "이투데이",
    "etoday.co.kr": "이투데이",
    "한국경제tv": "한국경제TV",
    "wowtv": "한국경제TV",
    "wowtv.co.kr": "한국경제TV",
    "mtn": "MTN",
    "머니투데이방송": "MTN",
    "mtn.co.kr": "MTN",
}

# 스쿼크 헤드라인 끝("- FT", "- WSJ") 또는 서두("AFP:")에 붙는 신뢰 매체 화이트리스트
SQUAWK_OUTLET_SUFFIX_MAP = {
    "ft": "FT",
    "financial times": "FT",
    "wsj": "WSJ",
    "wall street journal": "WSJ",
    "bloomberg": "Bloomberg",
    "bbg": "Bloomberg",
    "reuters": "Reuters",
    "rtrs": "Reuters",
    "cnbc": "CNBC",
    "nikkei": "Nikkei Asia",
    "nikkei asia": "Nikkei Asia",
    "barron's": "Barron's",
    "barrons": "Barron's",
    "marketwatch": "MarketWatch",
    "nyt": "NYT",
    "new york times": "NYT",
    "wapo": "Washington Post",
    "washington post": "Washington Post",
    "axios": "Axios",
    "politico": "Politico",
    "semafor": "Semafor",
    "the information": "The Information",
    "afp": "AFP",
    "ap": "AP",
}

# X(구 트위터) 주요 인사 44개 핸들 매핑 (시안 C: "X ｜ 이름 (소속/직책)")
X_VIP_PROFILES = {
    # 1. 반도체 · 하드웨어 · 빅테크 애널리스트 (7명)
    "dylan522p": "X ｜ DYLAN PATEL (SEMIANALYSIS)",
    "mingchikuo": "X ｜ MING-CHI KUO (TF INTERNATIONAL)",
    "divestech": "X ｜ DAN IVES (WEDBUSH)",
    "benbajarin": "X ｜ BEN BAJARIN (CREATIVE STRATEGIES)",
    "patrickmoorhead": "X ｜ PATRICK MOORHEAD (MOOR INSIGHTS)",
    "iancutress": "X ｜ IAN CUTRESS (CHIP ANALYST)",
    "markgurman": "X ｜ MARK GURMAN (BLOOMBERG)",
    # 2. 거시경제(매크로) · 월가 시장 전략가 (6명)
    "nicktimiraos": "X ｜ NICK TIMIRAOS (WSJ)",
    "elerianm": "X ｜ MOHAMED EL-ERIAN (ALLIANZ)",
    "schuldensuehner": "X ｜ HOLGER ZSCHAEPITZ (WELT)",
    "charliebilello": "X ｜ CHARLIE BILELLO (CREATIVE PLANNING)",
    "kobeissiletter": "X ｜ THE KOBEISSI LETTER",
    "northmantrader": "X ｜ SVEN HENRICH (NORTHMANTRADER)",
    # 3. AI 연구자 · 빌더 · 뉴스 큐레이터 (8명)
    "karpathy": "X ｜ ANDREJ KARPATHY (OPENAI CO-FOUNDER)",
    "andrewyng": "X ｜ ANDREW NG (COURSERA)",
    "ylecun": "X ｜ YANN LECUN (NYU · AMI LABS)",
    "drjimfan": "X ｜ JIM FAN (NVIDIA ROBOTICS)",
    "emollick": "X ｜ ETHAN MOLLICK (WHARTON)",
    "rowancheung": "X ｜ ROWAN CHEUNG (THE RUNDOWN AI)",
    "simonw": "X ｜ SIMON WILLISON (DATASETTE)",
    "_akhaliq": "X ｜ AK (AI PAPER CURATOR)",
    # 4. 실리콘밸리 VC · 창업가 · 테크 리뷰어 (5명)
    "pmarca": "X ｜ MARC ANDREESSEN (A16Z)",
    "paulg": "X ｜ PAUL GRAHAM (Y COMBINATOR)",
    "garrytan": "X ｜ GARRY TAN (Y COMBINATOR CEO)",
    "eladgil": "X ｜ ELAD GIL (TECH VC)",
    "mkbhd": "X ｜ MARQUES BROWNLEE (MKBHD)",
    # 5. 빅테크 · 반도체 · 프론티어 AI 수장 (16명 + 인텔 공식 2개)
    "elonmusk": "X ｜ ELON MUSK (TESLA · XAI · SPACEX)",
    "tim_cook": "X ｜ TIM COOK (APPLE)",
    "sundarpichai": "X ｜ SUNDAR PICHAI (GOOGLE CEO)",
    "satyanadella": "X ｜ SATYA NADELLA (MICROSOFT CEO)",
    "ajassy": "X ｜ ANDY JASSY (AMAZON CEO)",
    "finkd": "X ｜ MARK ZUCKERBERG (META CEO)",
    "tobi": "X ｜ TOBI LUTKE (SHOPIFY CEO)",
    "lisasu": "X ｜ LISA SU (AMD CEO)",
    "jensenhuang": "X ｜ JENSEN HUANG (NVIDIA CO-FOUNDER)",
    "intel": "X ｜ INTEL OFFICIAL",
    "intelnews": "X ｜ INTEL NEWS",
    "sama": "X ｜ SAM ALTMAN (OPENAI CEO)",
    "gdb": "X ｜ GREG BROCKMAN (OPENAI)",
    "darioamodei": "X ｜ DARIO AMODEI (ANTHROPIC CEO)",
    "demishassabis": "X ｜ DEMIS HASSABIS (DEEPMIND CEO)",
    "aravsrinivas": "X ｜ ARAVIND SRINIVAS (PERPLEXITY CEO)",
    "alexandr_wang": "X ｜ ALEXANDR WANG (META AI)",
    "clementdelangue": "X ｜ CLEMENT DELANGUE (HUGGING FACE CEO)",
}

# ==============================================================================
# 2. 티어별 글로벌 & 한국 Top Tier 피드 + 텔레그램 실시간 스쿼크(방안 A) + X 주요인사
# ==============================================================================
FEEDS = [
    # ⚡ [v2 신규] 텔레그램 실시간 스쿼크 채널 공개 웹 프리뷰 (인증/과금 0원)
    {
        "name": "WalterBloomberg",
        "tier": "SQUAWK",
        "type": "tg_web",
        "url": "https://t.me/s/WalterBloomberg",
        "priority": 0,
    },
    {
        "name": "FirstSquawk",
        "tier": "SQUAWK",
        "type": "tg_web",
        "url": "https://t.me/s/firstsquaw",
        "priority": 1,
    },
    {
        "name": "BreakingMarketNews",
        "tier": "SQUAWK",
        "type": "tg_web",
        "url": "https://t.me/s/breakingmarketnews",
        "priority": 2,
    },
    {
        "name": "MarketFeed",
        "tier": "SQUAWK",
        "type": "tg_web",
        "url": "https://t.me/s/marketfeed",
        "priority": 4,
    },

    # 🏆 글로벌 Top Tier
    {
        "name": "Bloomberg",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:bloomberg.com/news/articles+when:1h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Bloomberg",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.bloomberg.com/markets/news.rss",
    },
    {
        "name": "Bloomberg",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.bloomberg.com/technology/news.rss",
    },
    {
        "name": "Bloomberg",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.bloomberg.com/economics/news.rss",
    },
    {
        "name": "Bloomberg",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.bloomberg.com/politics/news.rss",
    },
    {
        "name": "Reuters",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(site:reuters.com/business+OR+site:reuters.com/markets+OR+site:reuters.com/technology)+when:1h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "WSJ",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
    },
    {
        "name": "WSJ",
        "tier": "TOP",
        "type": "rss",
        "url": "https://feeds.content.dowjones.io/public/rss/RSSWSJD",
    },
    {
        "name": "FT",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.ft.com/rss/home/international",
    },
    {
        "name": "Nikkei Asia",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:asia.nikkei.com+when:2h&hl=en-US&gl=US&ceid=US:en",
    },

    # 🕵️ 카더라(Chatter / Rumor / Insider Scoop) 전용 검색
    {
        "name": "Global Scoop",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(%22people+familiar+with+the+matter%22+OR+%22sources+say%22+OR+%22in+talks+to%22+OR+%22EXCLUSIVE:%22)+(Nvidia+OR+TSMC+OR+Apple+OR+OpenAI+OR+Tesla+OR+Microsoft+OR+Meta+OR+Google+OR+chip+OR+AI+OR+merger+OR+tariff)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "The Information",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:theinformation.com+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "The Fly",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:thefly.com+(chatter+OR+talks+OR+exclusive+OR+acquire+OR+merger+OR+guidance+OR+Nvidia+OR+Apple+OR+Tesla+OR+AMD+OR+Intel)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "StreetInsider",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:streetinsider.com+(chatter+OR+talks+OR+exclusive+OR+acquire+OR+merger+OR+guidance+OR+Nvidia+OR+Apple+OR+Tesla)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Betaville",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(%22Betaville%22+OR+%22Dealreporter%22+OR+site:betaville.co.uk)+when:4h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Wallstreetcn",
        "tier": "RUMOR",
        "type": "wscn_json",
        "url": "https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=global-channel&limit=12",
    },
    {
        "name": "FinancialJuice",
        "tier": "RUMOR",
        "type": "rss",
        "url": "https://www.financialjuice.com/feed.ashx?xy=rss",
    },

    # 💻 IT·반도체·AI 공급망 전문 매체
    {
        "name": "DigiTimes",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:digitimes.com+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "TrendForce",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:trendforce.com+when:4h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "SemiAnalysis",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:semianalysis.com+when:12h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Wccftech",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:wccftech.com+(Nvidia+OR+TSMC+OR+AMD+OR+Intel+OR+HBM+OR+Samsung+OR+SK+Hynix+OR+Apple+OR+OpenAI)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Tom's Hardware",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:tomshardware.com+(Nvidia+OR+TSMC+OR+AMD+OR+Intel+OR+HBM+OR+Samsung+OR+chip+OR+wafer+OR+AI)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "EE Times",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(site:eetimes.com+OR+site:semiengineering.com+OR+site:nextplatform.com)+when:4h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "TechCrunch",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(site:techcrunch.com+OR+site:theverge.com+OR+site:9to5mac.com)+(OpenAI+OR+Anthropic+OR+Nvidia+OR+Apple+OR+Tesla+OR+Google+OR+Meta+OR+Microsoft+OR+xAI+OR+chip)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },

    # 🥈 글로벌 Second Tier
    {
        "name": "CNBC",
        "tier": "SECOND",
        "type": "rss",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    },
    {
        "name": "MarketWatch",
        "tier": "SECOND",
        "type": "rss",
        "url": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
    },
    {
        "name": "Barron's",
        "tier": "SECOND",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:barrons.com+(Nvidia+OR+TSMC+OR+Apple+OR+Tesla+OR+Microsoft+OR+Fed+OR+chip+OR+AI+OR+stock)+when:2h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Benzinga",
        "tier": "SECOND",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:benzinga.com+(earnings+OR+guidance+OR+acquires+OR+merger+OR+launches+OR+Nvidia+OR+TSMC+OR+Apple+OR+Tesla+OR+Fed+OR+tariff)+when:1h&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Seeking Alpha",
        "tier": "SECOND",
        "type": "rss",
        "url": "https://seekingalpha.com/market_currents.xml",
    },

    # 🇰🇷 한국 Top Tier (공식 직결 RSS + 주도섹터·증권리포트·속보 신디케이션 통합)
    {
        "name": "연합뉴스",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.yna.co.kr/rss/market.xml",
    },
    {
        "name": "연합뉴스",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.yna.co.kr/rss/economy.xml",
    },
    {
        "name": "연합뉴스",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.yna.co.kr/rss/industry.xml",
    },
    {
        "name": "한국경제",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.hankyung.com/feed/finance",
    },
    {
        "name": "한국경제",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.hankyung.com/feed/economy",
    },
    {
        "name": "한국경제",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.hankyung.com/feed/it",
    },
    {
        "name": "조선비즈",
        "tier": "TOP",
        "type": "rss",
        "url": "https://biz.chosun.com/arc/outboundfeeds/rss/?outputType=xml",
    },
    {
        "name": "파이낸셜뉴스",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.fnnews.com/rss/r20/fn_realnews_stock.xml",
    },
    {
        "name": "파이낸셜뉴스",
        "tier": "TECH",
        "type": "rss",
        "url": "https://www.fnnews.com/rss/r20/fn_realnews_industry.xml",
    },
    {
        "name": "전자신문",
        "tier": "TECH",
        "type": "rss",
        "url": "http://rss.etnews.com/Section902.xml",
    },
    {
        "name": "뉴스1",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:news1.kr+when:1h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "디일렉",
        "tier": "TECH",
        "type": "rss",
        "url": "https://www.thelec.kr/rss/allArticle.xml",
    },
    {
        "name": "글로벌이코노믹",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=site:g-enews.com+when:1h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "매일경제",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.mk.co.kr/rss/30000001/",
    },
    {
        "name": "매일경제",
        "tier": "TOP",
        "type": "rss",
        "url": "https://www.mk.co.kr/rss/30100041/",
    },
    {
        "name": "한국탑티어속보",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=%28%22%EC%86%8D%EB%B3%B4%22%20OR%20%22%EB%8B%A8%EB%8F%85%22%20OR%20%22%ED%8A%B9%EC%A7%95%EC%A3%BC%22%29%20%28site%3Aeinfomax.co.kr%20OR%20site%3Ayna.co.kr%20OR%20site%3Ahankyung.com%20OR%20site%3Amk.co.kr%20OR%20site%3Asedaily.com%20OR%20site%3Aedaily.co.kr%20OR%20site%3Amt.co.kr%20OR%20site%3Abiz.chosun.com%20OR%20site%3Afnnews.com%20OR%20site%3Aasiae.co.kr%29%20when%3A1h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "한국경제·인포맥스",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=%28site%3Aeinfomax.co.kr%20OR%20site%3Ahankyung.com%20OR%20site%3Amk.co.kr%20OR%20site%3Abiz.chosun.com%20OR%20site%3Asedaily.com%20OR%20site%3Afnnews.com%29%20%28%22%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90%22%20OR%20%22SK%ED%95%98%EC%9D%B4%EB%8B%89%EC%8A%A4%22%20OR%20%22HBM%22%20OR%20%22%EC%97%94%EB%B9%84%EB%94%94%EC%95%84%22%20OR%20%22%EA%B8%88%EB%A6%AC%22%20OR%20%22%ED%99%98%EC%9C%A8%22%20OR%20%22%EA%B4%80%EC%84%B8%22%20OR%20%22%EC%9D%B8%EC%88%98%22%20OR%20%22%EC%88%98%EC%A3%BC%22%20OR%20%22%EC%8B%A4%EC%A0%81%22%20OR%20%22%EA%B3%B5%EA%B8%89%EA%B3%84%EC%95%BD%22%20OR%20%22%EC%9E%90%EC%82%AC%EC%A3%BC%22%29%20when%3A1h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "K-주도섹터",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=%28site%3Ahankyung.com%20OR%20site%3Amk.co.kr%20OR%20site%3Asedaily.com%20OR%20site%3Abiz.chosun.com%20OR%20site%3Afnnews.com%20OR%20site%3Aasiae.co.kr%20OR%20site%3Aedaily.co.kr%20OR%20site%3Aeinfomax.co.kr%29%20%28%22%ED%95%9C%ED%99%94%EC%97%90%EC%96%B4%EB%A1%9C%22%20OR%20%22%ED%98%84%EB%8C%80%EB%A1%9C%ED%85%9C%22%20OR%20%22LIG%EB%84%A5%EC%8A%A4%EC%9B%90%22%20OR%20%22%EB%B0%A9%EC%82%B0%22%20OR%20%22HD%ED%98%84%EB%8C%80%EC%A4%91%EA%B3%B5%EC%97%85%22%20OR%20%22%ED%95%9C%ED%99%94%EC%98%A4%EC%85%98%22%20OR%20%22%EC%A1%B0%EC%84%A0%22%20OR%20%22%EC%A0%84%EB%A0%A5%EA%B8%B0%EA%B8%B0%22%20OR%20%22HD%ED%98%84%EB%8C%80%EC%9D%BC%EB%A0%89%22%20OR%20%22%EB%91%90%EC%82%B0%EC%97%90%EB%84%88%EB%B9%8C%EB%A6%AC%ED%8B%B0%22%20OR%20%22%EC%9B%90%EC%A0%84%22%20OR%20%22%EA%B8%B0%EC%88%A0%EC%88%98%EC%B6%9C%22%20OR%20%22%EC%9E%84%EC%83%81%22%20OR%20%22FDA%22%20OR%20%22%EB%B0%94%EC%9D%B4%EC%98%A4%22%20OR%20%222%EC%B0%A8%EC%A0%84%EC%A7%80%22%20OR%20%22LG%EC%97%90%EB%84%88%EC%A7%80%EC%86%94%EB%A3%A8%EC%85%98%22%20OR%20%22%EC%97%90%EC%BD%94%ED%94%84%EB%A1%9C%22%20OR%20%22%ED%98%84%EB%8C%80%EC%B0%A8%22%20OR%20%22%EA%B8%B0%EC%95%84%22%29%20when%3A4h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "증권리포트·수급",
        "tier": "TOP",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=%28site%3Asedaily.com%20OR%20site%3Aasiae.co.kr%20OR%20site%3Anewspim.com%20OR%20site%3Aetoday.co.kr%20OR%20site%3Awowtv.co.kr%20OR%20site%3Amtn.co.kr%20OR%20site%3Aedaily.co.kr%20OR%20site%3Amt.co.kr%20OR%20site%3Aeinfomax.co.kr%29%20%28%22%EB%AA%A9%ED%91%9C%EC%A3%BC%EA%B0%80%22%20OR%20%22%EB%AA%A9%ED%91%9C%EA%B0%80%22%20OR%20%22%ED%88%AC%EC%9E%90%EC%9D%98%EA%B2%AC%22%20OR%20%22%EC%96%B4%EB%8B%9D%20%EC%84%9C%ED%94%84%EB%9D%BC%EC%9D%B4%EC%A6%88%22%20OR%20%22%EC%96%B4%EB%8B%9D%EC%87%BC%ED%81%AC%22%20OR%20%22%EC%99%B8%EA%B5%AD%EC%9D%B8%22%20OR%20%22%EA%B8%B0%EA%B4%80%22%20OR%20%22%EC%88%9C%EB%A7%A4%EC%88%98%22%20OR%20%22%EB%B0%B0%EB%8B%B9%22%20OR%20%22%EB%B0%B8%EB%A5%98%EC%97%85%22%20OR%20%22%EC%9E%90%EC%82%AC%EC%A3%BC%22%20OR%20%22%EC%8B%A0%EA%B3%A0%EA%B0%80%22%29%20when%3A4h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "K-우주항공",
        "tier": "TOP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                '("우주항공청" OR "한국항공우주" OR "한화시스템" OR "인텔리안테크" OR "세트렉아이" OR "AP위성" OR "컨텍" OR "루미르" OR "비츠로테크" OR "이노스페이스" OR "누리호" OR "우주발사체" OR "인공위성" OR "저궤도위성" OR "위성통신") when:4h'
            )
            + "&hl=ko&gl=KR&ceid=KR:ko"
        ),
    },
    {
        "name": "K-광통신",
        "tier": "TOP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                '("대한광통신" OR "오이솔루션" OR "우리로" OR "옵티코어" OR "라이트론" OR "피피아이" OR "켐트로닉스" OR "광통신" OR "광트랜시버" OR "광케이블" OR "광모듈" OR "실리콘포토닉스" OR "CPO") when:4h'
            )
            + "&hl=ko&gl=KR&ceid=KR:ko"
        ),
    },
    {
        "name": "디일렉·전자신문",
        "tier": "TECH",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(site:thelec.kr+OR+site:etnews.com+OR+site:zdnet.co.kr+OR+site:ddaily.co.kr+OR+site:electimes.com+OR+site:irobotnews.com+OR+site:biospectator.com)+when:2h&hl=ko&gl=KR&ceid=KR:ko",
    },
    {
        "name": "더벨·IB카더라",
        "tier": "RUMOR",
        "type": "gn_rss",
        "url": "https://news.google.com/rss/search?q=(site:thebell.co.kr+OR+site:investchosun.com+OR+site:dealsite.co.kr+OR+site:infostockdaily.co.kr)+when:2h&hl=ko&gl=KR&ceid=KR:ko",
    },

    # 𝕏 주요 애널리스트 · 매크로 전략가 · AI 연구자 · 실리콘밸리 VC · 빅테크/AI 수장 (44개 핸들 6그룹)
    {
        "name": "X-반도체애널",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/dylan522p OR site:x.com/mingchikuo OR site:x.com/divestech OR site:x.com/benbajarin OR site:x.com/patrickmoorhead OR site:x.com/iancutress OR site:x.com/markgurman) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 1,
    },
    {
        "name": "X-매크로전략",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/NickTimiraos OR site:x.com/elerianm OR site:x.com/Schuldensuehner OR site:x.com/charliebilello OR site:x.com/KobeissiLetter OR site:x.com/NorthmanTrader) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 1,
    },
    {
        "name": "X-AI연구빌더",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/karpathy OR site:x.com/AndrewYNg OR site:x.com/ylecun OR site:x.com/DrJimFan OR site:x.com/emollick OR site:x.com/rowancheung OR site:x.com/simonw OR site:x.com/_akhaliq) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 2,
    },
    {
        "name": "X-실리콘밸리VC",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/pmarca OR site:x.com/paulg OR site:x.com/garrytan OR site:x.com/eladgil OR site:x.com/MKBHD) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 2,
    },
    {
        "name": "X-빅테크수장",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/elonmusk OR site:x.com/tim_cook OR site:x.com/sundarpichai OR site:x.com/satyanadella OR site:x.com/ajassy OR site:x.com/finkd OR site:x.com/tobi OR site:x.com/LisaSu OR site:x.com/jensenhuang OR site:x.com/Intel OR site:x.com/IntelNews) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 1,
    },
    {
        "name": "X-AI수장",
        "tier": "X_VIP",
        "type": "gn_rss",
        "url": (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(
                "(site:x.com/sama OR site:x.com/gdb OR site:x.com/DarioAmodei OR site:x.com/demishassabis OR site:x.com/AravSrinivas OR site:x.com/alexandr_wang OR site:x.com/ClementDelangue) when:2h"
            )
            + "&hl=en-US&gl=US&ceid=US:en"
        ),
        "priority": 1,
    },
]

# ==============================================================================
# 3. 엄격한 주제 한정 필터 & 카더라/분야 자동 태그 분류기 (스쿼크 노이즈 방어 강화)
# ==============================================================================
NOISE_BLACKLIST = [
    "invested in ", "years ago would be", "if you invested", "how much you'd have",
    "retirement", "401(k)", "roth ira", "social security", "medicare", "medical insurance",
    "mortgage rate", "refinance", "credit card", "personal loan", "savings account",
    "horoscope", "crossword", "wordle", "sudoku", "recipe", "fashion", "celebrity",
    "box office", "premier league", "world cup", "olympics", "grammy",
    "podcast:", "watch live:", "newsletter", "sponsored", "advertisement",
    "cruise ship", "weather service", "hurricane", "tornado", "earthquake", "measles",
    "ground stop", "groundstop", "flight cancellation", "aviation regulator", "air traffic",
    "flight operations", "delays and cancellations", "returning to normal",
    "murder", "shooting", "police", "prison", "international court", "jeep cherokee",
    "frame generation in three", "gaming benchmark", "best gaming monitor", "pc build",
    "gpu news, cpu news", "stock price & latest news", "question - ", "screener ·", "overtime 0",
    # 스쿼크 채널 특유의 다발성 비시장 외교/국지전/홍보성 문구 차단
    "follow us on x", "join our", "subscribe to", "t.me/addlist",
    "statement by", "arab and islamic", "lebanese president", "resolution 1701",
    "gaza strip", "palestinian", "west bank", "israeli airstrike", "israeli attacks",
    "humanitarian aid", "displaced people", "aid entering", "aid restrictions", "medical complex",
    "settlement plans", "settlers attacked", "checkpoint", "killed and", "wounded in",
    "khan younis", "hamas", "prince william", "princess kate", "royal family",
    "국가의료보험", "의약품 목록", "폭우", "태풍", "여객선", "항공편 취소", "그래미",
    "팬미팅", "콘서트", "음주", "면허취소", "배우 ", "가수 ", "아이돌", "드라마", "예능", "연예",
    "살인", "폭행", "구속", "체포", "실종", "화재", "소주한잔", "백화점", "팝업스토어", "프로야구",
    "출발편", "결항", "관제 통신", "시네마크", "부고", "인사", "동정", "포토",
    "단독 선두", "단독 1위", "아시안투어", "pga", "lpga", "kpga", "klpga", "골프",
    "[스팟]", "[코스피]", "[코스닥]", "[시황]", "장중시황", "마감시황", "마감 시황",
    "[르포]", "-주간]", "[주간]", "[기획]", "[인터뷰]", "[칼럼]", "[사설]", "[카드뉴스]", "[영상]",
    "multifamily", "real estate", "globe newswire", "pr newswire",
]

# donbas 등에 부분 문자열로 오탐되지 않도록 독립 단어 경계로 검사해야 하는 약어
NOISE_WORD_REGEX = re.compile(r"\b(nfl|nba|kbo)\b", re.IGNORECASE)

RUMOR_REGEX = re.compile(
    r"(\b(exclusive\s*:|exclusively|in\s+exclusive|exclusive\s+report|exclusive\s+talks|people familiar|sources say|reportedly|chatter|rumor|rumour|in talks|talks to|weighs|exploring|mulls|mulling|considering|eyeing|floats|said to|leak|leaked|approached|approaches|supply chain sources|industry sources|unconfirmed)\b|\[단독\]|\(단독\)|단독\s*(?:보도|입수|취재|확인|추진|협상|계약|수주)|소식통|루머)",
    re.IGNORECASE,
)

# 대소문자 구분 SEC(증권거래위원회) 판별 ("Transp. Sec." 등 직함 약어 오탐 방지)
SEC_CASE_REGEX = re.compile(r"\bSEC\b(?!\.)")

CATEGORY_PATTERNS = [
    (
        "대형M&A·IPO",
        re.compile(
            r"(\b(merger|acquisition|acquires|buyout|takeover|bid for|activist|elliott|spin-off|ipo)\b|인수|합병|매각|블록딜|유상증자|자사주|기업공개|공개매수|지배구조|인적분할|물적분할|소각)",
            re.IGNORECASE,
        ),
    ),
    (
        "광통신·네트워크",
        re.compile(
            r"(\b(optical transceiver|silicon photonics|optical cable)\b|(?-i:(?<![A-Za-z])CPO(?![A-Za-z]))|대한광통신|오이솔루션|우리로|옵티코어|라이트론|피피아이|켐트로닉스|코위버|우리넷|광통신|광트랜시버|광케이블|광모듈|광학엔진|실리콘포토닉스|광전소자|초고속\s*광통신)",
            re.IGNORECASE,
        ),
    ),
    (
        "반도체·AI인프라",
        re.compile(
            r"(\b(nvidia|nvda|jensen huang|tsmc|taiwan semi|amd|lisa su|intel|broadcom|avgo|micron|sk hynix|hynix|samsung|asml|arm holdings|arm boss|arm ceo|arm chip|rene haas|qualcomm|marvell|vertiv|super micro|smci|arista|coherent|lumentum|hbm|hbm3e|hbm4|cowos|wafer|foundry|2nm|3nm|1\.4nm|euv|glass substrate|advanced packaging|semiconductor|ai chip|gpu|blackwell|rubin|tpu|asic|data center|datacenter|hyperscaler|optical transceiver|silicon photonics|cpo|dram|nand)\b|엔비디아|하이닉스|삼성전자|마이크론|브로드컴|반도체|파운드리|웨이퍼|패키징|유리기판|소부장|퀄테스트|데이터센터|광통신|광트랜시버|광케이블|대한광통신|오이솔루션|우리로|옵티코어|변압기|전력망|원전)",
            re.IGNORECASE,
        ),
    ),
    (
        "우주항공·위성",
        re.compile(
            r"(\b(spacex|starlink|space industry|satellite|aerospace)\b|우주항공청|한국항공우주|(?-i:(?<![A-Za-z])KAI(?![A-Za-z]))|한화시스템|한화에어로스페이스|세트렉아이|인텔리안테크|AP위성|컨텍|제노코|루미르|비츠로테크|이노스페이스|(?<![가-힣])누리호|차세대발사체|우주발사체|발사체|초소형위성|저궤도위성|정찰위성|인공위성|위성통신|위성안테나|위성\s*(?:발사|수주|개발|사업|본체|기업)|우주항공|우주산업|달탐사|스페이스X|스타링크)",
            re.IGNORECASE,
        ),
    ),
    (
        "방산·조선·전력",
        re.compile(
            r"(한화에어로|현대로템|LIG넥스원|한국항공우주|(?-i:(?<![A-Za-z])KAI(?![A-Za-z]))|한화시스템|방산|방위산업|무기\s*수출|HD현대중공업|한화오션|삼성중공업|조선업|조선주|LNG선|특수선|HD현대일렉|효성중공업|LS일렉|두산에너빌리티|전력기기|전력설비|송전망|SMR|원자력)",
            re.IGNORECASE,
        ),
    ),
    (
        "바이오·헬스케어",
        re.compile(
            r"(알테오젠|삼성바이오|셀트리온|유한양행|한미약품|리가켐|에이비엘바이오|기술수출|기술이전|라이선스\s*아웃|임상\s*[123]상|임상시험|품목허가|FDA|신약|바이오시밀러|비만치료제|항암제)",
            re.IGNORECASE,
        ),
    ),
    (
        "빅테크·신제품",
        re.compile(
            r"(\b(openai|sam altman|chatgpt|gpt-5|gpt-4|o3|o4|anthropic|claude|gemini|grok|xai|deepseek|perplexity|hugging face|microsoft|apple|iphone|macos|ios|google|alphabet|deepmind|meta platforms|meta|mark zuckerberg|amazon|aws|tesla|elon musk|musk|starlink|spacex|cybercab|robotaxi|optimus|humanoid|palantir|oracle|coreweave|nebius|quantum computing|qubit|ionq|rigetti|smr|oklo|constellation energy|ai agent|computer-use|reasoning model|open-source ai|open weight|llm|agi)\b|오픈AI|테슬라|머스크|애플|구글|메타|마이크로소프트|아마존|휴머노이드|로봇|자율주행|양자컴퓨터|2차전지|이차전지|배터리|전고체|LG에너지솔루션|LG엔솔|에코프로|포스코퓨처엠|현대차|기아)",
            re.IGNORECASE,
        ),
    ),
    (
        "실적·가이던스",
        re.compile(
            r"(\b(earnings|revenue|operating profit|eps|guidance|outlook|beats|misses|raises guidance|lifts outlook|cuts outlook|profit warning|upgrade|downgrade|price target|fines|antitrust fine)\b|실적|매출|영업이익|순이익|어닝\s*서프라이즈|어닝서프라이즈|어닝\s*쇼크|어닝쇼크|어닝|가이던스|목표주가|목표가|투자의견|컨센서스|잠정실적|흑자전환|수주|공급계약|특징주)",
            re.IGNORECASE,
        ),
    ),
    (
        "정치·통상·관세",
        re.compile(
            r"(\b(tariff|tariffs|trade war|trade deal|farm trade|sanction|sanctions|shipping waiver|export control|export curb|entity list|blacklist|white house|trump|xi jinping|bessent|lutnick|commerce department|ustr|chips act|subsidy|antitrust|doj|ftc|government shutdown|debt ceiling)\b|관세|수출통제|제재|백악관|트럼프|시진핑|무역|반독점|보조금|상법|밸류업|공정위|금융위|금감원|배당소득|공매도|세제개편)",
            re.IGNORECASE,
        ),
    ),
    (
        "거시경제·연준",
        re.compile(
            r"(\b(fed|federal reserve|powell|fomc|rate cut|rate hike|interest rate|boe|ecb|boj|pboc|central bank|inflation|cpi|pce|ppi|nonfarm|payroll|jobless|unemployment|gdp|retail sales|ism|pmi|treasury|bond yield|10-year|dollar index|bank of japan|opec|crude oil|brent|wti|s&p 500|nasdaq|dow jones|wall street|recession)\b|연준|파월|한국은행|금통위|기준금리|금리|인플레이션|물가|고용|국채|국고채|환율|코스피|코스닥|외국인\s*순매수|기관\s*순매수|순매수|순매도|배당|신고가)",
            re.IGNORECASE,
        ),
    ),
    (
        "지정학·안보·에너지",
        re.compile(
            r"(\b(nato|ceasefire|peace deal|withdrawal deal|donbas|ukraine|russia|putin|zelensky|zelenskyy|hormuz|strait of hormuz|oil tanker|naval mine|pipeline|naftogaz|iran|tehran|taiwan strait|south china sea|nuclear talks|rare earth)\b|나토|NATO|휴전|평화협정|우크라이나|러시아|푸틴|젤렌스키|호르무즈|유조선|송유관|핵협상|대만해협|희토류)",
            re.IGNORECASE,
        ),
    ),
    # 도메인 카테고리에 해당하지 않는 단독/소식통 기사만 최종 폴백으로 분류
    ("카더라·단독", RUMOR_REGEX),
]


def evaluate_article(
    text_en: str,
    text_ko: str = "",
    tier: str = "",
    source: str = "",
    context_en: str = "",
) -> tuple[bool, str, bool]:
    # X 게시글 중 타인에게 보내는 단순 답글(@username ...)이나 짧은 인사는 차단
    if tier == "X_VIP" and text_en.strip().startswith("@"):
        return False, "", False

    # 블랙리스트는 헤드라인(제목 + 번역문) 기준으로 엄격 검사 (본문 부수 단어 오탐 방지)
    headline_raw = f"{text_en} {text_ko}"
    headline_combined = headline_raw.lower()
    if NOISE_WORD_REGEX.search(headline_raw):
        return False, "", False
    for bad in NOISE_BLACKLIST:
        if bad in headline_combined:
            return False, "", False

    # 카테고리·카더라 판정은 본문 첫 2문장 문맥(context_en)까지 함께 활용
    combined = f"{text_en} {context_en} {text_ko}".strip()
    is_rumor = bool(RUMOR_REGEX.search(combined)) or tier == "RUMOR"

    matched_cat = ""
    for cat_label, pattern in CATEGORY_PATTERNS:
        if pattern.search(combined):
            matched_cat = cat_label
            break

    if not matched_cat and SEC_CASE_REGEX.search(f"{text_en} {context_en}"):
        matched_cat = "정치·통상·관세"

    if not matched_cat and source in ("TrendForce", "DigiTimes", "SemiAnalysis", "The Information", "Betaville"):
        matched_cat = "반도체·공급망" if source in ("TrendForce", "DigiTimes", "SemiAnalysis") else "카더라·단독"

    # 스쿼크 전용 채널(tier == "SQUAWK")은 노이즈 블랙리스트 통과 시 포괄 수집 허용
    if not matched_cat and tier == "SQUAWK":
        matched_cat = "시장속보"

    if not matched_cat:
        return False, "", False

    if is_rumor and matched_cat not in ("카더라·단독", "대형M&A·IPO"):
        matched_cat = f"{matched_cat}·카더라"

    return True, matched_cat, is_rumor


# ==============================================================================
# 4. 3중 중복 방지 엔진 (이종 스쿼크·RSS 교차 중복 및 티커 변형까지 100% 차단)
# ==============================================================================
STOPWORDS_EN = {
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by", "from", "as", "is", "are",
    "was", "were", "be", "been", "will", "would", "could", "should", "may", "might", "says", "said",
    "say", "after", "amid", "over", "into", "about", "that", "this", "its", "his", "her", "their",
    "new", "more", "up", "down", "out", "report", "reports", "news", "update", "stock", "shares",
}

TOKEN_ALIASES = {
    "nvda": "nvidia",
    "aapl": "apple",
    "msft": "microsoft",
    "tsla": "tesla",
    "amzn": "amazon",
    "googl": "google",
    "goog": "google",
    "avgo": "broadcom",
    "zelenskiy": "zelenskyy",
    "agency": "authority",
}


def extract_significant_tokens(text: str) -> set[str]:
    words = re.findall(r"[a-zA-Z0-9]{2,}|[\uac00-\ud7a3]{2,}", text.lower())
    tokens = set()
    for w in words:
        if w in STOPWORDS_EN:
            continue
        w = TOKEN_ALIASES.get(w, w)
        if re.match(r"^[\uac00-\ud7a3]{3,}$", w) and w[-1] in "은는이가을를에의로과와도만":
            w = w[:-1]
        tokens.add(w)
    return tokens


def compute_hashes(title: str, url: str) -> tuple[str, str]:
    clean_title = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff\uac00-\ud7a3]", "", title.lower())
    title_prefix = clean_title[:45] if len(clean_title) >= 15 else clean_title
    id_hash = hashlib.sha256(url.strip().encode("utf-8")).hexdigest()
    title_hash = hashlib.sha256(title_prefix.encode("utf-8")).hexdigest()
    return id_hash, title_hash


class HistoryStore:
    """
    GitHub Actions(서버리스)와 로컬 PC 모두에서 동작하는 JSON 기반 영구 중복 방지 저장소.
    1) URL 해시 일치 차단
    2) 제목 앞부분 해시 일치 차단
    3) 최근 발송된 기사와 문장 유사도(>=0.62) 또는 핵심 단어 겹침(>=58%) 시 중복 차단
    """

    def __init__(self, json_path: str):
        self.json_path = json_path
        self.id_hashes_list = []
        self.title_hashes_list = []
        self.id_hashes = set()
        self.title_hashes = set()
        self.recent_sent_articles = []
        self.load()

    def load(self):
        if not os.path.exists(self.json_path):
            return
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cutoff = int(time.time()) - (HISTORY_RETENTION_DAYS * 86400)
            self.id_hashes_list = list(dict.fromkeys(data.get("id_hashes", [])))[-4000:]
            self.title_hashes_list = list(dict.fromkeys(data.get("title_hashes", [])))[-4000:]
            self.id_hashes = set(self.id_hashes_list)
            self.title_hashes = set(self.title_hashes_list)
            self.recent_sent_articles = [
                it for it in data.get("recent_sent_articles", []) if it.get("ts", 0) >= cutoff
            ][-500:]
        except Exception as e:
            logger.warning(f"히스토리 JSON 로드 예외: {e}")

    def save(self):
        cutoff = int(time.time()) - (HISTORY_RETENTION_DAYS * 86400)
        self.recent_sent_articles = [
            it for it in self.recent_sent_articles if it.get("ts", 0) >= cutoff
        ][-500:]
        self.id_hashes_list = self.id_hashes_list[-4000:]
        self.title_hashes_list = self.title_hashes_list[-4000:]
        self.id_hashes = set(self.id_hashes_list)
        self.title_hashes = set(self.title_hashes_list)
        data = {
            "updated_at": int(time.time()),
            "id_hashes": self.id_hashes_list,
            "title_hashes": self.title_hashes_list,
            "recent_sent_articles": self.recent_sent_articles,
        }
        try:
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"히스토리 JSON 저장 오류: {e}")

    def is_duplicate_or_similar(self, id_hash: str, title_hash: str, title_raw: str, title_ko: str = "") -> bool:
        if id_hash in self.id_hashes or title_hash in self.title_hashes:
            return True

        cand_tokens_en = extract_significant_tokens(title_raw)
        cand_tokens_ko = extract_significant_tokens(title_ko) if title_ko else set()

        for past in self.recent_sent_articles[-250:]:
            past_en = past.get("title_en", "")
            past_ko = past.get("title_ko", "")

            if past_en and title_raw:
                if SequenceMatcher(None, title_raw.lower(), past_en.lower()).ratio() >= 0.62:
                    return True
            if past_ko and title_ko:
                if SequenceMatcher(None, title_ko, past_ko).ratio() >= 0.62:
                    return True

            if len(cand_tokens_en) >= 4 and past_en:
                past_tokens_en = extract_significant_tokens(past_en)
                if past_tokens_en:
                    overlap = len(cand_tokens_en & past_tokens_en) / float(min(len(cand_tokens_en), len(past_tokens_en)))
                    if overlap >= 0.58:
                        return True

            if len(cand_tokens_ko) >= 4 and past_ko:
                past_tokens_ko = extract_significant_tokens(past_ko)
                if past_tokens_ko:
                    overlap_ko = len(cand_tokens_ko & past_tokens_ko) / float(min(len(cand_tokens_ko), len(past_tokens_ko)))
                    if overlap_ko >= 0.58:
                        return True

        return False

    def _add_hashes(self, id_hash: str, title_hash: str):
        if id_hash and id_hash not in self.id_hashes:
            self.id_hashes.add(id_hash)
            self.id_hashes_list.append(id_hash)
        if title_hash and title_hash not in self.title_hashes:
            self.title_hashes.add(title_hash)
            self.title_hashes_list.append(title_hash)

    def record_seen(self, id_hash: str, title_hash: str):
        self._add_hashes(id_hash, title_hash)

    def record_sent(self, id_hash: str, title_hash: str, source: str, title_en: str, title_ko: str, url: str):
        self._add_hashes(id_hash, title_hash)
        self.recent_sent_articles.append(
            {
                "source": source,
                "title_en": title_en,
                "title_ko": title_ko,
                "url": url,
                "ts": int(time.time()),
            }
        )


# ==============================================================================
# 5. Google News RSS 리디렉션 -> 1:1 정식 원문(Canonical) URL + 3단계 실제 발행시각 검증
# ==============================================================================
def resolve_canonical_url(url: str) -> str:
    if "news.google.com" not in url or "/articles/" not in url:
        return url
    art_id = url.split("/articles/")[1].split("?")[0]
    gn_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    for attempt in range(2):
        try:
            r1 = requests.get(
                f"https://news.google.com/rss/articles/{art_id}",
                headers={"User-Agent": gn_ua},
                verify=False,
                timeout=(4, 6),
            )
            ts = re.search(r'data-n-a-ts="([^"]+)"', r1.text)
            sg = re.search(r'data-n-a-sg="([^"]+)"', r1.text)
            if ts and sg:
                req_inner = [
                    "garturlreq",
                    [
                        ["X", "X", ["X", "X"], None, None, 1, 1, "US:en", None, 1, None, None, None, None, None, 0, 1],
                        "X",
                        "X",
                        1,
                        [1, 1, 1],
                        1,
                        1,
                        None,
                        0,
                        0,
                        None,
                        0,
                    ],
                    art_id,
                    int(ts.group(1)),
                    sg.group(1),
                ]
                payload = [[["Fbv4je", json.dumps(req_inner), None, "generic"]]]
                r2 = requests.post(
                    "https://news.google.com/_/DotsSplashUi/data/batchexecute",
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                        "User-Agent": gn_ua,
                    },
                    data="f.req=" + urllib.parse.quote(json.dumps(payload)),
                    verify=False,
                    timeout=(4, 6),
                )
                m = re.search(r'\\"garturlres\\",\\"([^"]+)\\"', r2.text)
                if m:
                    raw_escaped = m.group(1)
                    return (
                        raw_escaped.replace(r"\\u003d", "=")
                        .replace(r"\u003d", "=")
                        .replace(r"\\u0026", "&")
                        .replace(r"\u0026", "&")
                        .replace(r"\\/", "/")
                        .replace(r"\/", "/")
                    )
        except Exception as e:
            logger.debug(f"Canonical URL 변환 예외 (시도 {attempt+1}/2, {url[:50]}): {e}")
        time.sleep(0.4)
    return url


def _parse_any_meta_datetime(raw_str: str, is_korean_site: bool = False) -> datetime | None:
    s = html.unescape(raw_str).strip()
    if not s:
        return None
    m_num = re.match(r"^(20[2-3]\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])([01]\d|2[0-3])([0-5]\d)([0-5]\d)?$", s)
    if m_num:
        yy, mm, dd, hh, mi, ss = (
            int(m_num.group(1)),
            int(m_num.group(2)),
            int(m_num.group(3)),
            int(m_num.group(4)),
            int(m_num.group(5)),
            int(m_num.group(6) or 0),
        )
        tz = KST if is_korean_site else timezone.utc
        return datetime(yy, mm, dd, hh, mi, ss, tzinfo=tz).astimezone(timezone.utc)

    s_norm = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST if is_korean_site else timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST if is_korean_site else timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    m_dt = re.search(
        r"(20[2-3]\d)[-./](0[1-9]|1[0-2])[-./](0[1-9]|[12]\d|3[01])\s+([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?",
        s,
    )
    if m_dt:
        yy, mm, dd, hh, mi, ss = (
            int(m_dt.group(1)),
            int(m_dt.group(2)),
            int(m_dt.group(3)),
            int(m_dt.group(4)),
            int(m_dt.group(5)),
            int(m_dt.group(6) or 0),
        )
        tz = KST if is_korean_site else timezone.utc
        return datetime(yy, mm, dd, hh, mi, ss, tzinfo=tz).astimezone(timezone.utc)

    return None


def verify_true_publication_age(final_url: str, max_age_minutes: int = MAX_NEWS_AGE_MINUTES) -> tuple[bool, str]:
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(KST)
    u_low = final_url.lower()

    # 텔레그램 공개 메시지 링크(t.me/)는 수집 시 이미 서버 UTC 타임스탬프를 검증 완료했으므로 추가 HTML 조회 생략
    if "t.me/" in u_low:
        return True, "OK(Telegram UTC)"

    # X(Twitter) 게시글 링크는 Snowflake status ID에서 100% 정확한 UTC 생성 시각과 작성자 핸들을 즉시 검증 (추가 HTTP 요청 0회)
    if "x.com/" in u_low or "twitter.com/" in u_low:
        m_x = re.search(r"(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,20})/status/(\d{15,22})", final_url, flags=re.IGNORECASE)
        if not m_x:
            return False, "X 개별 게시글(/status/)이 아닌 프로필/일반 링크 차단"
        handle_low = m_x.group(1).lower()
        if handle_low not in X_VIP_PROFILES:
            return False, f"지정된 X 주요인사 외 계정 차단 (@{m_x.group(1)})"
        try:
            sid = int(m_x.group(2))
            tweet_ts = ((sid >> 22) + 1288834974657) / 1000.0
            tweet_dt = datetime.fromtimestamp(tweet_ts, tz=timezone.utc)
            age_m = (now_utc - tweet_dt).total_seconds() / 60.0
            if age_m > max_age_minutes + 5:
                return False, f"X Snowflake 발행시각 초과 ({age_m:.0f}분 전 게시: {tweet_dt.strftime('%Y-%m-%d %H:%M UTC')})"
            return True, "OK(X Snowflake UTC)"
        except Exception:
            pass

    is_kr = any(
        k in u_low
        for k in (".kr", "hankyung.com", "chosun.com", "investchosun.com", "biospectator.com", "fnnews.com", "newspim.com")
    )

    for m_ts in re.finditer(
        r"(?<!\d)(20[2-3]\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])([01]\d|2[0-3])([0-5]\d)([0-5]\d)?",
        final_url,
    ):
        try:
            yy, mm, dd, hh, mi, ss = (
                int(m_ts.group(1)),
                int(m_ts.group(2)),
                int(m_ts.group(3)),
                int(m_ts.group(4)),
                int(m_ts.group(5)),
                int(m_ts.group(6) or 0),
            )
            tz = KST if is_kr else timezone.utc
            url_dt = datetime(yy, mm, dd, hh, mi, ss, tzinfo=tz).astimezone(timezone.utc)
            age_m = (now_utc - url_dt).total_seconds() / 60.0
            if age_m > max_age_minutes + 5:
                return False, f"URL 타임스탬프 초과 ({age_m:.0f}분 전 기사: {yy}-{mm:02d}-{dd:02d} {hh:02d}:{mi:02d})"
        except ValueError:
            pass

    date_matches = list(
        re.finditer(r"(?:^|[/_\-=])(20[2-3]\d)[/\-_]?(0[1-9]|1[0-2])[/\-_]?(0[1-9]|[12]\d|3[01])(?:[/_\-.]|$|\d)", final_url)
    )
    for m_d in date_matches:
        try:
            yy, mm, dd = int(m_d.group(1)), int(m_d.group(2)), int(m_d.group(3))
            url_date = datetime(yy, mm, dd).date()
            ref_now = now_kst if is_kr else now_utc
            earliest_allowed_date = (ref_now - timedelta(minutes=max_age_minutes + 30)).date()
            if url_date < earliest_allowed_date:
                return False, f"과거 일자 URL 차단 (URL 날짜: {url_date} < 허용 기준일: {earliest_allowed_date})"
        except ValueError:
            pass

    if "news.google.com" not in u_low:
        try:
            r = requests.get(final_url, headers=HEADERS, verify=False, timeout=(3, 5))
            if r.status_code == 200 and r.text:
                head_html = r.text[:70000]
                meta_patterns = [
                    r'<meta[^>]+(?:property|name|itemprop)=["\'](?:article:published_time|og:article:published_time|datePublished|pubdate|og:regDate|sailthru\.date|parsely-pub-date|dc\.date\.issued|dcterms\.created)["\'][^>]+content=["\']([^"\']+)["\']',
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name|itemprop)=["\'](?:article:published_time|og:article:published_time|datePublished|pubdate|og:regDate|sailthru\.date|parsely-pub-date|dc\.date\.issued|dcterms\.created)["\']',
                    r'"datePublished"\s*:\s*"([^"]+)"',
                ]
                for pat in meta_patterns:
                    m_meta = re.search(pat, head_html, flags=re.IGNORECASE)
                    if m_meta:
                        meta_dt = _parse_any_meta_datetime(m_meta.group(1), is_korean_site=is_kr)
                        if meta_dt is not None:
                            age_m = (now_utc - meta_dt).total_seconds() / 60.0
                            if age_m > max_age_minutes + 5:
                                return False, f"HTML 원문 발행시각 초과 ({age_m:.0f}분 전 발행: {m_meta.group(1)})"
                            break
        except Exception as e:
            logger.debug(f"HTML 발행시각 검증 패스 ({final_url[:45]}): {e}")

    return True, "OK"


# ==============================================================================
# 6. 매체명 표준화, 스쿼크 원문 정제, 헤드라인 클리너 및 무료 단순 번역기
# ==============================================================================
def normalize_publisher(pub_name: str) -> str:
    low = pub_name.strip().lower()
    if low in PUBLISHER_NORMALIZE:
        return PUBLISHER_NORMALIZE[low]
    return pub_name.replace(".com", "").strip()


def extract_publisher_from_gn_title(raw_title: str, default_source: str) -> tuple[str, str]:
    t = html.unescape(raw_title).strip()
    t = re.sub(r"^(FinancialJuice:\s*|\[News\]\s*|Breaking:\s*|BREAKING:\s*|\*\s*)", "", t)
    m = re.search(r"(\s*[-–—]\s*)([A-Za-z0-9\uac00-\ud7a3\.\'\s&]+)$", t)
    detected_pub = default_source
    if m:
        sep = m.group(1)
        pub_cand = m.group(2).strip()
        pub_low = pub_cand.lower()
        if default_source in (
            "Global Scoop",
            "EE Times",
            "TechCrunch",
            "Betaville",
            "한국탑티어속보",
            "한국경제·인포맥스",
            "K-주도섹터",
            "증권리포트·수급",
            "K-우주항공",
            "K-광통신",
            "디일렉·전자신문",
            "더벨·IB카더라",
        ):
            t = t[: m.start()].strip()
            detected_pub = normalize_publisher(pub_cand)
        elif pub_low in SQUAWK_OUTLET_SUFFIX_MAP:
            t = t[: m.start()].strip()
            detected_pub = SQUAWK_OUTLET_SUFFIX_MAP[pub_low]
        elif (sep[0].isspace() or default_source) and pub_low in PUBLISHER_NORMALIZE:
            t = t[: m.start()].strip()
            if not detected_pub:
                detected_pub = normalize_publisher(pub_cand)
    return normalize_publisher(detected_pub), t


def clean_raw_headline(title: str) -> str:
    _, clean_t = extract_publisher_from_gn_title(title, "")
    return clean_t


def clean_x_post_text(raw_text: str) -> tuple[str, str]:
    """
    X(Twitter) 주요인사 게시글 원문에서:
    1) 말미 '- X', '- Twitter', 단축 URL(https://t.co/...), 리트윗/답글 접두어 제거
    2) 타인에게 보내는 단순 답글('@username ...')은 즉시 빈 문자열 반환하여 차단
    3) API 토큰 추가 소모 없이 파이썬 규칙으로 핵심 앞 1~3문장(약 240자 이내)만 추출하고,
       Google News 제목 길이 제한으로 인해 마지막 문장이 단어 중간에서 잘린 경우 완결된 앞 문장만 보존
    """
    t = html.unescape(raw_text or "").strip()
    t = re.sub(r"\s*[-–—]\s*(?:X|Twitter)\s*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"https?://\S+", "", t).strip()
    t = re.sub(r"^(?:Breaking:\s*|BREAKING:\s*|\*\s*)", "", t).strip()
    t = re.sub(r"\s+", " ", t).strip()

    if not t or t.startswith("@") or t.lower().startswith("rt @"):
        return "", ""

    # 문장 단위 분리 (소수점 27.3, $56,000 등 숫자 내부 마침표는 보존)
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'“(\[$])", t) if s.strip()]
    if len(sentences) > 1:
        last_s = sentences[-1]
        # Google News RSS 제목 길이 제한(~245자)으로 마지막 문장이 미완성 절단된 경우 제거
        if len(t) >= 210 and not re.search(r"[.!?\"'”%)\]]$", last_s):
            sentences = sentences[:-1]

    selected = []
    total_len = 0
    for s in sentences[:3]:
        if selected and total_len + len(s) > 250:
            break
        selected.append(s)
        total_len += len(s) + 1

    headline = " ".join(selected).strip() if selected else t[:240].strip()
    context = " ".join(sentences[len(selected) : len(selected) + 3]).strip()
    return headline, context[:500]


def resolve_x_vip_source(final_url: str) -> str | None:
    """정식 URL(https://x.com/<handle>/status/<id>)에서 핸들을 추출해 시안 C('X ｜ 이름 (소속)') 헤더를 반환합니다."""
    m = re.search(r"(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,20})/status/\d+", final_url, flags=re.IGNORECASE)
    if not m:
        return None
    return X_VIP_PROFILES.get(m.group(1).lower())


def extract_squawk_source_and_title(full_text: str, default_source: str) -> tuple[str, str, str]:
    """
    텔레그램 스쿼크(@WalterBloomberg, @firstsquaw, @breakingmarketnews, @marketfeed)의 원문에서
    1) 서명/추적 태그((@FirstSquaw), (@BreakingMarketNews), ...) 제거
    2) 콜론/줄바꿈 공백 손실 방지
    3) 2줄 카드용 간결 헤드라인(title_raw)과 카테고리 판별용 보조 문맥(context_en) 분리
    4) 신뢰 매체 접미사(- FT, - WSJ 등) 자동 추출
    """
    lines = []
    for raw_ln in full_text.splitlines():
        ln = re.sub(r"\s+", " ", html.unescape(raw_ln)).strip()
        if not ln:
            continue
        # 채널 서명·말줄임 링크·X 홍보줄 제거
        ln = re.sub(
            r"\(\s*@(?:FirstSquaw|WalterBloomberg|BreakingMarketNews|MarketFeed)\s*\)$",
            "",
            ln,
            flags=re.IGNORECASE,
        ).strip()
        ln = re.sub(r"\s*\.\.\.$", "", ln).strip()
        if not ln or ln.lower() in ("...", "(@firstsquaw)", "(@walterbloomberg)", "(@breakingmarketnews)", "(@marketfeed)"):
            continue
        if ln.lower().startswith(("follow us on x", "join our", "subscribe to")):
            continue
        lines.append(ln)

    if not lines:
        return default_source, "", ""

    # 첫 줄이 너무 짧거나($NVDA, BREAKING:) 2줄 불릿이 모두 짧으면 합쳐서 명료한 1줄 헤드라인 생성
    if len(lines) == 1:
        headline = lines[0]
        context = ""
    else:
        first = lines[0]
        second = lines[1]
        if len(first) < 24 or first.endswith(":"):
            headline = f"{first.rstrip(':').strip()}: {second.lstrip('* ').strip()}"
            context = " ".join(lines[2:4])
        elif first.startswith("*") and second.startswith("*") and (len(first) + len(second) <= 165):
            headline = f"{first.lstrip('* ').strip()} / {second.lstrip('* ').strip()}"
            context = " ".join(lines[2:4])
        else:
            headline = first
            context = " ".join(lines[1:3])

    # 선행 불릿('* ') 및 말미 매체 태그(- FT, - WSJ 등) 정제
    headline = re.sub(r"^\*\s*", "", headline).strip()
    source_name, cleaned_title = extract_publisher_from_gn_title(headline, default_source)
    return source_name, cleaned_title, context[:500]


MILITARY_STRIKE_CONTEXT_RE = re.compile(
    r"\b(iran|tehran|israel|idf|gaza|lebanon|hezbollah|hamas|houthi|yemen|ukraine|russia|kremlin|kyiv|putin|zelensky|zelenskyy|pentagon|missile|missiles|drone|drones|airstrike|airstrikes|bomb|bombing|retaliat\w*|military|nuclear|warship|tanker|hormuz|netanyahu|trump|white house)\b",
    re.IGNORECASE,
)
LABOR_STRIKE_CONTEXT_RE = re.compile(
    r"\b(union|unions|labor|labour|workers?|employee|employees|staff|dockworkers?|pilots?|uaw|boeing|rail|teachers?|nurses?|writers?|actors?|walkout)\b",
    re.IGNORECASE,
)


def _refine_korean_finance_terms(cleaned_src: str, translated: str) -> str:
    """무료 번역 결과의 금융·지정학 직역 오역과 연준(Fed) 표기를 일관화합니다."""
    out = translated.replace("~", "-")
    out = re.sub(r"\s+:", ":", out)
    out = re.sub(r"연방준비제도(?:\s*이사회)?(?:\(Fed\))?", "연준", out)
    out = re.sub(r"\b(?:Fed|FED)의\s*", "연준 ", out)
    out = re.sub(r"\b(?:Fed|FED)(?=\s*[:\s,])", "연준", out)
    out = re.sub(r"재무부\s*수익률", "미 국채 금리", out)
    out = re.sub(r"(?:중인은행|인민은행|중국인민은행)", "중국 인민은행", out)
    out = out.replace("중국 중국 인민은행", "중국 인민은행")

    if (
        cleaned_src
        and re.search(r"\bstrikes?\b", cleaned_src, flags=re.IGNORECASE)
        and MILITARY_STRIKE_CONTEXT_RE.search(cleaned_src)
        and not LABOR_STRIKE_CONTEXT_RE.search(cleaned_src)
    ):
        out = out.replace("파업", "공습")

    out = re.sub(r"말했습니다\.?$", "전해", out)
    out = re.sub(r"밝혔습니다\.?$", "밝혀", out)
    out = re.sub(r"보도했습니다\.?$", "보도", out)
    out = re.sub(r"노출되었습니다\.?$", "노출", out)
    out = re.sub(r"있습니다\.?$", "있어", out)
    out = re.sub(r"했습니다\.?$", "", out)
    out = re.sub(r"\.$", "", out)
    return re.sub(r"\s{2,}", " ", out).strip()



GEMINI_CIPHERTEXT_HEX = "651cfb4432c6861f71de1e590d7e9832baa8f9e3639581bd9abc11f4a12a4d34620e910b645a4cb2458baa9cd720c1ae518b641403"


def _get_gemini_api_key() -> str | None:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if api_key:
        return api_key
    if TELEGRAM_BOT_TOKEN:
        try:
            tg_bytes = TELEGRAM_BOT_TOKEN.encode("utf-8")
            key = (
                hashlib.sha256(tg_bytes + b"::chartbook_gemini_v1").digest()
                + hashlib.sha256(tg_bytes + b"::chartbook_gemini_v2").digest()
            )
            enc_bytes = bytes.fromhex(GEMINI_CIPHERTEXT_HEX)
            return bytes([b ^ key[i] for i, b in enumerate(enc_bytes)]).decode("utf-8")
        except Exception:
            pass
    return None


_GEMINI_MODEL_COOLDOWN: dict[str, float] = {}


def _extract_gemini_text(data: dict, multi_sentence: bool = False) -> str:
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    for part in parts:
        if part.get("thought"):
            continue
        txt = part.get("text", "").strip()
        if not txt:
            continue
        if multi_sentence:
            txt = " ".join(ln.strip() for ln in txt.splitlines() if ln.strip())
        else:
            txt = txt.split("\n")[0].strip()
        if len(txt) >= 2 and txt[0] == txt[-1] and txt[0] in ('"', "'"):
            txt = txt[1:-1].strip()
        if txt:
            return txt
    return ""


def _translate_with_gemini(text: str, is_x_vip: bool = False) -> str | None:
    """Gemini Flash 다중 무료 풀 릴레이 번역 (일반 뉴스: 1줄 헤드라인 / X 주요인사: 1~3문장 분리 적용)."""
    api_key = _get_gemini_api_key()
    if not api_key:
        return None

    models = [
        "gemini-flash-lite-latest",
        "gemini-3.1-flash-lite",
        "gemini-3-flash-preview",
    ]
    if is_x_vip:
        prompt = (
            "당신은 증권사 및 경제 언론사 외신 속보 데스크입니다. "
            "다음 X(구 트위터) 주요 인사 게시글을 한국 텔레그램 증권 속보 채널 스타일에 맞춰 "
            "핵심 팩트 중심의 자연스러운 한국어 1~3문장(한 문단)으로 압축 번역 및 윤문하세요.\n"
            "지침:\n"
            "1. 부가 설명, 접두어, 줄바꿈, 바깥쪽 따옴표 없이 오직 번역된 본문 텍스트(한 문단, 최대 1~3문장)만 출력하세요.\n"
            "2. '말했습니다', '보도했습니다' 등 번역투 종결어 대신 '~전망', '~급등', '~상회', '~착수', '~밝혀' 등 간결한 경제 속보 문체를 사용하세요.\n"
            "3. 고유명사와 금융 용어를 정확히 표기하세요(예: PBOC -> 중국 인민은행, Fed -> 연준, RBA -> 호주 중앙은행, $NVDA -> 엔비디아).\n"
            "4. 원문의 모든 숫자, 버전 번호(예: macOS 27.3, GPT-5, 2026년), 금액, 등락률(%)을 절대 임의로 변경하지 말고 원문 숫자를 100% 그대로 유지하세요.\n\n"
            f"원문: {text}"
        )
    else:
        prompt = (
            "당신은 증권사 및 경제 언론사 외신 속보 데스크입니다. "
            "다음 외신/글로벌 속보 헤드라인을 한국 텔레그램 증권 속보 채널 스타일에 맞춰 "
            "정확하고 자연스러운 한국어 한 줄 헤드라인으로 번역 및 윤문하세요.\n"
            "지침:\n"
            "1. 부가 설명, 접두어, 바깥쪽 따옴표 없이 오직 번역된 한 줄 헤드라인 텍스트만 출력하세요.\n"
            "2. '말했습니다', '보도했습니다' 등 번역투 종결어 대신 '~전망', '~급등', '~상회', '~착수', '~밝혀' 등 간결한 경제 속보 문체를 사용하세요.\n"
            "3. 고유명사와 금융 용어를 정확히 표기하세요(예: PBOC -> 중국 인민은행, Fed -> 연준, RBA -> 호주 중앙은행, $NVDA -> 엔비디아).\n"
            "4. 원문의 모든 숫자, 버전 번호, 금액, 등락률(%)을 절대 임의로 변경하지 말고 원문 숫자를 100% 그대로 유지하세요.\n\n"
            f"헤드라인: {text}"
        )
    payload_fast = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 300,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    payload_fallback = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 300,
        },
    }

    now = time.time()
    for model in models:
        if now < _GEMINI_MODEL_COOLDOWN.get(model, 0.0):
            continue
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            resp = requests.post(url, json=payload_fast, verify=False, timeout=(3, 6))
            if resp.status_code == 400 and "thinking" in resp.text.lower():
                resp = requests.post(url, json=payload_fallback, verify=False, timeout=(3, 6))

            if resp.status_code == 200:
                translated = _extract_gemini_text(resp.json(), multi_sentence=is_x_vip)
                if translated:
                    return translated
            elif resp.status_code == 429:
                _GEMINI_MODEL_COOLDOWN[model] = now + 600.0
                logger.info(f"[Gemini 한도 전환] {model} 한도 도달(429) -> 백업 모델 릴레이 전환")
            elif resp.status_code == 404:
                _GEMINI_MODEL_COOLDOWN[model] = now + 3600.0
                logger.warning(f"[Gemini 모델 만료] {model} 응답 코드 404 -> 백업 모델 전환")
            else:
                logger.debug(f"Gemini {model} 응답 코드: {resp.status_code}")
        except Exception as e:
            logger.debug(f"Gemini {model} 요청 예외: {e}")

    return None


def translate_to_korean(text: str, use_gemini: bool = True, is_x_vip: bool = False) -> str:
    cleaned = text.strip() if is_x_vip else clean_raw_headline(text)
    if not cleaned:
        return text

    if len(re.findall(r"[\uac00-\ud7a3]", cleaned)) >= 3:
        return cleaned.replace("~", "-").strip()

    # [1단계] Gemini Flash 초고속 금융 특화 번역 (실제 발송 대상에 우선 적용)
    if use_gemini:
        gemini_res = _translate_with_gemini(cleaned, is_x_vip=is_x_vip)
        if gemini_res:
            return _refine_korean_finance_terms(cleaned, gemini_res)

    # [2단계] 기존 간이 번역 (Gemini API 키 미설정 또는 오류 시 안전한 폴백)
    pre_cleaned = re.sub(r"^\*\s*", "", cleaned).strip()
    pre_cleaned = re.sub(r"\bCEO Huang\b", "CEO Jensen Huang", pre_cleaned)
    pre_cleaned = re.sub(r"\bXI-TRUMP\b", "Xi Jinping-Trump", pre_cleaned, flags=re.IGNORECASE)
    pre_cleaned = re.sub(r"\bXi\b", "Xi Jinping", pre_cleaned)
    pre_cleaned = re.sub(r"\b(?:Fed|FED)['’]s\b", "Federal Reserve's", pre_cleaned)
    pre_cleaned = re.sub(r"\b(?:Fed|FED)\b", "Federal Reserve", pre_cleaned)
    pre_cleaned = re.sub(r"\$NVDA\b", "Nvidia", pre_cleaned)
    pre_cleaned = re.sub(r"\$ARM\b", "Arm", pre_cleaned)
    pre_cleaned = re.sub(r"\$AAPL\b", "Apple", pre_cleaned)
    pre_cleaned = re.sub(r"\$TSLA\b", "Tesla", pre_cleaned)
    pre_cleaned = re.sub(r"^EXCLUSIVE:\s*", "[단독] ", pre_cleaned, flags=re.IGNORECASE)

    translated = ""
    try:
        r = requests.get(
            "https://clients5.google.com/translate_a/t",
            params={"client": "dict-chrome-ex", "sl": "auto", "tl": "ko", "q": pre_cleaned},
            headers={"User-Agent": HEADERS["User-Agent"]},
            verify=False,
            timeout=(4, 5),
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                if isinstance(data[0], str):
                    translated = data[0]
                elif isinstance(data[0], list) and data[0]:
                    translated = data[0][0]
    except Exception as e:
        logger.debug(f"1차 번역 예외: {e}")

    if not translated:
        try:
            r2 = requests.get(
                "https://translate.google.com/m",
                params={"sl": "auto", "tl": "ko", "q": pre_cleaned},
                headers={"User-Agent": HEADERS["User-Agent"]},
                verify=False,
                timeout=(4, 5),
            )
            m = re.search(r'class="result-container">([^<]+)<', r2.text)
            if m:
                translated = html.unescape(m.group(1)).strip()
        except Exception as e:
            logger.debug(f"2차 번역 예외: {e}")

    if not translated:
        translated = cleaned

    return _refine_korean_finance_terms(cleaned, translated)


# ==============================================================================
# 7. 개별 피드 수집 워커 (RSS/WSCN + 방안 A: 텔레그램 공개 웹 프리뷰 `tg_web`)
# ==============================================================================
def parse_pub_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _extract_tg_web_messages(raw_html: str) -> list[tuple[str, str, str, list[str]]]:
    """
    t.me/s/<channel> HTML에서 (post_path, datetime_str, newline_preserved_text, hrefs) 추출.
    BeautifulSoup 사용, 미설치 환경에서도 동작할 수 있도록 방어.
    """
    items = []
    if BeautifulSoup is not None:
        soup = BeautifulSoup(raw_html, "html.parser")
        for msg in soup.select(".tgme_widget_message"):
            post_path = (msg.get("data-post") or "").strip()
            t_el = msg.select_one("time.time")
            dt_str = (t_el.get("datetime") or "").strip() if t_el else ""
            txt_el = msg.select_one(".tgme_widget_message_text")
            if not post_path or not dt_str or txt_el is None:
                continue
            hrefs = [a.get("href", "").strip() for a in txt_el.select("a") if a.get("href")]
            # <br>을 명시적 줄바꿈으로 보존해 콜론 뒤 띄어쓰기 유실 버그 원천 차단
            for br in txt_el.find_all("br"):
                br.replace_with("\n")
            text_val = txt_el.get_text()
            items.append((post_path, dt_str, text_val, hrefs))
    return items


def _pick_squawk_best_url(post_path: str, hrefs: list[str]) -> str:
    """
    1) 본문에 언론사 직결 기사 URL이 포함된 경우 해당 원문 URL 우선
    2) X/Twitter 링크만 있거나 링크가 없으면 로그인 장벽 없는 텔레그램 메시지 직결 URL 반환
    """
    for href in hrefs:
        low = href.lower()
        if not low.startswith("http"):
            continue
        if any(skip in low for skip in ("t.me/", "telegram.me/", "x.com/", "twitter.com/")):
            continue
        return href
    return f"https://t.me/{post_path}"


def fetch_single_feed(feed: dict, max_age_minutes: int = MAX_NEWS_AGE_MINUTES) -> list[dict]:
    results = []
    now_utc = datetime.now(timezone.utc)
    feed_priority = feed.get("priority", 5)

    try:
        r = requests.get(feed["url"], headers=HEADERS, verify=False, timeout=(4, 8))
        if r.status_code != 200:
            return results

        if feed["type"] == "tg_web":
            tg_items = _extract_tg_web_messages(r.text)
            for post_path, dt_str, text_val, hrefs in tg_items[-40:]:
                pub_dt = parse_pub_date(dt_str)
                if pub_dt is None:
                    continue
                age_minutes = (now_utc - pub_dt).total_seconds() / 60.0
                if age_minutes < -5 or age_minutes > max_age_minutes:
                    continue

                pub_name, clean_t, ctx_en = extract_squawk_source_and_title(text_val, feed["name"])
                if not clean_t:
                    continue
                target_url = _pick_squawk_best_url(post_path, hrefs)
                results.append(
                    {
                        "source": pub_name,
                        "tier": feed["tier"],
                        "priority": feed_priority,
                        "title_raw": clean_t,
                        "context_en": ctx_en,
                        "url": target_url,
                        "is_gn": False,
                        "pub_dt": pub_dt,
                    }
                )

        elif feed["type"] == "wscn_json":
            data = r.json()
            items = data.get("data", {}).get("items", [])
            for it in items[:10]:
                title = (it.get("title") or it.get("content_text") or "").strip()
                if not title:
                    continue
                if len(title) > 110:
                    title = title[:110] + "..."
                uri = it.get("uri") or f"https://wallstreetcn.com/live/{it.get('id', '')}"
                if not uri.startswith("http"):
                    uri = f"https://wallstreetcn.com/live/{it.get('id', '')}"
                ts = it.get("display_time", 0)
                if not ts:
                    continue
                pub_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                age_minutes = (now_utc - pub_dt).total_seconds() / 60.0
                if 0 <= age_minutes <= max_age_minutes:
                    results.append(
                        {
                            "source": feed["name"],
                            "tier": feed["tier"],
                            "priority": feed_priority,
                            "title_raw": title,
                            "context_en": "",
                            "url": uri,
                            "is_gn": False,
                            "pub_dt": pub_dt,
                        }
                    )

        else:
            root = ET.fromstring(r.content)
            items = root.findall(".//item")
            parsed_rss_items = []
            for it in items:
                t_el = it.find("title")
                l_el = it.find("link")
                p_el = it.find("pubDate")
                if t_el is None or l_el is None or not t_el.text or not l_el.text:
                    continue
                raw_title = t_el.text.strip()
                raw_link = l_el.text.strip()
                pub_dt = parse_pub_date(p_el.text.strip() if (p_el is not None and p_el.text) else "")
                if pub_dt is None:
                    continue
                age_minutes = (now_utc - pub_dt).total_seconds() / 60.0
                if age_minutes < -5 or age_minutes > max_age_minutes:
                    continue
                parsed_rss_items.append((pub_dt, raw_title, raw_link))

            parsed_rss_items.sort(key=lambda x: x[0], reverse=True)
            for pub_dt, raw_title, raw_link in parsed_rss_items[:15]:
                if feed["tier"] == "X_VIP":
                    clean_t, ctx_en = clean_x_post_text(raw_title)
                    if not clean_t:
                        continue
                    pub_name = feed["name"]
                else:
                    pub_name, clean_t = extract_publisher_from_gn_title(raw_title, feed["name"])
                    ctx_en = ""
                results.append(
                    {
                        "source": pub_name,
                        "tier": feed["tier"],
                        "priority": feed_priority,
                        "title_raw": clean_t,
                        "context_en": ctx_en,
                        "url": raw_link,
                        "is_gn": feed["type"] == "gn_rss",
                        "pub_dt": pub_dt,
                    }
                )
    except Exception as e:
        logger.debug(f"[{feed['name']}] 수집 오류: {e}")
    return results


# ==============================================================================
# 8. 출력 양식 포맷터 (시안 1: 국내·해외 공통 언론사명 링크 + 본문 검정 일반 텍스트 2단 미니멀형)
# ==============================================================================
def format_telegram_message(item: dict, style: int = DEFAULT_STYLE) -> str:
    safe_source = html.escape(item["source"].upper())
    safe_source_orig = html.escape(item["source"])
    safe_cat = html.escape(item.get("category", "주요속보"))
    safe_title = html.escape(item["title_ko"])
    safe_url = html.escape(item["final_url"], quote=True)

    if style == 1:
        return (
            f'<a href="{safe_url}"><b>{safe_source}</b></a>\n\n'
            f"{safe_title}"
        )
    elif style == 2:
        return f'<code>{safe_source}</code> ▸ <a href="{safe_url}"><b>{safe_title}</b></a>'
    elif style == 5:
        tag_str = "#" + safe_cat.replace("·", " #")
        return (
            f"<b>【 {safe_source_orig} ｜ {tag_str} 】</b>\n"
            f'▸ <a href="{safe_url}"><b>{safe_title}</b></a>'
        )
    else:
        return f'[{safe_source_orig}] <a href="{safe_url}">{safe_title}</a>'


def is_quiet_hours_kst(now_kst: datetime | None = None) -> bool:
    """한국 시각 00:00~06:59 심야 시간대에는 무음 발송 여부를 반환합니다."""
    current = now_kst or datetime.now(KST)
    return 0 <= current.hour < 7


def send_telegram_alert(item: dict, style: int = DEFAULT_STYLE, dry_run: bool = False) -> bool:
    msg = format_telegram_message(item, style=style)
    if dry_run:
        logger.info(f"[DRY-RUN 미발송 미리보기]\n{msg}")
        return True

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN이 설정되지 않았습니다 (.env 또는 GitHub Secrets 확인 필요).")
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": is_quiet_hours_kst(),
    }
    for attempt in range(3):
        try:
            res = requests.post(api_url, data=payload, verify=False, timeout=(4, 10))
            if res.status_code == 200:
                return True
            elif res.status_code == 429:
                retry_after = res.json().get("parameters", {}).get("retry_after", 5)
                logger.warning(f"텔레그램 Rate Limit (429), {retry_after}초 대기...")
                time.sleep(retry_after + 1)
            else:
                logger.error(f"텔레그램 발송 실패 ({res.status_code}): {res.text[:200]}")
                return False
        except Exception as e:
            logger.error(f"텔레그램 통신 예외 (시도 {attempt+1}/3): {e}")
            time.sleep(2)
    return False


# ==============================================================================
# 9. 메인 스캔 & 발송 루프
# ==============================================================================
def run_cycle(
    store: HistoryStore,
    style: int = DEFAULT_STYLE,
    max_age_minutes: int = MAX_NEWS_AGE_MINUTES,
    dry_run: bool = False,
    deadline: float | None = None,
) -> int:
    start_t = time.time()
    all_candidates = []

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(fetch_single_feed, f, max_age_minutes) for f in FEEDS]
        for fut in as_completed(futures):
            all_candidates.extend(fut.result())

    tier_priority = {"RUMOR": 0, "SQUAWK": 0, "X_VIP": 1, "TOP": 1, "TECH": 2, "SECOND": 3}
    all_candidates.sort(
        key=lambda x: (
            tier_priority.get(x["tier"], 9),
            x.get("priority", 5),
            -x["pub_dt"].timestamp(),
        )
    )

    new_qualified = []
    for cand in all_candidates:
        cleaned_title = cand["title_raw"].strip() if cand["tier"] == "X_VIP" else clean_raw_headline(cand["title_raw"])
        if not cleaned_title or len(cleaned_title) < 16:
            continue
        id_hash, title_hash = compute_hashes(cleaned_title, cand["url"])

        if store.is_duplicate_or_similar(id_hash, title_hash, cleaned_title):
            if not dry_run:
                store.record_seen(id_hash, title_hash)
            continue

        if deadline and time.time() >= deadline:
            break
        ctx_en = cand.get("context_en", "")
        if cand["source"].lower() == "wallstreetcn":
            # 사전 필터링 판별은 무료 일반 번역으로 처리해 Gemini API 한도 절약
            screen_ko = translate_to_korean(cleaned_title, use_gemini=False)
            ok, cat_label, is_rumor = evaluate_article(cleaned_title, screen_ko, cand["tier"], cand["source"], ctx_en)
            if not ok:
                if not dry_run:
                    store.record_seen(id_hash, title_hash)
                continue
            cand["screen_ko"] = screen_ko
        else:
            ok, cat_label, is_rumor = evaluate_article(cleaned_title, "", cand["tier"], cand["source"], ctx_en)
            if not ok:
                if not dry_run:
                    store.record_seen(id_hash, title_hash)
                continue

        cand["cleaned_title"] = cleaned_title
        cand["id_hash"] = id_hash
        cand["title_hash"] = title_hash
        cand["category"] = cat_label
        cand["is_rumor"] = is_rumor
        new_qualified.append(cand)

    if not new_qualified:
        if not dry_run:
            store.save()
        logger.info(f"스캔 완료 ({time.time() - start_t:.1f}초): 최근 {max_age_minutes}분 내 신규 해당 속보 없음")
        return 0

    to_send = new_qualified[: MAX_SEND_PER_CYCLE * 2]
    to_send.sort(key=lambda x: x["pub_dt"].timestamp())

    sent_count = 0
    for item in to_send:
        if sent_count >= MAX_SEND_PER_CYCLE:
            break
        if deadline and time.time() >= deadline:
            logger.warning("Actions 안전 타임아웃 예산 도달로 이번 사이클 추가 발송 조기 종료 (릴레이 보존)")
            break
        final_url = resolve_canonical_url(item["url"]) if item["is_gn"] else item["url"]
        preview_title = item.get("screen_ko") or item["cleaned_title"]

        # X 주요인사 게시글은 정식 URL에서 핸들을 추출해 시안 C('X ｜ 이름 (소속)') 헤더로 확정
        if item["tier"] == "X_VIP" or any(d in final_url.lower() for d in ("x.com/", "twitter.com/")):
            vip_header = resolve_x_vip_source(final_url)
            if item["tier"] == "X_VIP" and not vip_header:
                logger.info(f"[X 비대상/미확인 링크 차단] [{item['source']}] {preview_title} -> {final_url[:70]}")
                if not dry_run:
                    store.record_seen(item["id_hash"], item["title_hash"])
                continue
            if vip_header:
                item["source"] = vip_header

        # URL 섹션 차단 및 실제 발행 시각 검증을 Gemini 번역 전에 선행해 불필요한 API 소모 차단
        if any(seg in final_url.lower() for seg in ("/sports/", "/entertain/", "/entertainment/", "/press+releases/", "/press-releases/")):
            logger.info(f"[비시장 URL 섹션 차단] [{item['source']}] {preview_title} -> {final_url[:70]}")
            if not dry_run:
                store.record_seen(item["id_hash"], item["title_hash"])
            continue

        is_fresh, fresh_reason = verify_true_publication_age(final_url, max_age_minutes=max_age_minutes)
        if not is_fresh:
            logger.info(f"[과거 기사 재수집 차단] [{item['source']}] {preview_title} -> {fresh_reason}")
            if not dry_run:
                store.record_seen(item["id_hash"], item["title_hash"])
            continue

        is_x_post = item["tier"] == "X_VIP" or item["source"].startswith("X ｜ ")
        title_ko = translate_to_korean(item["cleaned_title"], use_gemini=True, is_x_vip=is_x_post)
        if store.is_duplicate_or_similar(item["id_hash"], item["title_hash"], item["cleaned_title"], title_ko):
            if not dry_run:
                store.record_seen(item["id_hash"], item["title_hash"])
            continue

        ok, cat_label, _ = evaluate_article(
            item["cleaned_title"],
            title_ko,
            item["tier"],
            item["source"],
            item.get("context_en", ""),
        )
        if not ok:
            if not dry_run:
                store.record_seen(item["id_hash"], item["title_hash"])
            continue

        item["final_url"] = final_url
        item["title_ko"] = title_ko
        item["category"] = cat_label

        if send_telegram_alert(item, style=style, dry_run=dry_run):
            store.record_sent(
                item["id_hash"],
                item["title_hash"],
                item["source"],
                item["cleaned_title"],
                title_ko,
                final_url,
            )
            sent_count += 1
            age_min = int((datetime.now(timezone.utc) - item["pub_dt"]).total_seconds() / 60)
            logger.info(f"[발송 완료 | {age_min}분 전 기사] [{item['source']} ｜ {cat_label}] {title_ko}")
            if not dry_run:
                time.sleep(1.0)

    if not dry_run:
        store.save()
    logger.info(f"사이클 완료 ({time.time() - start_t:.1f}초 소요) | 신규 발송: {sent_count}건")
    return sent_count


# ==============================================================================
# 10. GitHub Actions 타임랙 제로(Zero-Lag) 고속 릴레이 엔진
# ==============================================================================
RELAY_REPO = "chartbook-v3/chartbook-news-bot"
RELAY_WORKFLOW_ID = "418034205"
EXCLUSIVE_REPO = "chartbook-v3/Exclusive"
EXCLUSIVE_WORKFLOW_ID = "426534720"
RELAY_CIPHERTEXT_HEX = "d98d1546754122c8f7bbcbc55d90fb4efcf8884f1dcd0f3b02e7809e1ea66cb9a6c84dd8b70a1e84"


def git_push_history_in_actions():
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        subprocess.run(["git", "add", "sent_history.json"], check=False)
        diff_res = subprocess.run(["git", "diff", "--staged", "--quiet"], check=False)
        if diff_res.returncode != 0:
            subprocess.run(["git", "commit", "-m", "chore: update sent_history.json [skip ci]"], check=False)
            pull_res = subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=False)
            if pull_res.returncode != 0:
                subprocess.run(["git", "rebase", "--abort"], check=False)
                logger.warning("Git pull --rebase 실패로 rebase 중단 (Step 5에서 재시도)")
                return
            push_res = subprocess.run(["git", "push"], check=False)
            if push_res.returncode == 0:
                logger.info("GitHub 저장소에 최신 sent_history.json 동기화 완료")
    except Exception as e:
        logger.warning(f"Git 동기화 예외: {e}")


def ensure_exclusive_bot_running(headers: dict):
    """Exclusive 봇이 유휴(queued/in_progress 0건) 상태면 함께 자동 기동시킵니다."""
    try:
        for st in ("queued", "in_progress"):
            r_chk = requests.get(
                f"https://api.github.com/repos/{EXCLUSIVE_REPO}/actions/runs?status={st}&per_page=1",
                headers=headers,
                verify=False,
                timeout=(4, 6),
            )
            if r_chk.status_code == 200 and r_chk.json().get("total_count", 0) > 0:
                return
        r_disp = requests.post(
            f"https://api.github.com/repos/{EXCLUSIVE_REPO}/actions/workflows/{EXCLUSIVE_WORKFLOW_ID}/dispatches",
            headers=headers,
            json={"ref": "main"},
            verify=False,
            timeout=(4, 8),
        )
        if r_disp.status_code == 204:
            logger.info("⚡ [Exclusive 연동] Exclusive 봇 자동 실행 트리거 완료 (HTTP 204)")
    except Exception as e:
        logger.debug(f"Exclusive 연동 체크 예외: {e}")


def trigger_next_github_workflow():
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        tg_bytes = TELEGRAM_BOT_TOKEN.encode("utf-8")
        key = (
            hashlib.sha256(tg_bytes + b"::chartbook_relay_v1").digest()
            + hashlib.sha256(tg_bytes + b"::chartbook_relay_v2").digest()
        )
        enc_bytes = bytes.fromhex(RELAY_CIPHERTEXT_HEX)
        gh_token = bytes([b ^ key[i] for i, b in enumerate(enc_bytes)]).decode("utf-8")

        headers = {
            "Authorization": f"Bearer {gh_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        ensure_exclusive_bot_running(headers)
        try:
            r_chk = requests.get(
                f"https://api.github.com/repos/{RELAY_REPO}/actions/runs?status=queued&per_page=2",
                headers=headers,
                verify=False,
                timeout=(4, 6),
            )
            if r_chk.status_code == 200 and r_chk.json().get("total_count", 0) > 0:
                logger.info("이미 대기 중인 워크플로우(queued)가 존재하여 추가 릴레이 호출 생략")
                return
        except Exception as e:
            logger.debug(f"대기 워크플로우 조회 예외 (계속 릴레이 시도): {e}")

        for attempt in range(3):
            try:
                r_disp = requests.post(
                    f"https://api.github.com/repos/{RELAY_REPO}/actions/workflows/{RELAY_WORKFLOW_ID}/dispatches",
                    headers=headers,
                    json={"ref": "main"},
                    verify=False,
                    timeout=(4, 10),
                )
                if r_disp.status_code == 204:
                    logger.info("⚡ [Zero-Lag Relay] 다음 회차 GitHub Actions 즉시 실행 예약 완료 (HTTP 204)")
                    return
                logger.warning(f"릴레이 호출 응답 (시도 {attempt + 1}/3): HTTP {r_disp.status_code}")
            except Exception as e:
                logger.warning(f"릴레이 트리거 예외 (시도 {attempt + 1}/3): {e}")
            time.sleep(2)
    except Exception as e:
        logger.warning(f"릴레이 초기화 예외: {e}")


def main():
    script_start = time.time()
    parser = argparse.ArgumentParser(description="ChartBook Global & Korea Breaking News Telegram Bot v2")
    parser.add_argument("--once", action="store_true", help="1회 또는 GitHub Actions 연속 스캔 + 릴레이 실행")
    parser.add_argument("--style", type=int, default=DEFAULT_STYLE, help="출력 시안 번호 (기본값: 1)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SECONDS, help="스캔 주기(초), 기본 180초")
    parser.add_argument("--max-age", type=int, default=MAX_NEWS_AGE_MINUTES, help="최대 허용 기사 경과 시간(분), 기본 60분")
    parser.add_argument("--dry-run", action="store_true", help="텔레그램 실제 발송 없이 콘솔 미리보기 및 필터 검증만 수행")
    args = parser.parse_args()

    store = HistoryStore(HISTORY_JSON_PATH)

    logger.info("=================================================================")
    logger.info(f"🚀 ChartBook News Bot v2 가동 (시안 {args.style}번 | 실시간 {args.max_age}분 타임락)")
    logger.info(f"   - 타깃 채널: {TELEGRAM_CHAT_ID} {'[DRY-RUN]' if args.dry_run else ''}")
    squawk_cnt = sum(1 for f in FEEDS if f.get("type") == "tg_web")
    x_vip_cnt = sum(1 for f in FEEDS if f.get("tier") == "X_VIP")
    rss_cnt = len(FEEDS) - squawk_cnt - x_vip_cnt
    logger.info(
        f"   - 모니터링 피드: {len(FEEDS)}개 (글로벌/한국 {rss_cnt}개 + 텔레그램 스쿼크 {squawk_cnt}개 + X 주요인사 {x_vip_cnt}개 그룹/{len(X_VIP_PROFILES)}명)"
    )
    logger.info(f"   - 누적 중복방지 해시: {len(store.id_hashes)}건")
    logger.info("=================================================================")

    if args.once:
        if os.getenv("GITHUB_ACTIONS") == "true" and not args.dry_run:
            # GitHub Actions 5분(300초) 강제 종료 방지를 위해 최대 200초(3분 20초) 안전 예산 설정
            actions_deadline = script_start + 200.0
            burst_cycles = 3
            burst_interval = 65
            try:
                for idx in range(burst_cycles):
                    if idx > 0 and time.time() + 35.0 >= actions_deadline:
                        logger.info(f"안전 타임아웃 예산({actions_deadline - script_start:.0f}초) 도달로 남은 스캔 생략 후 릴레이 진행")
                        break
                    cycle_start = time.time()
                    logger.info(f"--- [GitHub Actions 고속 스캔 {idx + 1}/{burst_cycles}] ---")
                    run_cycle(store, style=args.style, max_age_minutes=args.max_age, dry_run=False, deadline=actions_deadline)
                    if idx < burst_cycles - 1:
                        elapsed = time.time() - cycle_start
                        remaining = actions_deadline - time.time()
                        if remaining <= 40.0:
                            logger.info("남은 실행 시간 부족으로 대기 생략 후 즉시 릴레이 전환")
                            break
                        sleep_sec = min(max(10.0, burst_interval - elapsed), remaining - 35.0)
                        if sleep_sec >= 5.0:
                            logger.info(f"다음 고속 스캔까지 {sleep_sec:.0f}초 대기...")
                            time.sleep(sleep_sec)
            except Exception as e:
                logger.error(f"Actions 스캔 루프 예외 (릴레이는 계속 진행): {e}")
            finally:
                git_push_history_in_actions()
                trigger_next_github_workflow()
        else:
            run_cycle(store, style=args.style, max_age_minutes=args.max_age, dry_run=args.dry_run)
        return

    while True:
        try:
            run_cycle(store, style=args.style, max_age_minutes=args.max_age, dry_run=args.dry_run)
        except KeyboardInterrupt:
            logger.info("사용자 요청으로 봇을 종료합니다.")
            break
        except Exception as e:
            logger.error(f"메인 루프 예외 발생: {e}")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()

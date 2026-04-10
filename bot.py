"""
Unusual Options & Whale Alert 뉴스 알리미 봇
- 대상 종목 19개에 대해 Google News RSS를 1시간마다 스캔
- 새 기사 발견 시 Gemini로 한국어 번역 및 주가 영향 심층 분석
- Slack 채널에 한글 알림 전송 (분석 요약 포함)
- Notion 데이터베이스에 결과 자동 기록
- history.json으로 중복 전송 방지
"""

import json
import logging
import os
import socket
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

import feedparser
import requests
import schedule
from dotenv import load_dotenv
from google import genai
from google.genai import types as genai_types
from notion_client import Client as NotionClient

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

load_dotenv()

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

HISTORY_FILE = Path(__file__).parent / "data" / "history.json"

TICKERS = [
    "GOOGL", "GEV", "TSLA", "RKLB", "CLS",
    "MU", "COHR", "IREN", "TEM", "OKLO",
    "PL", "AMZN", "SOFI", "IONQ", "LUV",
    "LAES", "KTOS", "GE", "BCS",
]

TICKER_BATCH_SIZE = 6
RSS_BASE = "https://news.google.com/rss/search?hl=en-US&gl=US&ceid=US:en&q={query}"

GEMINI_MODEL = "gemini-2.5-flash"

# ─────────────────────────────────────────────
# 외부 클라이언트 초기화
# ─────────────────────────────────────────────

gemini_client = genai.Client(api_key=GEMINI_API_KEY)
notion = NotionClient(auth=NOTION_TOKEN)

# ─────────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 히스토리 (중복 방지)
# ─────────────────────────────────────────────

def load_history() -> set:
    """이미 전송한 기사 URL/ID 목록을 불러옵니다."""
    if not HISTORY_FILE.exists():
        return set()
    with open(HISTORY_FILE, encoding="utf-8") as f:
        try:
            data = json.load(f)
            return set(data.get("sent", []))
        except json.JSONDecodeError:
            return set()


def save_history(history: set) -> None:
    """전송 기록을 파일에 저장합니다."""
    HISTORY_FILE.parent.mkdir(exist_ok=True)
    tmp = str(HISTORY_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"sent": list(history)}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, HISTORY_FILE)


# ─────────────────────────────────────────────
# RSS 스크래핑
# ─────────────────────────────────────────────

def build_rss_url(tickers: list) -> str:
    """종목 배치와 키워드를 조합한 Google News RSS URL을 생성합니다."""
    keyword_part = '("unusual options" OR "whale alert" OR "options sweep")'
    ticker_part = "(" + " OR ".join(tickers) + ")"
    query = f"{keyword_part} {ticker_part}"
    encoded = query.replace('"', '%22').replace(' ', '+').replace('(', '%28').replace(')', '%29')
    return RSS_BASE.format(query=encoded)


def fetch_articles(tickers: list) -> list:
    """RSS 피드에서 기사 목록을 가져옵니다."""
    url = build_rss_url(tickers)
    articles = []
    try:
        socket.setdefaulttimeout(15)
        feed = feedparser.parse(url)
        for entry in feed.entries:
            guid = entry.get("id") or entry.get("link", "")
            articles.append({
                "guid": guid,
                "title": entry.get("title", "(제목 없음)"),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "source": entry.get("source", {}).get("title", "Google News"),
            })
    except Exception as e:
        log.error("RSS 피드 오류 (%s): %s", tickers, e)
    return articles


def scan_all_tickers() -> list:
    """전체 종목을 배치로 나눠 RSS를 조회하고 결과를 합칩니다."""
    all_articles = []
    for i in range(0, len(TICKERS), TICKER_BATCH_SIZE):
        batch = TICKERS[i: i + TICKER_BATCH_SIZE]
        articles = fetch_articles(batch)
        log.info("배치 %s → %d개 기사 수신", batch, len(articles))
        all_articles.extend(articles)
        time.sleep(1)

    seen_guids = set()
    unique = []
    for a in all_articles:
        if a["guid"] not in seen_guids:
            seen_guids.add(a["guid"])
            unique.append(a)
    return unique


# ─────────────────────────────────────────────
# Gemini 번역 & 주가 영향 분석
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """당신은 미국 주식 시장 전문 애널리스트이자 금융 번역가입니다.
주어진 영문 뉴스 기사 제목을 한국어로 번역하고, 해당 뉴스가 관련 종목의 주가에 미칠 영향을 분석합니다.

분석 시 다음 관점을 반드시 포함하세요:
1. 뉴스의 핵심 내용과 시장 신호 (옵션 거래, 고래 매수/매도 등)
2. 단기 주가 방향성 (상승/하락/변동성 확대 가능성)
3. 투자자가 주목해야 할 리스크 또는 기회 요인

응답은 반드시 아래 JSON 형식으로만 반환하세요. 다른 텍스트는 포함하지 마세요:
{
  "korean_title": "한국어로 번역된 기사 제목",
  "analysis": "3~4줄 분량의 주가 영향 분석 (한국어)"
}"""


GEMINI_MAX_RETRIES = 3
GEMINI_RETRY_BACKOFF = 5  # 초 (시도마다 2배씩 증가)


def analyze_with_gemini(article: dict) -> dict:
    """Gemini API를 사용해 기사를 번역하고 주가 영향을 분석합니다.
    503/429 등 일시적 오류는 지수 백오프로 최대 3회 재시도합니다."""
    title = article["title"]
    ticker_hint = ", ".join([t for t in TICKERS if t in title.upper()]) or "관련 종목"

    prompt = f"""다음 뉴스 기사를 분석해 주세요.

관련 종목: {ticker_hint}
기사 제목: {title}
출처: {article.get('source', 'Google News')}
발행일: {article.get('published', '알 수 없음')}"""

    raw = ""
    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.3,
                    response_mime_type="application/json",
                ),
            )
            raw = response.text.strip()
            result = json.loads(raw)
            log.info("Gemini 분석 완료: %s", result.get("korean_title", "")[:60])
            return result
        except json.JSONDecodeError as e:
            log.error("Gemini 응답 JSON 파싱 실패: %s | 응답: %s", e, raw[:200])
            break  # JSON 오류는 재시도해도 의미 없음
        except Exception as e:
            err_msg = str(e)
            is_transient = any(code in err_msg for code in ("503", "429", "UNAVAILABLE", "RESOURCE_EXHAUSTED"))
            if is_transient and attempt < GEMINI_MAX_RETRIES:
                wait = GEMINI_RETRY_BACKOFF * (2 ** (attempt - 1))
                log.warning("Gemini 일시 오류 (시도 %d/%d), %d초 후 재시도: %s", attempt, GEMINI_MAX_RETRIES, wait, err_msg[:80])
                time.sleep(wait)
            else:
                log.error("Gemini API 최종 실패 (시도 %d/%d): %s", attempt, GEMINI_MAX_RETRIES, err_msg)
                break

    # 모든 재시도 실패 시 기본값 반환 (봇 중단 방지)
    return {
        "korean_title": title,
        "analysis": "분석 실패 — 원문을 직접 확인하세요.",
    }


# ─────────────────────────────────────────────
# Notion 저장
# ─────────────────────────────────────────────

def parse_published_date(date_str: str):
    """RSS 날짜 문자열을 ISO 8601 형식으로 변환합니다."""
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def save_to_notion(article: dict, analysis: dict) -> bool:
    """분석 결과를 Notion 데이터베이스에 저장합니다.
    - 제목(Name): 한글 기사 제목
    - 페이지 본문: 종목 / 주가 분석 / 발행일 / 원문 링크
    """
    title = analysis.get("korean_title") or article["title"]
    analysis_text = analysis.get("analysis", "")
    link = article.get("link", "")
    published_date = parse_published_date(article.get("published", "")) or "날짜 미상"

    mentioned = [t for t in TICKERS if t in article["title"].upper()]
    ticker_str = ", ".join(mentioned) if mentioned else "기타"

    children = [
        {
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📌 종목"}}]},
        },
        {
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": ticker_str}}]},
        },
        {
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📊 주가 영향 분석"}}]},
        },
        {
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": analysis_text[:2000]}}]},
        },
        {
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📅 발행일"}}]},
        },
        {
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": published_date}}]},
        },
        {
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": "🔗 원문 링크"}}]},
        },
        {
            "object": "block", "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": link, "link": {"url": link} if link else None}}]
            },
        },
    ]

    try:
        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "Name": {"title": [{"text": {"content": title[:2000]}}]}
            },
            children=children,
        )
        log.info("Notion 저장 완료: %s", title[:60])
        return True
    except Exception as e:
        log.error("Notion 저장 실패: %s", e)
        return False


# ─────────────────────────────────────────────
# Slack 알림 (한글 + 분석 포함)
# ─────────────────────────────────────────────

def build_slack_payload(article: dict, analysis: dict) -> dict:
    """Slack Block Kit 메시지 페이로드를 생성합니다 (한글 번역 + 주가 분석 포함)."""
    original_title = article["title"]
    link = article["link"]
    published = article.get("published", "")
    source = article.get("source", "Google News")

    korean_title = analysis.get("korean_title") or original_title
    stock_analysis = analysis.get("analysis", "")

    mentioned = [t for t in TICKERS if t in original_title.upper()]
    ticker_tags = " ".join(f"`{t}`" for t in mentioned) if mentioned else "`관련종목`"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":rotating_light: *옵션/고래 알림 포착*  {ticker_tags}\n"
                    f"*<{link}|{korean_title}>*"
                ),
            },
        },
    ]

    # 주가 영향 분석 블록 (분석 결과가 있을 때만 추가)
    if stock_analysis and stock_analysis != "분석 실패 — 원문을 직접 확인하세요.":
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":bar_chart: *주가 영향 분석*\n{stock_analysis}",
            },
        })

    blocks.extend([
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f":newspaper: {source}   "
                        f":clock3: {published}   "
                        f":link: <{link}|원문 보기>"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ])

    return {"blocks": blocks}


def send_to_slack(article: dict, analysis: dict) -> bool:
    """Slack Webhook으로 기사를 전송합니다."""
    payload = build_slack_payload(article, analysis)
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200 and resp.text == "ok":
            return True
        log.error("Slack 전송 실패: %d %s", resp.status_code, resp.text)
        return False
    except requests.RequestException as e:
        log.error("Slack 요청 오류: %s", e)
        return False


def send_test_message() -> bool:
    """봇 시작 시 Slack에 테스트 메시지를 전송합니다."""
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        ":white_check_mark: *Unusual Options 알리미 봇 가동!*\n"
                        f"모니터링 종목 ({len(TICKERS)}개): "
                        + " ".join(f"`{t}`" for t in TICKERS)
                        + "\n검색 키워드: `unusual options` | `whale alert` | `options sweep`\n"
                        ":robot_face: Gemini AI 번역 & 주가 영향 분석 활성화\n"
                        ":notebook: Notion 데이터베이스 자동 기록 활성화\n"
                        ":alarm_clock: 1시간마다 자동 스캔합니다."
                    ),
                },
            }
        ]
    }
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        return resp.status_code == 200 and resp.text == "ok"
    except requests.RequestException as e:
        log.error("테스트 메시지 오류: %s", e)
        return False


# ─────────────────────────────────────────────
# 메인 작업 함수
# ─────────────────────────────────────────────

def run_scan() -> None:
    """RSS 수집 → Gemini 분석 → Slack 알림 → Notion 기록 전체 파이프라인 실행."""
    log.info("===== 스캔 시작 =====")
    history = load_history()
    articles = scan_all_tickers()

    new_articles = [a for a in articles if a["guid"] not in history]
    log.info("전체 %d개 중 신규 %d개", len(articles), len(new_articles))

    # 과거 기사는 전송하지 않고 캐시에만 저장 (최근 3시간 이내 기사만 유지)
    recent_articles = []
    now = datetime.now(timezone.utc)
    for a in new_articles:
        try:
            pub_date = parsedate_to_datetime(a["published"])
            if now - pub_date <= timedelta(hours=3):
                recent_articles.append(a)
            else:
                history.add(a["guid"]) # 전송하진 않되, 앞으로 중복 검색되지 않도록 히스토리에만 추가
        except Exception:
            recent_articles.append(a)

    log.info("이 중 최근 3시간 이내의 최신 기사 %d개만 처리합니다.", len(recent_articles))

    sent_count = 0
    for article in recent_articles:
        log.info("처리 중: %s", article["title"][:80])

        # 1) Gemini 번역 & 분석
        analysis = analyze_with_gemini(article)

        # 2) Slack 전송
        if send_to_slack(article, analysis):
            history.add(article["guid"])
            sent_count += 1
            log.info("Slack 전송 완료: %s", analysis.get("korean_title", "")[:60])

        # 3) Notion 저장 (Slack 성공 여부와 무관하게 시도)
        save_to_notion(article, analysis)

        time.sleep(1)  # Gemini/Notion rate limit 방지

    if sent_count > 0:
        save_history(history)
        log.info("Slack 전송: %d건", sent_count)
    else:
        log.info("신규 기사 없음 — 다음 스캔까지 대기")

    log.info("===== 스캔 완료 =====\n")


# ─────────────────────────────────────────────
# 실행 진입점
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="1회 스캔 후 종료 (GitHub Actions용)")
    args = parser.parse_args()

    missing = []
    if not SLACK_WEBHOOK_URL:
        missing.append("SLACK_WEBHOOK_URL")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if not NOTION_TOKEN:
        missing.append("NOTION_TOKEN")
    if not NOTION_DATABASE_ID:
        missing.append("NOTION_DATABASE_ID")
    if missing:
        raise SystemExit(f"오류: 다음 환경변수가 설정되지 않았습니다: {', '.join(missing)}")

    log.info("봇 초기화 중...")

    # GitHub Actions 또는 --once 플래그: 1회 스캔 후 종료
    if args.once or os.environ.get("GITHUB_ACTIONS") == "true":
        run_scan()
        raise SystemExit(0)

    # 로컬 실행: 테스트 메시지 + 1시간 간격 반복
    if send_test_message():
        log.info("테스트 메시지 전송 성공!")
    else:
        log.warning("테스트 메시지 전송 실패 — Webhook URL을 확인하세요.")

    run_scan()

    schedule.every(1).hours.do(run_scan)
    log.info("스케줄러 시작 — 1시간마다 스캔합니다. 종료하려면 Ctrl+C")

    while True:
        schedule.run_pending()
        time.sleep(30)

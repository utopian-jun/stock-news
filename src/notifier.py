import logging
import time
from collections import defaultdict

import requests

from src.fetcher import Article

logger = logging.getLogger(__name__)


def _format_time(published_parsed) -> str:
    if not published_parsed:
        return "시간 미상"
    t = time.strftime("%Y-%m-%d %H:%M UTC", published_parsed)
    return t


def _build_payload(articles: list[Article]) -> dict:
    """한 티커의 기사들을 Slack Block Kit 메시지로 변환합니다."""
    ticker = articles[0]
    header = f":newspaper: *[{ticker.ticker_symbol}] {ticker.ticker_label}* 뉴스 알림"

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"[{ticker.ticker_symbol}] {ticker.ticker_label} 뉴스"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "divider"},
    ]

    for article in articles:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*<{article.link}|{article.title}>*",
            },
        })
        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f":clock3: {_format_time(article.published_parsed)}  |  Google News"},
            ],
        })

    return {"blocks": blocks}


def send_articles(
    articles: list[Article],
    webhook_url: str,
    max_retries: int = 3,
    retry_backoff: int = 2,
) -> bool:
    """티커별로 묶어서 Slack에 전송합니다."""
    # 티커별 그룹핑
    grouped: dict[str, list[Article]] = defaultdict(list)
    for a in articles:
        grouped[a.ticker_symbol].append(a)

    all_ok = True
    for symbol, group in grouped.items():
        payload = _build_payload(group)
        ok = _post(payload, webhook_url, symbol, max_retries, retry_backoff)
        if not ok:
            all_ok = False

    return all_ok


def _post(payload: dict, webhook_url: str, symbol: str, max_retries: int, backoff: int) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            if resp.status_code == 200 and resp.text == "ok":
                logger.info("[%s] Slack 전송 성공 (%d개 기사)", symbol, len(payload["blocks"]))
                return True
            elif resp.status_code in (429, 500, 502, 503):
                logger.warning("[%s] Slack 응답 %d, 재시도 %d/%d", symbol, resp.status_code, attempt, max_retries)
            else:
                logger.error("[%s] Slack 전송 실패: %d %s", symbol, resp.status_code, resp.text)
                return False
        except requests.RequestException as e:
            logger.error("[%s] Slack 요청 오류 (시도 %d/%d): %s", symbol, attempt, max_retries, e)

        if attempt < max_retries:
            time.sleep(backoff * attempt)

    logger.error("[%s] Slack 전송 최종 실패", symbol)
    return False

import logging
import socket
import time
from dataclasses import dataclass

import feedparser

from src.settings import TickerConfig

logger = logging.getLogger(__name__)

GOOGLE_NEWS_RSS = (
    "https://news.google.com/rss/search"
    "?q={query}+stock&hl=en-US&gl=US&ceid=US:en"
)


@dataclass
class Article:
    guid: str
    title: str
    link: str
    published_parsed: time.struct_time | None
    ticker_symbol: str
    ticker_label: str


def fetch(ticker: TickerConfig) -> list[Article]:
    url = GOOGLE_NEWS_RSS.format(query=ticker.symbol)
    try:
        socket.setdefaulttimeout(10)
        feed = feedparser.parse(url)
    except Exception as e:
        logger.error("피드 파싱 실패 [%s]: %s", ticker.symbol, e)
        return []

    articles = []
    for entry in feed.entries:
        guid = entry.get("id") or entry.get("link", "")
        if not guid:
            continue
        articles.append(
            Article(
                guid=guid,
                title=entry.get("title", "(제목 없음)"),
                link=entry.get("link", ""),
                published_parsed=entry.get("published_parsed"),
                ticker_symbol=ticker.symbol,
                ticker_label=ticker.label,
            )
        )

    logger.debug("[%s] 피드에서 %d개 기사 수신", ticker.symbol, len(articles))
    return articles

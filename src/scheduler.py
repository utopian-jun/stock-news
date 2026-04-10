import logging
import random
import time

from src import deduplicator, fetcher, notifier
from src.settings import Settings

logger = logging.getLogger(__name__)


def run_forever(settings: Settings) -> None:
    logger.info("모니터링 시작 — 종목: %s", [t.symbol for t in settings.tickers])
    logger.info("폴링 간격: %d초 (+최대 60초 jitter)", settings.poll_interval_seconds)

    while True:
        _poll_once(settings)
        deduplicator.purge_old(settings.dedup_store_path, settings.dedup_ttl_days)
        jitter = random.uniform(0, 60)
        sleep_sec = settings.poll_interval_seconds + jitter
        logger.debug("다음 폴링까지 %.0f초 대기", sleep_sec)
        time.sleep(sleep_sec)


def _poll_once(settings: Settings) -> None:
    for ticker in settings.tickers:
        try:
            articles = fetcher.fetch(ticker)
            new_articles = [
                a for a in articles
                if deduplicator.is_new(a.guid, settings.dedup_store_path)
            ]
            new_articles = new_articles[:settings.max_articles_per_ticker]

            if not new_articles:
                logger.debug("[%s] 새 기사 없음", ticker.symbol)
                continue

            logger.info("[%s] 새 기사 %d개 발견 — Slack 전송", ticker.symbol, len(new_articles))
            ok = notifier.send_articles(
                new_articles,
                webhook_url=settings.slack_webhook_url,
                max_retries=settings.slack_max_retries,
                retry_backoff=settings.slack_retry_backoff,
            )

            if ok:
                for a in new_articles:
                    deduplicator.mark_seen(a.guid, settings.dedup_store_path)

        except Exception as e:
            logger.error("[%s] 폴링 중 예외 발생: %s", ticker.symbol, e, exc_info=True)

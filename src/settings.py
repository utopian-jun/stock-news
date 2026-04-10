import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent


@dataclass
class TickerConfig:
    symbol: str
    label: str


@dataclass
class Settings:
    slack_webhook_url: str
    poll_interval_seconds: int
    max_articles_per_ticker: int
    tickers: list[TickerConfig]
    dedup_ttl_days: int
    dedup_store_path: str
    slack_max_retries: int
    slack_retry_backoff: int


def load_settings() -> Settings:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL이 설정되지 않았습니다. .env 파일을 확인하세요.")

    config_path = ROOT / "config" / "stocks.yaml"
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    tickers = [
        TickerConfig(symbol=t["symbol"], label=t["label"])
        for t in cfg["tickers"]
    ]

    return Settings(
        slack_webhook_url=webhook_url,
        poll_interval_seconds=cfg.get("poll_interval_seconds", 300),
        max_articles_per_ticker=cfg.get("max_articles_per_ticker", 3),
        tickers=tickers,
        dedup_ttl_days=cfg["dedup"]["ttl_days"],
        dedup_store_path=str(ROOT / cfg["dedup"]["store_path"]),
        slack_max_retries=cfg["slack"]["max_retries"],
        slack_retry_backoff=cfg["slack"]["retry_backoff_seconds"],
    )

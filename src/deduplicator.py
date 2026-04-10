import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_lock = threading.Lock()


def _load(store_path: str) -> dict:
    if not os.path.exists(store_path):
        return {}
    with open(store_path, encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logger.warning("seen_articles.json 파싱 실패 — 초기화합니다.")
            return {}


def _save(store_path: str, data: dict) -> None:
    tmp = store_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, store_path)


def is_new(guid: str, store_path: str) -> bool:
    with _lock:
        data = _load(store_path)
        return guid not in data


def mark_seen(guid: str, store_path: str) -> None:
    with _lock:
        data = _load(store_path)
        data[guid] = datetime.now(timezone.utc).isoformat()
        _save(store_path, data)


def purge_old(store_path: str, ttl_days: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    with _lock:
        data = _load(store_path)
        before = len(data)
        data = {
            guid: ts
            for guid, ts in data.items()
            if datetime.fromisoformat(ts) > cutoff
        }
        after = len(data)
        if before != after:
            _save(store_path, data)
            logger.info("중복 기록 정리: %d개 삭제 (남은 항목: %d개)", before - after, after)

"""Danh mục: lấy từ API KingStock (chép order-flow/job/watchlist.py), giữ bản chụp data/watchlist.json để job vẫn
chạy được khi Fly.io ngủ/chết. Không có cả hai → không có gì để quét, job thoát mã 2.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

import httpx

from common.config import BROWSER_HEADERS, HTTP_TIMEOUT, KINGSTOCK_WATCHLIST, DATA, TZ

logger = logging.getLogger(__name__)

CACHE = DATA / "watchlist.json"


def _fetch() -> list[dict]:
    r = httpx.get(KINGSTOCK_WATCHLIST, headers=BROWSER_HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    items = []
    for it in r.json():
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym or it.get("enabled") is False:
            continue
        items.append({"symbol": sym, "company_name": it.get("company_name") or "",
                      "exchange": it.get("exchange") or ""})
    return sorted(items, key=lambda x: x["symbol"])


def load() -> tuple[list[dict], str]:
    """(danh sách mã, nguồn) — nguồn là "kingstock" | "cache" | "none"."""
    try:
        items = _fetch()
        if items:
            CACHE.parent.mkdir(parents=True, exist_ok=True)
            CACHE.write_text(json.dumps({
                "fetched_at": datetime.now(TZ).isoformat(timespec="seconds"),
                "source": KINGSTOCK_WATCHLIST, "items": items,
            }, ensure_ascii=False, indent=1), encoding="utf-8")
            return items, "kingstock"
        logger.warning("KingStock trả danh mục rỗng — dùng bản chụp")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc được KingStock (%s) — dùng bản chụp", exc)
    try:
        items = json.loads(CACHE.read_text(encoding="utf-8")).get("items") or []
        return items, ("cache" if items else "none")
    except FileNotFoundError:
        return [], "none"

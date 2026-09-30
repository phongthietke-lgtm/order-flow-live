"""Danh sách mã: TOÀN BỘ cổ phiếu đang niêm yết HOSE + HNX (không UPCOM), lấy thẳng từ VNDirect finfo — không phụ thuộc KingStock.

GET {VNDIRECT_BASE}/stocks?q=type:STOCK~status:LISTED&size=9999 — đo 30/09/2026: 1.522 mã (HOSE 405, HNX 299,
UPCOM 818). Bản chụp data/symbols.json được commit để job vẫn chạy khi endpoint danh sách lỗi.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

import httpx

from common.config import BROWSER_HEADERS, DATA, HTTP_TIMEOUT, TZ
from common.vndirect import VNDIRECT_BASE

logger = logging.getLogger(__name__)

CACHE = DATA / "symbols.json"
# UPCOM bỏ theo yêu cầu người dùng 30/09/2026 (818 mã, phần lớn ít giao dịch).
FLOORS = ("HOSE", "HNX")
# Danh sách trả ít hơn chừng này là bất thường (lỗi nguồn/trang dở) → dùng bản chụp thay vì ghi đè.
MIN_SYMBOLS = 500


def _fetch() -> list[dict]:
    params = {"q": "type:STOCK~status:LISTED", "size": 9999, "fields": "code,floor,companyName"}
    r = httpx.get(f"{VNDIRECT_BASE}/stocks", params=params, headers=BROWSER_HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    items = []
    for it in r.json().get("data") or []:
        sym = str(it.get("code") or "").strip().upper()
        floor = str(it.get("floor") or "").upper()
        if sym and floor in FLOORS:
            items.append({"symbol": sym, "exchange": floor, "company_name": it.get("companyName") or ""})
    return sorted(items, key=lambda x: x["symbol"])


def load() -> tuple[list[dict], str]:
    """(danh sách mã, nguồn) — nguồn là "vndirect" | "cache" | "none"."""
    try:
        items = _fetch()
        if len(items) >= MIN_SYMBOLS:
            CACHE.parent.mkdir(parents=True, exist_ok=True)
            CACHE.write_text(json.dumps({"fetched_at": datetime.now(TZ).isoformat(timespec="seconds"),
                                         "count": len(items), "items": items},
                                        ensure_ascii=False, indent=0), encoding="utf-8")
            return items, "vndirect"
        logger.warning("Danh sách mã chỉ có %d mã — dùng bản chụp", len(items))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không lấy được danh sách mã (%s) — dùng bản chụp", exc)
    try:
        items = json.loads(CACHE.read_text(encoding="utf-8")).get("items") or []
        return items, ("cache" if items else "none")
    except FileNotFoundError:
        return [], "none"

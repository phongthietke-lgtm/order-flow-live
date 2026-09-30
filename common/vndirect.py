"""Khớp lệnh chi tiết trong phiên từ VNDirect finfo — miễn phí, không token. Chép nguyên order-flow/common/vndirect.py (30/09/2026).

Endpoint: GET {VNDIRECT_BASE}/stock_intraday_latest?q=code:FPT&size=5000&page=N&sort=accumulatedVol:asc
Trả {"data":[{"code","tradingDate","time","last","adLast","lastVol","side","accumulatedVol",...}],
     "currentPage","size","totalElements","totalPages"} (trang 2+ không có totalPages), side ∈ {PB, PS, ATO, ATC}.

Bốn điều đã đo 20/09/2026 (FPT phiên 18/09: 12.669 tick, 3 trang, gộp đúng 15.500.700 cp như app CTCK):
1. Chỉ có PHIÊN GẦN NHẤT — hỏi `tradingDate:` ngày cũ trả rỗng. Nguồn giữ "latest" tới khi phiên mới
   bắt đầu (~09:00 hôm sau), nên cron sáng hôm sau vẫn vớt được phiên hôm trước.
2. Host `finfo-api.vndirect.com.vn` treo (HTTP 000), phải dùng `api-finfo.vndirect.com.vn`.
3. `last` là giá THÔ (nghìn đồng), `adLast` là giá điều chỉnh tại thời điểm hỏi — không tin adLast
   cho lịch sử, hệ số điều chỉnh tính lúc dựng chỉ báo từ kho nến DNSE (zone/profile.py).
4. KHÔNG có `sort` thì thứ tự giữa các trang không ổn định: 3 trang trả 12.669 dòng nhưng chỉ 9.486 tick
   khác nhau, tổng KL 18,75 tr (trùng + thiếu tuỳ lần gọi). `sort=accumulatedVol:asc` là khoá duy nhất
   tăng dần theo thời gian → trang ổn định, và cho phép kiểm tra đủ phiên: Σ lastVol == accumulatedVol cuối.
5. Nguồn thỉnh thoảng BỎ SÓT một tick lẻ, dù `sort` đã đúng và không có dòng trùng: TCB phiên 23/09/2026
   hụt 500 cp trên 39.007.100. Vì thế cổng chặn là `shortfall()` (dung sai cực nhỏ + ghi lại số hụt),
   không còn là "bằng đúng" — xem docstring của hàm đó.

Giữ khuôn common/dnse.py: một httpx.Client cho cả vòng lặp, nghỉ 0,3 s giữa các lần gọi, lỗi → [] +
last_error, không ném ra ngoài.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import httpx

from .config import BROWSER_HEADERS, HTTP_TIMEOUT, TZ

logger = logging.getLogger(__name__)

VNDIRECT_BASE = os.getenv("VNDIRECT_BASE", "https://api-finfo.vndirect.com.vn/v4")
THROTTLE_SECONDS = 0.3
PAGE_SIZE = 5000
# Chặn vòng lặp vô tận nếu nguồn báo totalPages lạ: 20 trang × 5000 = 100 k tick, gấp ~8 lần FPT ngày bán tháo.
MAX_PAGES = 20
# Dung sai cho phiên nguồn bỏ sót tick lẻ — xem shortfall(). Tương đối thuần, KHÔNG có sàn tuyệt đối, để
# `tests/test_zone.py` còn ý nghĩa và để mã mỏng không bị nhận phiên sai lệch đáng kể.
MAX_GAP_RATIO = 0.002
MAX_GAP_POINTS = 3

last_ok: datetime | None = None
last_error: str = ""


def parse_rows(rows: list[dict]) -> list[dict]:
    """Chuẩn hoá một trang → list tick {"date","time","price","vol","side","acc"}; bỏ dòng hỏng. Thứ tự giữ nguyên."""
    out: list[dict] = []
    for r in rows:
        try:
            vol = int(float(r["lastVol"]))
            price = float(r["last"])
            acc = int(float(r["accumulatedVol"]))
        except (KeyError, TypeError, ValueError):
            continue
        if vol <= 0 or price <= 0:
            continue
        out.append({
            "date": str(r.get("tradingDate") or ""),
            "time": str(r.get("time") or ""),
            "price": price,
            "vol": vol,
            "side": str(r.get("side") or "").upper(),
            "acc": acc,
        })
    return out


def gap_points(ticks: list[dict]) -> int:
    """Số chỗ chuỗi acc không liền: acc của tick này khác acc tick trước cộng KL của chính nó.

    Tick đầu phiên phải có acc == vol (chưa có gì luỹ kế trước đó), nên bắt đầu từ prev = 0.
    Một tick bị bỏ sót tạo ĐÚNG một điểm hụt; phân trang hỏng làm hụt rải khắp chuỗi.
    """
    prev = 0
    n = 0
    for t in ticks:
        if t["acc"] != prev + t["vol"]:
            n += 1
        prev = t["acc"]
    return n


def shortfall(ticks: list[dict]) -> int | None:
    """Số cp sàn đã đếm mà nguồn không trả tick; None nghĩa là phiên KHÔNG dùng được.

    0 = phiên đủ (Σ KL == accumulatedVol lớn nhất, KL luỹ kế do sàn đếm nên không phụ thuộc phân trang).
    Nguồn thỉnh thoảng bỏ sót một tick lẻ — TCB phiên 23/09/2026 hụt 500 cp trên 39.007.100 (0,0013 %) tại
    09:50:47, làm job đỏ 3/3 lượt và mất cả phiên của 38 mã còn lại. Bỏ cả phiên vì chừng đó là quá đắt,
    nên nhận phiên hụt rất nhỏ và trả về số cp hụt để bên gọi ghi lại.

    Từ chối khi: hụt quá MAX_GAP_RATIO, hụt rải quá MAX_GAP_POINTS chỗ, hoặc Σ KL VƯỢT luỹ kế. Hai điều kiện
    đầu phải cùng lúc để không mở cửa lại cho bệnh phân trang ngày 20/09 (thiếu `sort` → Σ KL 18,75 tr so với
    15,5 tr thật, lệch ~21 % và rải khắp chuỗi). Vượt luỹ kế là bệnh khác (trùng tick/dữ liệu hỏng).
    """
    if not ticks:
        return None
    total = sum(t["vol"] for t in ticks)
    acc = max(t["acc"] for t in ticks)
    gap = acc - total
    if gap < 0 or gap > acc * MAX_GAP_RATIO:
        return None
    if gap and gap_points(ticks) > MAX_GAP_POINTS:
        return None
    return gap


class VndirectClient:
    """Dùng trong `with` để giữ kết nối qua nhiều mã."""

    def __init__(self):
        self._client = httpx.Client(timeout=HTTP_TIMEOUT, headers=BROWSER_HEADERS)
        self._last_call = 0.0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._client.close()

    def _page(self, symbol: str, page: int) -> dict | None:
        global last_ok, last_error
        wait = THROTTLE_SECONDS - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        params = {"q": f"code:{symbol.upper()}", "size": PAGE_SIZE, "page": page, "sort": "accumulatedVol:asc"}
        try:
            r = self._client.get(f"{VNDIRECT_BASE}/stock_intraday_latest", params=params)
            self._last_call = time.monotonic()
            r.raise_for_status()
            payload = r.json()
        except Exception as exc:  # noqa: BLE001 — một mã hỏng không được làm chết cả vòng
            self._last_call = time.monotonic()
            last_error = f"{symbol}/p{page}: {exc}"
            logger.warning("VNDirect lỗi %s trang %d: %s", symbol, page, exc)
            return None
        last_ok = datetime.now(TZ)
        last_error = ""
        return payload

    def latest_session(self, symbol: str) -> list[dict]:
        """Toàn bộ tick của phiên gần nhất, CŨ → MỚI (theo KL luỹ kế). Thiếu trang hoặc không đủ phiên
        (Σ KL ≠ KL luỹ kế cuối) → trả [] — thà không ghi còn hơn ghi một phiên thiếu."""
        global last_error
        first = self._page(symbol, 1)
        if first is None:
            return []
        ticks = parse_rows(first.get("data") or [])
        try:
            pages = int(first.get("totalPages") or 1)
        except (TypeError, ValueError):
            pages = 1
        if pages > MAX_PAGES:
            logger.warning("VNDirect %s báo %d trang — cắt còn %d", symbol, pages, MAX_PAGES)
            pages = MAX_PAGES
        for p in range(2, pages + 1):
            nxt = self._page(symbol, p)
            if nxt is None:
                return []
            ticks.extend(parse_rows(nxt.get("data") or []))
        dates = {t["date"] for t in ticks}
        if len(dates) > 1:
            # Chưa từng thấy, nhưng nếu nguồn trộn hai phiên thì chỉ giữ phiên mới nhất.
            newest = max(dates)
            ticks = [t for t in ticks if t["date"] == newest]
        # Khử trùng phòng khi sort vẫn lệch giữa hai lần gọi trang; acc là khoá duy nhất của tick.
        seen: set[int] = set()
        uniq = []
        for t in ticks:
            if t["acc"] in seen:
                continue
            seen.add(t["acc"])
            uniq.append(t)
        uniq.sort(key=lambda t: t["acc"])
        if not uniq:
            # Khác bản gốc order-flow: gom toàn thị trường thì hàng trăm mã UPCOM không khớp lệnh nào — nguồn trả rỗng
            # là "không có giao dịch", không phải lỗi. last_error để trống (đã reset ở _page) để bên gọi phân biệt.
            return []
        gap = shortfall(uniq)
        if gap is None:
            last_error = (f"{symbol}: phiên thiếu tick (Σ KL {sum(t['vol'] for t in uniq):,} ≠ luỹ kế "
                          f"{max((t['acc'] for t in uniq), default=0):,}, {gap_points(uniq)} chỗ hụt)")
            logger.warning("VNDirect %s", last_error)
            return []
        if gap:
            logger.warning("VNDirect %s: nguồn thiếu %s cp tick — vẫn nhận phiên", symbol, f"{gap:,}")
        return uniq

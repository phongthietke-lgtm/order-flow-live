"""Cấu hình chung. Chép phần cần của order-flow/common/config.py — app này không phụ thuộc order-flow."""
from __future__ import annotations

import os
from datetime import timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
# Tick thô nén gz, mỗi (mã, ngày) một file, giữ mãi — nguồn gốc để dựng lại mọi thứ về sau (máy chủ live, lab…).
TICKS = DATA / "ticks"
# Bản ghi phiên (nến 5' có footprint) cùng định dạng kho order-flow/data/store.
STORE = DATA / "store"

# Offset cố định +7: VN không có giờ mùa hè, Windows mặc định thiếu bộ tzdata.
TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

# Danh mục lấy từ KingStock (app cảnh báo Stochastic trên Fly). Fly chết thì dùng bản chụp data/watchlist.json
# (khác order-flow: bản chụp được COMMIT để runner Actions cũng có dự phòng).
KINGSTOCK_WATCHLIST = os.getenv("KINGSTOCK_WATCHLIST", "https://kingstock-deptlink.fly.dev/api/watchlist")

# Header giả trình duyệt — một số nguồn có WAF chặn UA lạ.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

"""Tick VNDirect → nến 5 phút có footprint. Chép nguyên order-flow/flow/ticks.py (30/09/2026) — kho store cùng định dạng.

- VNDirect đặt tên `side` theo bên BỊ ĐỘNG: PB (passive buy) = người bán chủ động đập vào lệnh mua chờ → BÁN;
  PS = MUA. Đã đối chiếu 1.497 lệnh với Vietcap: đọc ngược 100 %.
- ATO/ATC không có bên chủ động → cột x, không cộng vào delta.
- Một lệnh chủ động bị tách nhiều tick (mỗi lệnh chờ bị khớp một dòng) → gộp tick liên tiếp cùng giây/giá/hướng
  thành một lệnh trước khi xét "lệnh lớn" (≥ 500 tr đ).
- Mã không có trường side (DGC: 0/667 tick) → mọi KL rơi vào x, cờ no_side.
"""
from __future__ import annotations

BUY, SELL, OTHER = "b", "s", "x"
SIDE_INDEX = {"PS": BUY, "PB": SELL}
BIG_VALUE = 500_000   # giá(nghìn) × KL ≥ 500 tr đ
BAR_MIN = 5


def tick_size(price: float, exchange: str = "HOSE") -> float:
    """Bước giá: HOSE theo dải giá (10/50/100 đ), HNX và UPCOM cố định 100 đ."""
    if (exchange or "HOSE").upper() != "HOSE":
        return 0.1
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    return 0.1


def band(exchange: str) -> float:
    """Biên độ trần/sàn theo sàn."""
    return {"HNX": 0.10, "UPCOM": 0.15}.get((exchange or "HOSE").upper(), 0.07)


def pkey(p: float) -> float:
    return round(p, 3)


def orders(ticks: list[dict]) -> list[dict]:
    """Gộp tick LIÊN TIẾP cùng giây + giá + hướng thành một lệnh. ATO/ATC giữ nguyên."""
    out: list[dict] = []
    for t in ticks:
        last = out[-1] if out else None
        if (last and last["time"] == t["time"] and last["price"] == t["price"] and last["side"] == t["side"]
                and t["side"] in SIDE_INDEX):
            last["vol"] += t["vol"]
        else:
            out.append(dict(t))
    return out


def _minute(t: str) -> int:
    h, m, *_ = t.split(":")
    return int(h) * 60 + int(m)


def session_bars(ticks: list[dict]) -> list[dict]:
    """Tick (cũ → mới) → nến thô để lưu kho: ATO, các nến 5', ATC. Mỗi nến
    {t, auction, o, h, l, c, buy, sell, x, bb, bs, bbv, bsv, lv: [[giá, bán, mua, x], ...] giá giảm dần,
     blv: [[giá, bán_lớn, mua_lớn], ...] chỉ các mức có lệnh lớn}.
    bbv/bsv = Σ giá × KL của lệnh lớn mua/bán chủ động (giá thô) → giá vốn bình quân cá mập = bbv / bb.
    Bản ghi trước 26/09/2026 không có bbv/bsv; trước 27/09/2026 không có blv (footprint cá mập)."""
    bars: dict[str, dict] = {}
    for o in orders(ticks):
        side = o["side"]
        if side in ("ATO", "ATC"):
            key = label = side
        else:
            m = _minute(o["time"]) // BAR_MIN * BAR_MIN
            key, label = f"{m:04d}", f"{m // 60:02d}:{m % 60:02d}"
        b = bars.get(key)
        p = o["price"]
        if b is None:
            b = bars[key] = {"t": label, "auction": side in ("ATO", "ATC"), "o": p, "h": p, "l": p, "c": p,
                             "buy": 0, "sell": 0, "x": 0, "bb": 0, "bs": 0, "bbv": 0.0, "bsv": 0.0, "lv": {}, "blv": {}}
        b["h"], b["l"], b["c"] = max(b["h"], p), min(b["l"], p), p
        row = b["lv"].setdefault(pkey(p), [0, 0, 0])  # bán, mua, x
        idx = SIDE_INDEX.get(side, OTHER)
        big = idx != OTHER and p * o["vol"] >= BIG_VALUE
        if idx == BUY:
            b["buy"] += o["vol"]; row[1] += o["vol"]
            if big:
                b["bb"] += o["vol"]; b["bbv"] += p * o["vol"]
                b["blv"].setdefault(pkey(p), [0, 0])[1] += o["vol"]
        elif idx == SELL:
            b["sell"] += o["vol"]; row[0] += o["vol"]
            if big:
                b["bs"] += o["vol"]; b["bsv"] += p * o["vol"]
                b["blv"].setdefault(pkey(p), [0, 0])[0] += o["vol"]
        else:
            b["x"] += o["vol"]; row[2] += o["vol"]
    keys = (["ATO"] if "ATO" in bars else []) + sorted(k for k in bars if k not in ("ATO", "ATC")) \
        + (["ATC"] if "ATC" in bars else [])
    out = []
    for k in keys:
        b = bars[k]
        b["lv"] = [[p, *r] for p, r in sorted(b["lv"].items(), reverse=True)]
        b["blv"] = [[p, *r] for p, r in sorted(b["blv"].items(), reverse=True)]
        b["bbv"], b["bsv"] = round(b["bbv"], 2), round(b["bsv"], 2)
        out.append(b)
    return out


def resample(bars: list[dict], tf: int) -> list[dict]:
    """Gộp nến 5' thô của kho lên khung tf phút (bội của 5), theo mốc đồng hồ (09:30, 10:00…). ATO/ATC giữ nguyên."""
    if tf == BAR_MIN:
        return bars
    out: list[dict] = []
    cur, cur_key = None, None
    for b in bars:
        if b["auction"]:
            cur = None
            out.append(b)
            continue
        key = _minute(b["t"]) // tf
        if cur is None or cur_key != key:
            # nhãn = mốc thật của nến 5' đầu tiên trong khung (khung 30' đầu phiên bắt đầu 09:15, không phải 09:00)
            cur_key = key
            cur = {"t": b["t"], "auction": False, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"],
                   "buy": 0, "sell": 0, "x": 0, "bb": 0, "bs": 0, "lv": {}}
            out.append(cur)
        cur["h"], cur["l"], cur["c"] = max(cur["h"], b["h"]), min(cur["l"], b["l"]), b["c"]
        for k in ("buy", "sell", "x", "bb", "bs"):
            cur[k] += b[k]
        for k in ("bbv", "bsv"):  # bản ghi cũ không có → không thêm khoá
            if k in b:
                cur[k] = round(cur.get(k, 0.0) + b[k], 2)
        for p, s, bu, x in b["lv"]:
            row = cur["lv"].setdefault(p, [0, 0, 0])
            row[0] += s; row[1] += bu; row[2] += x
        if "blv" in b:  # bản ghi trước 27/09/2026 không có → khung gộp cũng không có
            for p, s, bu in b["blv"]:
                row = cur.setdefault("blv", {}).setdefault(p, [0, 0])
                row[0] += s; row[1] += bu
    for b in out:
        for k in ("lv", "blv"):
            if isinstance(b.get(k), dict):
                b[k] = [[p, *r] for p, r in sorted(b[k].items(), reverse=True)]
    return out


def session_record(ticks: list[dict]) -> dict:
    """Bản ghi một phiên để lưu kho data/store/<MÃ>/<ngày>.json."""
    total = sum(t["vol"] for t in ticks)
    acc = max((t.get("acc") or 0 for t in ticks), default=0)
    no_side = not any(t["side"] in SIDE_INDEX for t in ticks)
    rec = {"date": ticks[-1]["date"], "src": "vnd", "close": ticks[-1]["price"], "total": total,
           "ticks": len(ticks), "gap": max(0, acc - total), "bars": session_bars(ticks)}
    if no_side:
        rec["no_side"] = True
    return rec


def settled(ticks: list[dict], today: str) -> bool:
    """Phiên đã xong chưa: phiên của ngày trước hôm nay, hoặc đã có ATC / tick sau 14:45.
    Chạy job trong giờ giao dịch thì nguồn trả phiên dở dang — không được lưu như phiên đủ."""
    if not ticks:
        return False
    if ticks[-1]["date"] < today:
        return True
    return any(t["side"] == "ATC" for t in ticks) or ticks[-1]["time"] >= "14:45:00"

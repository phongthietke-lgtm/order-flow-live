"""Giai đoạn 0 (b) — gom THỬ cả ngày HOSE + HNX theo cách máy chủ live sẽ làm: hỏi THEO GIỜ cho cả thị trường
thay vì từng mã. Đo: tổng lệnh/ngày, dung lượng, số lần gọi mỗi vòng, bộ nhớ, và có sót lệnh không.

Phát hiện 30/09/2026 09:23: `q=floor:HOSE,HNX~time:gte:HH:MM:SS~time:lt:HH:MM:SS` trả lệnh của MỌI mã trong khoảng
giờ đó (1 phút ≈ 1.600 lệnh, vừa một trang 5.000). Không cần sort: mỗi khoảng được giữ < 1 trang (quá thì chia đôi),
nên không có bệnh phân trang lệch của ngày 20/09.

Cách chạy mỗi vòng (30 s): hỏi khoảng [con trỏ − 90 s, hết] — lùi 90 s vì nguồn giao lệnh trễ ~17–40 s (đo 09:16);
khử trùng theo (mã, KL luỹ kế). Khởi động: nạp lại từ 09:00 theo từng phút.
Cuối buổi: mỗi mã so Σ KL với KL luỹ kế (chuỗi liền) — bản gom từng mã của Actions 15:10 dùng để đối chiếu thêm.

Chạy:  venv\\Scripts\\python -m scripts.probe_market --until 15:00
Kết quả: data/probe/market-<ngày>/summary.json (+ ghi giữa chừng mỗi 10 phút), <MÃ>.json.gz
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
import tracemalloc
from datetime import datetime, timedelta

import httpx

from common import vndirect
from common.config import BROWSER_HEADERS, DATA, HTTP_TIMEOUT, TZ
from job.collect import pack_ticks

URL = f"{vndirect.VNDIRECT_BASE}/stock_intraday_latest"
FLOORS = "HOSE,HNX"
OVERLAP = 90          # giây lùi con trỏ mỗi vòng
MIN_SPLIT = 1         # khoảng nhỏ nhất (giây) khi chia đôi


def hms(s: int) -> str:
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def secs(t: str) -> int:
    h, m, s = (int(x) for x in t.split(":"))
    return h * 3600 + m * 60 + s


class Market:
    def __init__(self):
        self.c = httpx.Client(timeout=HTTP_TIMEOUT, headers=BROWSER_HEADERS)
        # mã → {acc: (time, price, vol, side)}; floor riêng
        self.ticks: dict[str, dict[int, tuple]] = {}
        self.floor: dict[str, str] = {}
        self.calls = 0
        self.errors: list[str] = []
        self.split_seconds: list[str] = []   # khoảng 1 giây mà vẫn > 1 trang (ATC) — phải vá bằng hỏi từng mã
        self.day = ""
        self.repairs: list[dict] = []
        # chỉ cổ phiếu HOSE/HNX trong danh sách bộ gom; nguồn còn trả chứng quyền/ETF cùng sàn → đếm riêng rồi bỏ
        stocks = json.loads((DATA / "symbols.json").read_text(encoding="utf-8"))["items"]
        self.stocks = {it["symbol"] for it in stocks}
        self.other: dict[str, int] = {}

    def _get(self, q: str, page: int = 1) -> dict | None:
        for attempt in range(3):
            try:
                r = self.c.get(URL, params={"q": q, "size": vndirect.PAGE_SIZE, "page": page})
                self.calls += 1
                r.raise_for_status()
                return r.json()
            except Exception as exc:  # noqa: BLE001
                self.errors.append(f"{datetime.now(TZ):%H:%M:%S} {q} p{page}: {str(exc)[:80]}")
                time.sleep(1 + attempt * 2)
            finally:
                time.sleep(vndirect.THROTTLE_SECONDS)
        return None

    def _add(self, rows: list[dict]) -> int:
        new = 0
        for r in rows:
            for t in vndirect.parse_rows([r]):
                sym = str(r.get("code") or "").upper()
                if not sym:
                    continue
                if sym not in self.stocks:
                    self.other[sym] = self.other.get(sym, 0) + 1
                    continue
                self.day = max(self.day, t["date"])
                book = self.ticks.setdefault(sym, {})
                if t["acc"] not in book:
                    book[t["acc"]] = (t["time"], t["price"], t["vol"], t["side"])
                    new += 1
                self.floor[sym] = str(r.get("floor") or "")
        return new

    def window(self, a: int, b: int | None) -> int:
        """Lệnh có giờ trong [a, b) (b=None: tới hết). Quá 1 trang thì chia đôi; khoảng 1 giây vẫn quá → đọc hết trang."""
        q = f"floor:{FLOORS}~time:gte:{hms(a)}" + (f"~time:lt:{hms(b)}" if b is not None else "")
        d = self._get(q)
        if d is None:
            return 0
        pages = int(d.get("totalPages") or 1)
        if pages <= 1:
            return self._add(d.get("data") or [])
        end = b if b is not None else secs(datetime.now(TZ).strftime("%H:%M:%S")) + 1
        if end - a > MIN_SPLIT:
            mid = (a + end) // 2
            return self.window(a, mid) + self.window(mid, b)
        # một giây > 5.000 lệnh (ATC): đọc từng trang; không sort nên có thể lệch → chuỗi KL luỹ kế sẽ bắt ở cuối
        self.split_seconds.append(hms(a))
        n = self._add(d.get("data") or [])
        for p in range(2, min(pages, vndirect.MAX_PAGES) + 1):
            dp = self._get(q, p)
            if dp:
                n += self._add(dp.get("data") or [])
        return n

    def total(self) -> int:
        return sum(len(b) for b in self.ticks.values())

    def latest(self) -> int:
        return max((secs(v[0]) for b in self.ticks.values() for v in b.values()), default=9 * 3600)

    def repair(self) -> int:
        """Mã có chuỗi KL luỹ kế hụt (thường do giây ATO/ATC > 1 trang, phân trang không sort) → hỏi riêng mã đó cả
        phiên và thay. Máy chủ live sẽ làm đúng như vậy; đếm để biết mỗi ngày phải vá bao nhiêu mã."""
        bad = list(self.check())
        if not bad:
            return 0
        fixed = 0
        with vndirect.VndirectClient() as vc:
            for sym in bad:
                full = vc.latest_session(sym)
                if full and full[-1]["date"] == self.day:
                    self.ticks[sym] = {t["acc"]: (t["time"], t["price"], t["vol"], t["side"]) for t in full}
                    fixed += 1
        self.repairs.append({"at": datetime.now(TZ).strftime("%H:%M:%S"), "broken": len(bad), "fixed": fixed,
                             "sample": bad[:10]})
        return fixed

    def check(self) -> dict:
        """Mỗi mã: chuỗi KL luỹ kế có liền không (Σ KL == KL luỹ kế cuối)."""
        bad = {}
        for sym, book in self.ticks.items():
            accs = sorted(book)
            total = sum(book[a][2] for a in accs)
            if total != accs[-1]:
                bad[sym] = {"sum": total, "acc": accs[-1], "missing": accs[-1] - total}
        return bad

    def summary(self, cycles: list[dict], final: bool) -> dict:
        by_floor: dict[str, dict] = {}
        for sym, book in self.ticks.items():
            f = by_floor.setdefault(self.floor.get(sym, "?"), {"symbols": 0, "ticks": 0})
            f["symbols"] += 1
            f["ticks"] += len(book)
        top = sorted(((len(b), s) for s, b in self.ticks.items()), reverse=True)[:10]
        cur, peak = tracemalloc.get_traced_memory()
        bad = self.check()
        return {
            "day": self.day, "at": datetime.now(TZ).isoformat(timespec="seconds"), "final": final,
            "ticks": self.total(), "symbols": len(self.ticks), "by_floor": by_floor,
            "top10": [{"sym": s, "ticks": n} for n, s in top],
            "calls": self.calls, "errors": len(self.errors), "errors_tail": self.errors[-10:],
            "atc_split_seconds": self.split_seconds,
            "memory_mb": {"now": round(cur / 1e6, 1), "peak": round(peak / 1e6, 1)},
            "cycles": {"n": len(cycles),
                       "calls_median": statistics.median(c["calls"] for c in cycles) if cycles else None,
                       "calls_max": max((c["calls"] for c in cycles), default=None),
                       "secs_median": statistics.median(c["secs"] for c in cycles) if cycles else None,
                       "secs_max": max((c["secs"] for c in cycles), default=None)},
            "not_stock": {"symbols": len(self.other), "rows": sum(self.other.values())},
            "repairs": self.repairs,
            "broken_chains": len(bad), "broken_sample": dict(list(bad.items())[:20]),
        }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="15:00", help="giờ VN HH:MM")
    ap.add_argument("--every", type=float, default=30)
    a = ap.parse_args()
    tracemalloc.start()
    m = Market()
    out = DATA / "probe" / f"market-{datetime.now(TZ).date().isoformat()}"
    out.mkdir(parents=True, exist_ok=True)

    # Nạp lại từ 09:00 theo từng phút tới hiện tại
    t0 = time.monotonic()
    now_s = secs(datetime.now(TZ).strftime("%H:%M:%S"))
    for a_s in range(9 * 3600, now_s, 60):
        m.window(a_s, a_s + 60)
    fixed = m.repair()
    print(f"nạp lại 09:00→{hms(now_s)} (vá {fixed} mã): {m.total():,} lệnh, {len(m.ticks)} mã, {m.calls} lần gọi, "
          f"{time.monotonic() - t0:.0f}s", flush=True)

    cycles: list[dict] = []
    last_dump = time.monotonic()
    until = secs(a.until + ":00")
    while secs(datetime.now(TZ).strftime("%H:%M:%S")) < until:
        ts = time.monotonic()
        calls0 = m.calls
        cursor = max(9 * 3600, m.latest() - OVERLAP)
        n = m.window(cursor, None)
        dur = time.monotonic() - ts
        cycles.append({"at": datetime.now(TZ).strftime("%H:%M:%S"), "calls": m.calls - calls0, "new": n,
                       "secs": round(dur, 1)})
        print(f"{cycles[-1]['at']} +{n} lệnh · {m.calls - calls0} lần gọi · {dur:.1f}s · tổng {m.total():,}", flush=True)
        if time.monotonic() - last_dump > 600:
            m.repair()
            (out / "summary.json").write_text(json.dumps(m.summary(cycles, False), ensure_ascii=False, indent=1),
                                              encoding="utf-8")
            last_dump = time.monotonic()
        time.sleep(max(0.0, a.every - dur))

    # Vét cuối: ATC và lệnh giao trễ, rồi vá mã hụt
    m.window(secs("14:25:00"), None)
    m.repair()
    size = 0
    for sym, book in m.ticks.items():
        ticks = [{"date": m.day, "time": v[0], "price": v[1], "vol": v[2], "side": v[3], "acc": acc}
                 for acc, v in sorted(book.items())]
        data = pack_ticks(ticks)
        (out / f"{sym}.json.gz").write_bytes(data)
        size += len(data)
    s = m.summary(cycles, True)
    s["gz_bytes"] = size
    (out / "summary.json").write_text(json.dumps(s, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\n== TỔNG KẾT ==")
    print(json.dumps({k: s[k] for k in ("ticks", "symbols", "by_floor", "calls", "errors", "memory_mb", "cycles",
                                        "broken_chains", "atc_split_seconds", "gz_bytes", "not_stock")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

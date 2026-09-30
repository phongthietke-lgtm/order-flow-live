"""Giai đoạn 0 — đo VNDirect trong giờ phiên, trước khi viết máy chủ live. Chỉ đọc, không ghi kho.

Đo 4 điều (kế hoạch C:\\Users\\IT\\.claude\\plans\\h-y-nghi-n-c-u-3-keen-wirth.md):
1. Bộ lọc tăng dần `q=code:X~accumulatedVol:gt:<acc>` có đúng không (chỉ trả tick mới, không sót, không trùng).
2. Proxy có giữ bản cũ không (`Cache-Control: max-age=300`, header X-Proxy-Cache HIT/MISS).
3. Độ trễ: giờ tick mới nhất so với đồng hồ, ở mã thanh khoản cao.
4. 39 mã × nhịp 30 s có bị chặn (429/403/timeout) không.

Cuối buổi so chuỗi tick gom tăng dần với một lần tải cả phiên: phải trùng tuyệt đối.

Chạy:  venv\\Scripts\\python -m scripts.probe_live --minutes 20
Kết quả: data/probe/<ngày>-<giờ>.json (gitignored) + tóm tắt in ra.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import datetime

import httpx

from common import vndirect
from common.config import BROWSER_HEADERS, DATA, HTTP_TIMEOUT, TZ

# 39 mã danh mục order-flow/KingStock (chép tay 30/09/2026) — chỉ để đo, không phụ thuộc lúc chạy.
SYMS = ("ACB AGR BID CEO CII CTG DCM DGC DPR DXG EIB FPT GAS GMD GVR HCM HPG KBC LPB MBB MBS MSB PLX PVD PVS SHB "
        "SHS SSI STB SZC TCB TCH TPB VCB VCI VDS VIB VIX VPB").split()
LIQUID = ("FPT", "HPG", "SSI", "VPB", "MBB", "TCB", "SHB", "VIX")
URL = f"{vndirect.VNDIRECT_BASE}/stock_intraday_latest"


def secs(hms: str) -> int:
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


BUST = False  # --bust: thêm tham số _=<ms> để mỗi URL là duy nhất, proxy không thể trả bản giữ sẵn


def fetch_new(c: httpx.Client, sym: str, acc: int) -> tuple[list[dict], dict]:
    """Tick có accumulatedVol > acc, cũ → mới. Trả (tick, số đo của lần gọi)."""
    ticks, meta = [], {"pages": 0, "cache": set(), "status": 200, "ms": 0.0}
    page = 1
    while True:
        params = {"q": f"code:{sym}~accumulatedVol:gt:{acc}", "size": vndirect.PAGE_SIZE, "page": page,
                  "sort": "accumulatedVol:asc"}
        if BUST:
            params["_"] = int(time.time() * 1000)
        t0 = time.monotonic()
        r = c.get(URL, params=params)
        meta["ms"] += (time.monotonic() - t0) * 1000
        meta["pages"] += 1
        meta["status"] = r.status_code
        meta["cache"].add(r.headers.get("X-Proxy-Cache", "-"))
        if r.status_code != 200:
            return ticks, meta
        payload = r.json()
        ticks.extend(vndirect.parse_rows(payload.get("data") or []))
        total = int(payload.get("totalPages") or 1) if page == 1 else meta.get("total_pages", 1)
        meta["total_pages"] = total
        if page >= total or page >= vndirect.MAX_PAGES:
            return ticks, meta
        page += 1
        time.sleep(vndirect.THROTTLE_SECONDS)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=20)
    ap.add_argument("--every", type=float, default=30)
    ap.add_argument("--bust", action="store_true")
    a = ap.parse_args()
    global BUST
    BUST = a.bust

    state = {s: {"acc": 0, "ticks": [], "bad": 0} for s in SYMS}
    cycles, errors, lags = [], [], {s: [] for s in LIQUID}
    caches: dict[str, int] = {}
    hits: list[dict] = []
    deliver: list[int] = []
    stop = time.monotonic() + a.minutes * 60
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=BROWSER_HEADERS) as c:
        while time.monotonic() < stop:
            t_start = time.monotonic()
            now = datetime.now(TZ)
            got, calls, ms = 0, 0, []
            for sym in SYMS:
                st = state[sym]
                try:
                    new, meta = fetch_new(c, sym, st["acc"])
                except Exception as exc:  # noqa: BLE001
                    errors.append({"at": now.strftime("%H:%M:%S"), "sym": sym, "err": str(exc)[:120]})
                    time.sleep(vndirect.THROTTLE_SECONDS)
                    continue
                calls += meta["pages"]
                ms.append(meta["ms"])
                for k in meta["cache"]:
                    caches[k] = caches.get(k, 0) + 1
                hit = any("HIT" in k for k in meta["cache"])
                if meta["status"] != 200:
                    errors.append({"at": now.strftime("%H:%M:%S"), "sym": sym, "err": f"HTTP {meta['status']}"})
                new = [t for t in new if t["acc"] > st["acc"]]
                if new and new[0]["acc"] != st["acc"] + new[0]["vol"] and st["acc"]:
                    st["bad"] += 1  # chỗ nối giữa hai lượt không liền → bộ lọc sót tick
                if new and cycles:  # bỏ vòng đầu (tải cả phần đầu phiên): trễ giao = đồng hồ − giờ khớp của tick mới
                    wall = now.hour * 3600 + now.minute * 60 + now.second
                    deliver.extend(wall - secs(t["time"]) for t in new if t["side"] not in ("ATO", "ATC"))
                if new:
                    st["ticks"].extend(new)
                    st["acc"] = new[-1]["acc"]
                    got += len(new)
                if hit:
                    hits.append({"at": now.strftime("%H:%M:%S"), "sym": sym, "new": len(new)})
                if sym in LIQUID and st["ticks"]:
                    wall = now.hour * 3600 + now.minute * 60 + now.second
                    lags[sym].append(wall - secs(st["ticks"][-1]["time"]))
                time.sleep(vndirect.THROTTLE_SECONDS)
            dur = time.monotonic() - t_start
            cycles.append({"at": now.strftime("%H:%M:%S"), "secs": round(dur, 1), "calls": calls, "new_ticks": got,
                           "ms_median": round(statistics.median(ms), 0) if ms else None})
            print(f"{now:%H:%M:%S} vòng {dur:5.1f}s · {calls} lần gọi · +{got} tick · lỗi tích luỹ {len(errors)}", flush=True)
            time.sleep(max(0.0, a.every - dur))

        # Đối chiếu: tải cả phiên một lần, so với chuỗi gom tăng dần (cắt tới acc cuối đã gom)
        check = {}
        with vndirect.VndirectClient() as vc:
            for sym in SYMS:
                full = vc.latest_session(sym) or vc.latest_session(sym)  # thử lại 1 lần nếu timeout
                st = state[sym]
                part = [t for t in full if t["acc"] <= st["acc"]]
                key = lambda ts: [(t["time"], t["price"], t["vol"], t["side"], t["acc"]) for t in ts]  # noqa: E731
                if not full:
                    check[sym] = {"inc": len(st["ticks"]), "full_failed": True, "same": None}
                    continue
                check[sym] = {"inc": len(st["ticks"]), "full_upto": len(part), "same": key(part) == key(st["ticks"]),
                              "joins_bad": st["bad"], "gap_points": vndirect.gap_points(st["ticks"])}

    lag_all = [x for v in lags.values() for x in v]
    report = {
        "day": datetime.now(TZ).date().isoformat(), "minutes": a.minutes, "every": a.every, "symbols": len(SYMS),
        "cycles": cycles, "errors": errors, "proxy_cache": caches,
        "cycle_secs": {"median": statistics.median(c_["secs"] for c_ in cycles),
                       "max": max(c_["secs"] for c_ in cycles)} if cycles else None,
        "lag_secs": {"median": statistics.median(lag_all), "p90": sorted(lag_all)[int(len(lag_all) * .9)],
                     "max": max(lag_all)} if lag_all else None,
        "deliver_secs": {"n": len(deliver), "median": statistics.median(deliver),
                         "p90": sorted(deliver)[int(len(deliver) * .9)], "p99": sorted(deliver)[int(len(deliver) * .99)],
                         "max": max(deliver)} if deliver else None,
        "lag_by_sym": {s: {"median": statistics.median(v), "max": max(v)} for s, v in lags.items() if v},
        "incremental_vs_full": check,
        "all_same": all(v["same"] for v in check.values() if v["same"] is not None),
        "bust": a.bust, "hits": hits,
        "hits_with_nothing_new": sum(1 for h in hits if not h["new"]),
    }
    out = DATA / "probe" / f"{datetime.now(TZ):%Y-%m-%d-%H%M}{'-bust' if a.bust else ''}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\n== TÓM TẮT ==")
    print("vòng (s):", report["cycle_secs"], "· lỗi:", len(errors), "· proxy:", caches)
    print("trễ (s) mã thanh khoản:", report["lag_secs"])
    print("trễ giao tick mới (s):", report["deliver_secs"])
    bad = {s: v for s, v in check.items() if v["same"] is False or v.get("joins_bad")}
    print("phá bộ đệm:", a.bust, "· lượt HIT:", len(hits), "· HIT mà không có tick mới:", report["hits_with_nothing_new"])
    print("gom tăng dần == tải cả phiên:", report["all_same"], "· mã lệch:", bad or "không")
    print("tệp:", out)


if __name__ == "__main__":
    main()

"""Bộ gom sau phiên: danh mục KingStock → tick VNDirect → kho riêng data/ticks (tick thô gz) + data/store (nến 5').

Giai đoạn −1 của order-flow-live: gom sẵn ≥ 20 phiên trước khi có máy chủ chạy trong phiên. Độc lập với order-flow.

Chạy:
    venv\\Scripts\\python -m job.collect           # gom phiên gần nhất, bỏ (mã, ngày) đã có
    venv\\Scripts\\python -m job.collect --force   # gom lại cả mã đã có (ghi đè)

Gom NGAY TRONG NGÀY (cron 15:10 + bù 15:30/17:00/20:00): sáng 30/09/2026 lúc 07:22 nguồn đã trả rỗng, không còn
giữ phiên hôm trước tới 09:00 như ghi chú cũ. Idempotent theo (mã, ngày); gz ghi mtime=0 nên cùng tick → cùng byte.

Mã thoát: 0 ổn · 1 có mã lỗi (ĐÃ ghi xong phần còn lại — workflow vẫn phải commit) · 2 không có danh mục.
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from common import vndirect
from common.config import DATA, STORE, TICKS, TZ
from flow.ticks import session_record, settled
from job import watchlist

logger = logging.getLogger("order-flow-live")

# Một tick = [giờ, giá thô, KL, side, KL luỹ kế] — mảng thay cho dict để gz gọn; ngày ghi một lần ở đầu file.
FIELDS = ("time", "price", "vol", "side", "acc")


def dump(path: Path, obj, pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(obj, ensure_ascii=False, indent=1) if pretty else \
        json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    path.write_text(txt, encoding="utf-8")


def write_ticks(path: Path, ticks: list[dict]) -> None:
    """Tick (cũ → mới, cùng một ngày) → gz. mtime=0 + không tên file: nội dung giống thì byte giống, git không thấy diff."""
    doc = {"date": ticks[-1]["date"], "fields": list(FIELDS), "ticks": [[t[k] for k in FIELDS] for t in ticks]}
    raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f, gzip.GzipFile(filename="", mode="wb", fileobj=f, mtime=0, compresslevel=9) as gz:
        gz.write(raw)


def read_ticks(path: Path) -> list[dict]:
    """Ngược của write_ticks: trả tick dạng dict như vndirect.parse_rows (có "date")."""
    doc = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
    return [{"date": doc["date"], **dict(zip(doc["fields"], row))} for row in doc["ticks"]]


def has(sym: str, day: str) -> bool:
    return (TICKS / sym / f"{day}.json.gz").exists() and (STORE / sym / f"{day}.json").exists()


def collect(items: list[dict], force: bool, client) -> dict:
    """client có latest_session(sym) như vndirect.VndirectClient — test truyền bản giả."""
    today = datetime.now(TZ).date().isoformat()
    res = {"new": [], "have": [], "failed": {}, "unsettled": [], "gaps": {}, "day": None}
    for it in items:
        sym = it["symbol"]
        ticks = client.latest_session(sym)
        if not ticks:
            res["failed"][sym] = vndirect.last_error or "không có tick"
            continue
        day = ticks[-1]["date"]
        res["day"] = max(res["day"] or day, day)
        if not settled(ticks, today):  # chạy nhầm trong giờ phiên → không lưu phiên dở dang
            res["unsettled"].append(sym)
            continue
        if has(sym, day) and not force:
            res["have"].append(sym)
            continue
        rec = session_record(ticks)
        write_ticks(TICKS / sym / f"{day}.json.gz", ticks)
        dump(STORE / sym / f"{day}.json", rec)
        if rec["gap"]:
            res["gaps"][sym] = rec["gap"]
        res["new"].append(sym)
        logger.info("%s %s: %s tick", sym, day, f"{len(ticks):,}")
    return res


def sessions() -> list[str]:
    """Các ngày đã có trong kho (ít nhất một mã) — để biết đã gom được bao nhiêu phiên."""
    return sorted({p.name[:10] for p in TICKS.glob("*/*.json.gz")})


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    items, src = watchlist.load()
    if not items:
        print("Không có danh mục (KingStock chết và chưa có bản chụp)")
        return 2
    print(f"Danh mục {len(items)} mã ({src})")
    with vndirect.VndirectClient() as c:
        run = collect(items, a.force, c)
    days = sessions()
    state = {"run_at": datetime.now(TZ).isoformat(timespec="seconds"), "watchlist": src, "symbols": len(items),
             "day": run["day"], "new": run["new"], "have": len(run["have"]), "failed": run["failed"],
             "unsettled": run["unsettled"], "gaps": run["gaps"], "sessions": len(days),
             "first": days[0] if days else None, "vndirect_last_error": vndirect.last_error}
    dump(DATA / "state.json", state, pretty=True)
    print(f"phiên {run['day']}: mới {len(run['new'])} · đã có {len(run['have'])} · chưa xong phiên "
          f"{len(run['unsettled'])} · lỗi {len(run['failed'])} {' '.join(run['failed'])} · kho {len(days)} phiên")
    return 1 if run["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())

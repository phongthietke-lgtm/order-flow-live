"""Bộ gom sau phiên — TOÀN BỘ cổ phiếu HOSE + HNX (≈700 mã, không UPCOM), lệnh khớp gốc từ VNDirect. Độc lập hoàn toàn.

Giai đoạn −1 của order-flow-live: gom sẵn lịch sử lệnh khớp trước khi có máy chủ chạy trong phiên.

Lưu trữ (≈ vài MB nén/ngày — quá lớn để nhét vào git lâu dài, nên tách hai nơi):
- data/ticks/<ngày>/<MÃ>.json.gz  thư mục làm việc (không commit); đóng gói thành
- data/rel/ticks-<ngày>.tar        → workflow đưa lên GitHub Release tag `t<ngày>` (mỗi ngày một tệp đính kèm);
- data/days/<ngày>.json           (commit) mục lục: mỗi mã số tick, KL, mua/bán chủ động, số cp hụt; mã không khớp
                                   lệnh hôm nay; mã lỗi. Lượt bù đọc tệp này để chỉ gom mã còn thiếu.

Chạy:
    venv\\Scripts\\python -m job.collect                    # phiên hôm nay (chỉ sau 14:50)
    venv\\Scripts\\python -m job.collect --only FPT,VCB     # thử vài mã
    venv\\Scripts\\python -m job.collect --force            # gom lại cả mã đã có

Bẫy đã biết:
- Mã không khớp lệnh hôm nay vẫn được nguồn trả phiên CŨ (bẫy "nến cũ phát lại" của TuDoanh/KingStock) → chỉ nhận
  tick có ngày == ngày gom; khác ngày = "không giao dịch hôm nay".
- 07:22 sáng 30/09/2026 nguồn đã rỗng → phải gom trong ngày (cron 15:10, bù 15:30/17:00/20:00).
- Ngày nghỉ lễ: không mã mẫu nào có phiên hôm nay → dừng sớm, không ghi gì.

Mã thoát: 0 ổn · 1 có mã lỗi (ĐÃ ghi xong phần còn lại — workflow vẫn phải đóng gói + commit) · 2 không có danh sách mã.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import os
import sys
import tarfile
from datetime import datetime
from pathlib import Path

from common import vndirect
from common.config import DATA, TICKS, TZ
from flow.ticks import SIDE_INDEX, settled
from job import symbols

logger = logging.getLogger("order-flow-live")

DAYS = DATA / "days"
REL = DATA / "rel"
# Một tick = [giờ, giá thô, KL, side, KL luỹ kế] — mảng thay cho dict để gz gọn; ngày ghi một lần ở đầu file.
FIELDS = ("time", "price", "vol", "side", "acc")
# Mã thanh khoản cao để nhận ra ngày nghỉ trước khi quét cả nghìn mã.
PROBE = ("FPT", "HPG", "VCB", "SSI", "MBB")
CLOSE_AFTER = "14:50"


def dump(path: Path, obj, pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(obj, ensure_ascii=False, indent=1) if pretty else \
        json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    path.write_text(txt, encoding="utf-8")


# ---------------------------------------------------------------- định dạng tệp
def pack_ticks(ticks: list[dict]) -> bytes:
    """Tick (cũ → mới, cùng một ngày) → gz. mtime=0 + không tên: cùng tick thì cùng byte."""
    doc = {"date": ticks[-1]["date"], "fields": list(FIELDS), "ticks": [[t[k] for k in FIELDS] for t in ticks]}
    raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9) as gz:
        gz.write(raw)
    return buf.getvalue()


def unpack_ticks(data: bytes) -> list[dict]:
    """Ngược của pack_ticks: tick dạng dict như vndirect.parse_rows (có "date")."""
    doc = json.loads(gzip.decompress(data).decode("utf-8"))
    return [{"date": doc["date"], **dict(zip(doc["fields"], row))} for row in doc["ticks"]]


def write_ticks(path: Path, ticks: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pack_ticks(ticks))


def read_ticks(path: Path) -> list[dict]:
    return unpack_ticks(path.read_bytes())


def build_tar(day: str) -> Path:
    """data/ticks/<ngày>/*.json.gz → data/rel/ticks-<ngày>.tar, tất định (thứ tự tên, mtime 0) — gz bên trong đã nén."""
    out = REL / f"ticks-{day}.tar"
    out.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(out, "w", format=tarfile.USTAR_FORMAT) as tar:
        for p in sorted((TICKS / day).glob("*.json.gz")):
            data = p.read_bytes()
            info = tarfile.TarInfo(f"{day}/{p.name}")
            info.size, info.mtime, info.mode = len(data), 0, 0o644
            tar.addfile(info, io.BytesIO(data))
    return out


def restore_tar(day: str) -> int:
    """Lượt bù: workflow đã tải tệp Release của ngày về data/rel → bung lại vào data/ticks/<ngày>."""
    src = REL / f"ticks-{day}.tar"
    if not src.exists():
        return 0
    n = 0
    with tarfile.open(src) as tar:
        for m in tar.getmembers():
            name = Path(m.name).name
            if m.isfile() and name.endswith(".json.gz") and "/" not in name:
                dst = TICKS / day / name
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(tar.extractfile(m).read())
                n += 1
    return n


def summary(ticks: list[dict], ex: str) -> dict:
    buy = sum(t["vol"] for t in ticks if SIDE_INDEX.get(t["side"]) == "b")
    sell = sum(t["vol"] for t in ticks if SIDE_INDEX.get(t["side"]) == "s")
    total = sum(t["vol"] for t in ticks)
    acc = max(t["acc"] for t in ticks)
    row = {"ex": ex, "ticks": len(ticks), "total": total, "buy": buy, "sell": sell, "gap": max(0, acc - total),
           "close": ticks[-1]["price"]}
    if not any(t["side"] in SIDE_INDEX for t in ticks):
        row["no_side"] = True
    return row


# ---------------------------------------------------------------- gom
def load_day(day: str) -> dict:
    f = DAYS / f"{day}.json"
    if f.exists():
        return json.loads(f.read_text(encoding="utf-8"))
    return {"date": day, "symbols": {}, "none": [], "failed": {}}


def collect(items: list[dict], day: str, force: bool, client, now_hm: str = "23:59") -> dict:
    """Gom các mã chưa có trong mục lục ngày `day`. client có latest_session(sym) — test truyền bản giả.
    Trả mục lục đã cập nhật + khoá phụ: new (list), holiday (bool), unsettled (list)."""
    doc = load_day(day)
    done = set() if force else set(doc["symbols"]) | set(doc["none"])
    todo = [it for it in items if it["symbol"] not in done]
    new, unsettled = [], []
    failed: dict[str, str] = {}
    none = set(doc["none"]) if not force else set()
    syms = dict(doc["symbols"]) if not force else {}

    # Ngày nghỉ: thử vài mã thanh khoản cao trước; không mã nào có phiên hôm nay → dừng, không ghi gì.
    if not syms:
        probe = [it for it in items if it["symbol"] in PROBE] or items[:5]
        if probe and not any((t := client.latest_session(it["symbol"])) and t[-1]["date"] == day for it in probe):
            return {**doc, "new": [], "holiday": True, "unsettled": []}

    for k, it in enumerate(todo, 1):
        sym, ex = it["symbol"], it.get("exchange") or "HOSE"
        ticks = client.latest_session(sym)
        if not ticks:
            if vndirect.last_error:
                failed[sym] = vndirect.last_error
            else:
                none.add(sym)          # nguồn rỗng = không khớp lệnh nào
        elif ticks[-1]["date"] != day:
            none.add(sym)              # phiên cũ của mã không giao dịch hôm nay
        elif not settled(ticks, day) or now_hm < CLOSE_AFTER:
            unsettled.append(sym)      # chạy nhầm trong giờ phiên
        else:
            write_ticks(TICKS / day / f"{sym}.json.gz", ticks)
            syms[sym] = summary(ticks, ex)
            none.discard(sym)
            new.append(sym)
        if k % 100 == 0:
            logger.info("… %d/%d mã (mới %d, không GD %d, lỗi %d)", k, len(todo), len(new), len(none), len(failed))

    out = {"date": day, "symbols": dict(sorted(syms.items())), "none": sorted(none), "failed": failed}
    return {**out, "new": new, "holiday": False, "unsettled": unsettled}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--only", default="", help="danh sách mã cách nhau dấu phẩy (thử)")
    a = ap.parse_args(argv)

    now = datetime.now(TZ)
    day, now_hm = now.date().isoformat(), now.strftime("%H:%M")
    if now_hm < CLOSE_AFTER:
        print(f"Mới {now_hm} — phiên {day} chưa xong, không gom (tick trước giờ này là của phiên dở dang hoặc phiên cũ)")
        return 0
    items, src = symbols.load()
    if a.only:
        want = {s.strip().upper() for s in a.only.split(",") if s.strip()}
        items = [it for it in items if it["symbol"] in want]
    if not items:
        print("Không có danh sách mã (VNDirect lỗi và chưa có bản chụp)")
        return 2
    restored = restore_tar(day)
    print(f"Danh sách {len(items)} mã ({src}) · phiên {day}" + (f" · khôi phục {restored} mã từ Release" if restored else ""))

    with vndirect.VndirectClient() as c:
        res = collect(items, day, a.force, c, now_hm)
    if res["holiday"]:
        print(f"{day}: các mã mẫu {', '.join(PROBE)} không có phiên hôm nay → ngày nghỉ, không ghi gì")
        return 0

    changed = bool(res["new"]) or a.force
    idx = {k: res[k] for k in ("date", "symbols", "none", "failed")}
    idx["updated"] = now.isoformat(timespec="seconds")
    idx["count"] = {"traded": len(res["symbols"]), "none": len(res["none"]), "failed": len(res["failed"]),
                    "list": len(items)}
    if changed or res["failed"] or not (DAYS / f"{day}.json").exists():
        dump(DAYS / f"{day}.json", idx)
    if changed:
        tar = build_tar(day)
        print(f"Đóng gói {tar.name}: {tar.stat().st_size / 1e6:.1f} MB")
    days = sorted(p.stem for p in DAYS.glob("*.json"))
    dump(DATA / "state.json", {"run_at": idx["updated"], "list_source": src, "day": day, **idx["count"],
                               "new": len(res["new"]), "unsettled": res["unsettled"][:50],
                               "failed": dict(list(res["failed"].items())[:50]), "sessions": len(days),
                               "first": days[0] if days else None}, pretty=True)
    gh_out = os.getenv("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"changed={'true' if changed else 'false'}\nday={day}\n")
    print(f"phiên {day}: mới {len(res['new'])} · có GD {idx['count']['traded']} · không GD {idx['count']['none']} · "
          f"lỗi {idx['count']['failed']} · chưa xong phiên {len(res['unsettled'])} · kho {len(days)} phiên")
    return 1 if res["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())

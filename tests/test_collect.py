"""Chạy: venv\\Scripts\\python -m pytest -q

Phiên vàng FPT 25/09/2026: tick thô từ order-flow-lab (6.689 tick) đã nén theo định dạng gz của kho này, và bản ghi
store tương ứng chép từ order-flow/data/store — tick lưu ở đây phải dựng ra ĐÚNG bản ghi đó (không mất thông tin).
"""
import json
import tarfile
from datetime import datetime
from pathlib import Path

import pytest

from common import vndirect
from flow.ticks import session_record
from job import collect as C

FIX = Path(__file__).resolve().parent / "fixtures"
GOLD = C.read_ticks(FIX / "FPT_2026-09-25.json.gz")
GOLD_STORE = FIX / "FPT_2026-09-25.store.json"
DAY = "2026-09-25"


def tk(time, price, vol, side, acc, date=DAY):
    return {"date": date, "time": time, "price": price, "vol": vol, "side": side, "acc": acc}


SMALL = [tk("09:15:00", 20.0, 100, "ATO", 100), tk("10:00:00", 20.1, 200, "PS", 300),
         tk("10:01:00", 20.0, 300, "PB", 600), tk("14:45:00", 20.0, 400, "ATC", 1000)]


class FakeClient:
    """by_sym: mã → tick; mã trong `errors` giả lỗi HTTP (last_error), mã không có trong by_sym = nguồn rỗng."""

    def __init__(self, by_sym, errors=()):
        self.by_sym, self.errors, self.calls = by_sym, set(errors), []

    def latest_session(self, sym):
        self.calls.append(sym)
        vndirect.last_error = f"{sym}: 503" if sym in self.errors else ""
        if sym in self.errors:
            return []
        return [dict(t) for t in self.by_sym.get(sym, [])]


@pytest.fixture
def kho(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "TICKS", tmp_path / "ticks")
    monkeypatch.setattr(C, "DAYS", tmp_path / "days")
    monkeypatch.setattr(C, "REL", tmp_path / "rel")
    return tmp_path


def items(*syms):
    return [{"symbol": s, "exchange": "HOSE"} for s in syms]


def save_index(res):
    C.dump(C.DAYS / f"{res['date']}.json", {k: res[k] for k in ("date", "symbols", "none", "failed")})


def test_gz_roundtrip_exact():
    assert C.unpack_ticks(C.pack_ticks(SMALL)) == SMALL


def test_gz_bytes_deterministic():
    assert C.pack_ticks(SMALL) == C.pack_ticks([dict(t) for t in SMALL])


def test_golden_fpt_ticks_rebuild_order_flow_store():
    assert len(GOLD) == 6689
    assert session_record(GOLD) == json.loads(GOLD_STORE.read_text(encoding="utf-8"))


def test_collect_classifies_traded_none_stale_failed(kho):
    old = [dict(t, date="2026-09-24") for t in SMALL]  # mã không GD hôm nay: nguồn trả phiên cũ
    cl = FakeClient({"FPT": GOLD, "OLD": old}, errors={"ERR"})
    res = C.collect(items("FPT", "OLD", "EMPTY", "ERR"), DAY, False, cl)
    assert res["new"] == ["FPT"] and not res["holiday"]
    assert res["none"] == ["EMPTY", "OLD"]
    assert list(res["failed"]) == ["ERR"]
    s = res["symbols"]["FPT"]
    assert (s["ticks"], s["total"]) == (6689, sum(t["vol"] for t in GOLD))
    assert C.read_ticks(kho / "ticks" / DAY / "FPT.json.gz") == GOLD
    assert not (kho / "ticks" / DAY / "OLD.json.gz").exists()


def test_backup_run_only_retries_missing(kho):
    cl = FakeClient({"FPT": GOLD, "VCB": SMALL}, errors={"VCB"})
    res = C.collect(items("FPT", "VCB", "EMPTY"), DAY, False, cl)
    save_index(res)
    cl2 = FakeClient({"FPT": GOLD, "VCB": SMALL})
    res2 = C.collect(items("FPT", "VCB", "EMPTY"), DAY, False, cl2)
    assert cl2.calls == ["VCB"]                         # FPT đã có, EMPTY đã biết không GD
    assert res2["new"] == ["VCB"] and not res2["failed"]
    assert set(res2["symbols"]) == {"FPT", "VCB"}


def test_holiday_writes_nothing(kho):
    stale = [dict(t, date="2026-09-01") for t in SMALL]
    res = C.collect(items("FPT", "HPG", "AAA"), DAY, False, FakeClient({"FPT": stale, "HPG": stale, "AAA": stale}))
    assert res["holiday"] and not res["new"]
    assert not (kho / "ticks").exists()


def test_quiet_symbol_without_atc_is_saved_after_close(kho):
    # ABT 30/09/2026: lệnh cuối 13:44:05, không khớp ATC — phiên đã xong, phải lưu
    quiet = [tk("10:05:00", 49.8, 300, "PS", 300), tk("13:44:05", 49.9, 100, "PS", 400)]
    res = C.collect(items("ABT"), DAY, False, FakeClient({"ABT": quiet}), now_hm="17:00")
    assert res["new"] == ["ABT"] and not res["unsettled"]


def test_unsettled_session_not_saved(kho):
    morning = SMALL[:2]
    res = C.collect(items("FPT"), DAY, False, FakeClient({"FPT": morning}), now_hm="11:35")
    assert res["unsettled"] == ["FPT"] and not res["new"]


def test_tar_deterministic_and_restore(kho):
    C.collect(items("FPT", "VCB"), DAY, False, FakeClient({"FPT": GOLD, "VCB": SMALL}))
    a = C.build_tar(DAY).read_bytes()
    assert C.build_tar(DAY).read_bytes() == a         # đóng gói lại không đổi byte
    with tarfile.open(C.REL / f"ticks-{DAY}.tar") as t:
        assert t.getnames() == [f"{DAY}/FPT.json.gz", f"{DAY}/VCB.json.gz"]
    for p in (kho / "ticks" / DAY).glob("*"):
        p.unlink()
    assert C.restore_tar(DAY) == 2
    assert C.read_ticks(kho / "ticks" / DAY / "FPT.json.gz") == GOLD


def at(d, hm):
    return datetime.fromisoformat(f"{d}T{hm}:00+07:00")


def test_session_day_after_midnight_takes_previous_session():
    # cron 20:00 bị GitHub chạy lúc 00:30 hôm sau: nguồn còn phiên D → gom vào D, không coi là ngày nghỉ
    cl = FakeClient({"FPT": SMALL, "HPG": SMALL})
    assert C.session_day(items("FPT", "HPG"), cl, at("2026-09-26", "00:30")) == (DAY, "")


def test_session_day_during_session_waits():
    day, why = C.session_day(items("FPT"), FakeClient({"FPT": SMALL}), at(DAY, "11:00"))
    assert day is None and "đang diễn ra" in why


def test_session_day_source_empty():
    day, why = C.session_day(items("FPT", "HPG"), FakeClient({}), at(DAY, "16:00"))
    assert day is None

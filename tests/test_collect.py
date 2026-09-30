"""Chạy: venv\\Scripts\\python -m pytest -q

Phiên vàng FPT 25/09/2026: tick thô từ order-flow-lab (6.689 tick) đã nén theo định dạng gz của kho này, và bản ghi
store tương ứng chép từ order-flow/data/store — bộ gom phải dựng ra ĐÚNG bản ghi đó.
"""
import json
from datetime import datetime
from pathlib import Path

import pytest

from common.config import TZ
from flow.ticks import session_record, settled
from job import collect as C

FIX = Path(__file__).resolve().parent / "fixtures"
GOLD_TICKS = FIX / "FPT_2026-09-25.json.gz"
GOLD_STORE = FIX / "FPT_2026-09-25.store.json"


def tk(time, price, vol, side, acc, date="2026-09-25"):
    return {"date": date, "time": time, "price": price, "vol": vol, "side": side, "acc": acc}


class FakeClient:
    def __init__(self, by_sym):
        self.by_sym = by_sym

    def latest_session(self, sym):
        return [dict(t) for t in self.by_sym.get(sym, [])]


@pytest.fixture
def kho(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "TICKS", tmp_path / "ticks")
    monkeypatch.setattr(C, "STORE", tmp_path / "store")
    return tmp_path


def snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_gz_roundtrip_exact(tmp_path):
    t = [tk("09:15:00", 65.5, 300, "ATO", 300), tk("09:15:03", 65.6, 1200, "PS", 1500),
         tk("14:45:00", 65.4, 900, "ATC", 2400)]
    p = tmp_path / "x.json.gz"
    C.write_ticks(p, t)
    assert C.read_ticks(p) == t


def test_gz_bytes_deterministic(tmp_path):
    # mtime=0: ghi hai lần cùng tick → cùng byte, git không thấy diff ở lượt cron dự phòng
    t = [tk("10:00:00", 30.0, 100, "PB", 100)]
    a, b = tmp_path / "a.gz", tmp_path / "b.gz"
    C.write_ticks(a, t)
    C.write_ticks(b, t)
    assert a.read_bytes() == b.read_bytes()


def test_golden_fpt_store_matches_order_flow():
    ticks = C.read_ticks(GOLD_TICKS)
    assert len(ticks) == 6689
    assert session_record(ticks) == json.loads(GOLD_STORE.read_text(encoding="utf-8"))


def test_collect_writes_ticks_and_store(kho):
    ticks = C.read_ticks(GOLD_TICKS)
    res = C.collect([{"symbol": "FPT"}], False, FakeClient({"FPT": ticks}))
    assert res["new"] == ["FPT"] and res["day"] == "2026-09-25"
    assert C.read_ticks(kho / "ticks" / "FPT" / "2026-09-25.json.gz") == ticks
    assert json.loads((kho / "store" / "FPT" / "2026-09-25.json").read_text(encoding="utf-8")) == session_record(ticks)


def test_collect_idempotent(kho):
    cl = FakeClient({"FPT": C.read_ticks(GOLD_TICKS)})
    C.collect([{"symbol": "FPT"}], False, cl)
    before = snapshot(kho)
    res = C.collect([{"symbol": "FPT"}], False, cl)
    assert res["have"] == ["FPT"] and not res["new"]
    assert snapshot(kho) == before
    C.collect([{"symbol": "FPT"}], True, cl)  # --force ghi lại nhưng cùng byte
    assert snapshot(kho) == before


def test_collect_skips_unsettled_session(kho):
    today = datetime.now(TZ).date().isoformat()
    morning = [tk("09:15:00", 20.0, 100, "ATO", 100, today), tk("10:30:00", 20.1, 200, "PS", 300, today)]
    assert not settled(morning, today)
    res = C.collect([{"symbol": "ABC"}], False, FakeClient({"ABC": morning}))
    assert res["unsettled"] == ["ABC"] and not res["new"]
    assert not (kho / "ticks").exists() and not (kho / "store").exists()


def test_collect_failed_symbol_does_not_block_others(kho):
    res = C.collect([{"symbol": "XXX"}, {"symbol": "FPT"}], False,
                    FakeClient({"FPT": C.read_ticks(GOLD_TICKS)}))
    assert list(res["failed"]) == ["XXX"] and res["new"] == ["FPT"]

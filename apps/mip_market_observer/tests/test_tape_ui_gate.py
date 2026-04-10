"""Binary tape UI gate + dwell."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.tape_ui_gate import (
    strict_tape_ui_raw,
    tape_ui_inactive_reason_code,
    update_tape_ui_dwell,
)


class _Rt:
    tape_ui_since = None
    tape_ui_snap_streak = 0
    tape_ui_last_log_ts = None


def _base_kw():
    return dict(
        feed_health="live",
        warmup_state="ready",
        quote_sizes_available=True,
        side_confidence_aggregate="high",
        buy_volume_60s=100.0,
        sell_volume_60s=40.0,
        ib_connected=True,
        simulate_mode=False,
    )


def test_strict_ok():
    assert strict_tape_ui_raw(**_base_kw()) is True


def test_strict_ib_disconnected_non_sim():
    kw = _base_kw()
    kw["ib_connected"] = False
    kw["simulate_mode"] = False
    assert strict_tape_ui_raw(**kw) is False
    assert "ib_not_connected" in tape_ui_inactive_reason_code(**kw)


def test_strict_feed_not_live():
    kw = _base_kw()
    kw["feed_health"] = "delayed"
    assert strict_tape_ui_raw(**kw) is False


def test_strict_warmup():
    kw = _base_kw()
    kw["warmup_state"] = "warming"
    assert strict_tape_ui_raw(**kw) is False


def test_strict_simulate_without_ib():
    kw = _base_kw()
    kw["ib_connected"] = False
    kw["simulate_mode"] = True
    assert strict_tape_ui_raw(**kw) is True


def test_dwell_requires_time_and_snapshots(monkeypatch):
    monkeypatch.setattr("app.tape_ui_gate.TAPE_UI_DWELL_SEC", 5.0)
    monkeypatch.setattr("app.tape_ui_gate.TAPE_UI_MIN_SNAPSHOTS", 2)
    rt = _Rt()
    t0 = datetime(2026, 4, 10, 17, 0, 0, tzinfo=timezone.utc)
    assert update_tape_ui_dwell(rt, t0, True) is False
    assert rt.tape_ui_snap_streak == 1
    t1 = t0 + timedelta(seconds=2)
    assert update_tape_ui_dwell(rt, t1, True) is False
    assert rt.tape_ui_snap_streak == 2
    t2 = t0 + timedelta(seconds=6)
    assert update_tape_ui_dwell(rt, t2, True) is True


def test_dwell_resets_on_strict_fail(monkeypatch):
    monkeypatch.setattr("app.tape_ui_gate.TAPE_UI_DWELL_SEC", 0.0)
    monkeypatch.setattr("app.tape_ui_gate.TAPE_UI_MIN_SNAPSHOTS", 1)
    rt = _Rt()
    t0 = datetime(2026, 4, 10, 17, 0, 0, tzinfo=timezone.utc)
    assert update_tape_ui_dwell(rt, t0, True) is True
    assert update_tape_ui_dwell(rt, t0, False) is False
    assert rt.tape_ui_snap_streak == 0
    assert rt.tape_ui_since is None

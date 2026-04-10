"""Per-symbol tape state, snapshot build, subscription bookkeeping."""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.feed_health import combine_worst, tier_from_age_sec
from app.engine.classifier_phase1 import classify_phase1_raw
from app.engine.hysteresis import ChipHysteresis, LabelHysteresis
from app.engine.trade_side import infer_trade_side
from app.session_regime import opening_price_discovery_window
from app.threshold_profile import (
    BASELINE_DEQUE_MAX,
    BASELINE_LOW_MAX,
    BASELINE_MED_MAX,
    DISCONNECTED_GRACE_SEC,
    IDLE_UNSUBSCRIBE_SEC,
    SYMBOL_GRACE_SEC,
    THRESHOLD_PROFILE_VERSION,
)
from app.warmup import WarmupInputs, compute_warmup_state, utc_now

SNAPSHOT_SCHEMA_VERSION = "1.0.0"

_PRUNE_SEC = 200.0
_EPS = 1e-9


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


@dataclass
class SymbolRuntime:
    symbol: str
    trades: deque = field(default_factory=lambda: deque(maxlen=50_000))
    quotes: deque = field(default_factory=lambda: deque(maxlen=20_000))
    first_event_ts: datetime | None = None
    trade_count_session: int = 0
    baseline_volumes: deque = field(default_factory=lambda: deque(maxlen=BASELINE_DEQUE_MAX))
    baseline_sample_count: int = 0
    last_baseline_bucket: int | None = None
    created_ts: datetime = field(default_factory=utc_now)
    label_hyst: LabelHysteresis = field(default_factory=LabelHysteresis)
    chip_hyst: ChipHysteresis = field(default_factory=ChipHysteresis)
    last_quote_sizes: bool = False
    last_touch_ts: datetime = field(default_factory=utc_now)

    def touch_poll(self) -> None:
        self.last_touch_ts = utc_now()

    def _bump_baseline(self, vol_60: float, now: datetime) -> None:
        bucket = int(now.timestamp()) // 60
        if self.last_baseline_bucket is None:
            self.last_baseline_bucket = bucket
            return
        if bucket != self.last_baseline_bucket:
            self.baseline_volumes.append(vol_60)
            self.last_baseline_bucket = bucket
        self.baseline_sample_count = len(self.baseline_volumes)


class TapeCoordinator:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._symbols: dict[str, SymbolRuntime] = {}
        self.ib_connected: bool = False
        self.simulate_mode: bool = False

    def set_ib_connected(self, ok: bool) -> None:
        with self._lock:
            self.ib_connected = ok

    def touch(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        if not sym:
            return
        with self._lock:
            self._ensure_symbol_unlocked(sym).touch_poll()

    def ensure_symbol(self, symbol: str) -> SymbolRuntime:
        sym = symbol.strip().upper()
        with self._lock:
            return self._ensure_symbol_unlocked(sym)

    def _ensure_symbol_unlocked(self, sym: str) -> SymbolRuntime:
        if sym not in self._symbols:
            self._symbols[sym] = SymbolRuntime(symbol=sym)
        return self._symbols[sym]

    def list_active_symbols(self) -> list[str]:
        with self._lock:
            return list(self._symbols.keys())

    def drop_symbol_if_idle(self, now: datetime | None = None) -> list[str]:
        """Remove symbols not polled recently (except grace after last touch)."""
        now = now or utc_now()
        dropped: list[str] = []
        with self._lock:
            for sym, rt in list(self._symbols.items()):
                idle = (now - rt.last_touch_ts).total_seconds()
                if idle > IDLE_UNSUBSCRIBE_SEC + SYMBOL_GRACE_SEC:
                    del self._symbols[sym]
                    dropped.append(sym)
        return dropped

    def ingest_trade(
        self,
        symbol: str,
        event_ts: datetime,
        price: float,
        size: float,
        *,
        aggressor: str | None = None,
    ) -> None:
        sym = symbol.strip().upper()
        bid = ask = None
        with self._lock:
            rt = self._ensure_symbol_unlocked(sym)
            if rt.quotes:
                _ts, bid, ask, _bs, _as = rt.quotes[-1]
            agg = None
            if aggressor in ("buy", "sell"):
                agg = aggressor
            sr = infer_trade_side(price, bid, ask, aggressor_from_feed=agg)
            rt.trades.append((event_ts, price, size, sr.side, sr.confidence))
            rt.trade_count_session += 1
            if rt.first_event_ts is None:
                rt.first_event_ts = event_ts

    def ingest_quote(
        self,
        symbol: str,
        event_ts: datetime,
        bid: float,
        ask: float,
        bid_size: float | None,
        ask_size: float | None,
    ) -> None:
        sym = symbol.strip().upper()
        with self._lock:
            rt = self._ensure_symbol_unlocked(sym)
            rt.quotes.append((event_ts, bid, ask, bid_size, ask_size))
            if bid_size is not None and ask_size is not None and bid_size + ask_size > 0:
                rt.last_quote_sizes = True
            if rt.first_event_ts is None:
                rt.first_event_ts = event_ts

    def _prune(self, rt: SymbolRuntime, now: datetime) -> None:
        cutoff = now.timestamp() - _PRUNE_SEC
        while rt.trades and rt.trades[0][0].timestamp() < cutoff:
            rt.trades.popleft()
        while rt.quotes and rt.quotes[0][0].timestamp() < cutoff:
            rt.quotes.popleft()

    def _agg_trades_60s(self, rt: SymbolRuntime, now: datetime) -> tuple[float, float, float, float, int]:
        t0 = now.timestamp() - 60.0
        total = buy_v = sell_v = unk_v = 0.0
        n = 0
        for ts, _p, sz, side, _c in rt.trades:
            if ts.timestamp() < t0:
                continue
            n += 1
            total += sz
            if side == "buy":
                buy_v += sz
            elif side == "sell":
                sell_v += sz
            else:
                unk_v += sz
        return total, buy_v, sell_v, unk_v, n

    def _side_aggregate(self, buy_v: float, sell_v: float, trades_60: list[tuple], now: datetime) -> str:
        known = buy_v + sell_v
        if known < _EPS:
            return "low"
        hi = med = 0.0
        t0 = now.timestamp() - 60.0
        for ts, _p, sz, side, conf in trades_60:
            if ts.timestamp() < t0 or side == "unknown":
                continue
            if conf == "high":
                hi += sz
            elif conf == "medium":
                med += sz
        frac_hi = hi / known
        frac_ok = (hi + med) / known
        if frac_hi >= 0.7:
            return "high"
        if frac_ok >= 0.5:
            return "medium"
        return "low"

    def _trades_list_60s(self, rt: SymbolRuntime, now: datetime) -> list:
        t0 = now.timestamp() - 60.0
        return [x for x in rt.trades if x[0].timestamp() >= t0]

    def _mid_ret_60s(self, rt: SymbolRuntime, now: datetime) -> float | None:
        if not rt.quotes:
            return None

        def mid_at(target_ts: float) -> float | None:
            best = None
            best_dt = None
            for ts, bid, ask, _bs, _as in rt.quotes:
                if bid is None or ask is None:
                    continue
                if ask < bid:
                    continue
                m = (bid + ask) / 2
                dt = abs(ts.timestamp() - target_ts)
                if best is None or dt < best_dt:
                    best = m
                    best_dt = dt
            if best is None or best_dt is None or best_dt > 15.0:
                return None
            return best

        cur = mid_at(now.timestamp())
        past = mid_at(now.timestamp() - 60.0)
        if cur is None or past is None or past < _EPS:
            return None
        return (cur - past) / past

    def build_snapshot(self, symbol: str) -> dict[str, Any]:
        sym = symbol.strip().upper()
        now = utc_now()
        with self._lock:
            rt = self._ensure_symbol_unlocked(sym)
            rt.touch_poll()
            self._prune(rt, now)

            last_trade_ts = rt.trades[-1][0] if rt.trades else None
            last_quote_ts = rt.quotes[-1][0] if rt.quotes else None

            trade_age = (now - last_trade_ts).total_seconds() if last_trade_ts else None
            quote_age = (now - last_quote_ts).total_seconds() if last_quote_ts else None

            trade_tier = tier_from_age_sec(trade_age)
            quotes_expected = last_quote_ts is not None
            quote_tier = tier_from_age_sec(quote_age) if last_quote_ts else None

            combined = combine_worst(trade_tier, quote_tier, quotes_expected)

            session_age = (now - rt.created_ts).total_seconds()
            no_events = last_trade_ts is None and last_quote_ts is None
            if not self.ib_connected and not self.simulate_mode:
                feed_health = "disconnected"
            elif no_events and session_age > DISCONNECTED_GRACE_SEC:
                feed_health = "disconnected"
            elif no_events:
                feed_health = "delayed"
            else:
                feed_health = combined

            total, buy_v, sell_v, unk_v, n60 = self._agg_trades_60s(rt, now)
            tlist = self._trades_list_60s(rt, now)
            side_agg = self._side_aggregate(buy_v, sell_v, tlist, now)

            known_side = buy_v + sell_v
            tape_signed = (buy_v - sell_v) / (known_side + _EPS) if known_side > _EPS else 0.0

            last_bid = last_ask = last_bs = last_as = None
            spread = None
            mid = None
            book_signed = None
            if rt.quotes:
                _ts, last_bid, last_ask, last_bs, last_as = rt.quotes[-1]
                if last_bid and last_ask and last_ask >= last_bid:
                    spread = last_ask - last_bid
                    mid = (last_bid + last_ask) / 2
                if (
                    last_bs is not None
                    and last_as is not None
                    and last_bs + last_as > 0
                ):
                    book_signed = (last_bs - last_as) / (last_bs + last_as)

            rt._bump_baseline(total, now)
            baseline_n = len(rt.baseline_volumes)
            if baseline_n <= BASELINE_LOW_MAX:
                baseline_conf = "low"
            elif baseline_n <= BASELINE_MED_MAX:
                baseline_conf = "medium"
            else:
                baseline_conf = "high"

            if baseline_n > 0:
                sorted_v = sorted(rt.baseline_volumes)
                med = sorted_v[len(sorted_v) // 2]
                rv = math.log1p(total + _EPS) - math.log1p(med + _EPS)
                rv_score = max(0.0, min(1.0, (rv + 0.5) / 2.0))
            else:
                rv_score = None

            wu = compute_warmup_state(
                WarmupInputs(
                    first_event_ts=rt.first_event_ts,
                    now=now,
                    trade_count_session=rt.trade_count_session,
                    baseline_sample_count=rt.baseline_sample_count,
                )
            )

            mid_ret = self._mid_ret_60s(rt, now)
            open_win = opening_price_discovery_window(now, sym)

            raw_mq = classify_phase1_raw(
                warmup_state=wu,
                baseline_confidence=baseline_conf,
                feed_health=feed_health,
                side_confidence_aggregate=side_agg,
                tape_pressure_signed=tape_signed,
                book_pressure_signed=book_signed,
                mid_ret_60s=mid_ret,
                spread=spread,
                relative_volume_score=rv_score,
                opening_price_discovery_window=open_win,
            )

            emitted = rt.label_hyst.update(raw_mq)
            chips_raw = self._chips_raw(
                emitted,
                tape_signed,
                book_signed,
                side_agg,
                wu,
                feed_health,
                rt.last_quote_sizes,
            )
            chips = list(rt.chip_hyst.update(chips_raw))

            expl = self._explain(emitted, open_win, side_agg, wu, feed_health)

            last_px = rt.trades[-1][1] if rt.trades else mid

            return {
                "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
                "threshold_profile_version": THRESHOLD_PROFILE_VERSION,
                "symbol": sym,
                "snapshot_ts": _iso(now),
                "feed_health": feed_health,
                "warmup_state": wu,
                "baseline_confidence": baseline_conf,
                "last_price": last_px,
                "mid_price": mid,
                "spread": spread,
                "quote_sizes_available": bool(
                    last_bs is not None and last_as is not None and (last_bs + last_as) > 0
                ),
                "volume_60s": total,
                "buy_volume_60s": buy_v if side_agg == "high" else None,
                "sell_volume_60s": sell_v if side_agg == "high" else None,
                "unknown_volume_60s": unk_v if side_agg == "high" else None,
                "side_confidence_aggregate": side_agg,
                "relative_volume_score": rv_score,
                "tape_pressure_score": tape_signed,
                "book_pressure_score": book_signed,
                "move_quality": emitted,
                "move_quality_raw": raw_mq,
                "opening_price_discovery_window": open_win,
                "explanation_short": expl[0],
                "explanation_long": expl[1],
                "active_chips": chips,
                "last_trade_age_sec": trade_age,
                "last_quote_age_sec": quote_age,
                "baseline_sample_count": rt.baseline_sample_count,
                "trades_60s_count": n60,
            }

    def _chips_raw(
        self,
        mq: str,
        tape: float,
        book: float | None,
        side: str,
        warmup: str,
        health: str,
        quote_sizes: bool,
    ) -> list[str]:
        chips: list[str] = []
        if warmup != "ready":
            chips.append("Warming up")
        if health == "delayed":
            chips.append("Tape delayed")
        elif health == "stale":
            chips.append("Tape stale")
        elif health == "disconnected":
            chips.append("Tape offline")
        if side in ("medium", "low"):
            chips.append("Low-confidence side")
        if not quote_sizes and health != "disconnected":
            chips.append("Quote sizes N/A")
        if tape > 0.35:
            chips.append("Buyer-led tape")
        elif tape < -0.35:
            chips.append("Seller-led tape")
        if book is not None and book > 0.35:
            chips.append("Heavy bid L1")
        elif book is not None and book < -0.35:
            chips.append("Heavy ask L1")
        if mq in ("directional_push_up", "directional_push_down"):
            chips.append("Tape push")
        # cap 4
        return chips[:6]

    def _explain(
        self,
        mq: str,
        open_win: bool,
        side: str,
        warmup: str,
        health: str,
    ) -> tuple[str, str]:
        prefix = "Opening window — interpretations are conservative. " if open_win else ""
        if mq == "insufficient_evidence":
            return (
                prefix + "Tape: not enough reliable evidence yet.",
                prefix
                + f"Warmup={warmup}, feed={health}, side confidence={side}. Wait for more tape and baseline before reading direction.",
            )
        if mq == "neutral_chop":
            return (
                prefix + "Tape: mixed or quiet — no clear push.",
                prefix + "Executed flow and mid drift do not line up for a directional push label.",
            )
        if mq == "directional_push_up":
            return (
                prefix + "Tape: upward push on executed flow.",
                prefix + "Buy-led volume and short-horizon mid drift support a directional read (advisory only).",
            )
        return (
            prefix + "Tape: downward push on executed flow.",
            prefix + "Sell-led volume and short-horizon mid drift support a directional read (advisory only).",
        )

    def _empty_snapshot(self, sym: str, now: datetime, *, reason: str) -> dict[str, Any]:
        return {
            "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
            "threshold_profile_version": THRESHOLD_PROFILE_VERSION,
            "symbol": sym,
            "snapshot_ts": _iso(now),
            "feed_health": "disconnected",
            "warmup_state": "cold",
            "baseline_confidence": "low",
            "last_price": None,
            "mid_price": None,
            "spread": None,
            "quote_sizes_available": False,
            "volume_60s": 0.0,
            "buy_volume_60s": None,
            "sell_volume_60s": None,
            "unknown_volume_60s": None,
            "side_confidence_aggregate": "low",
            "relative_volume_score": None,
            "tape_pressure_score": 0.0,
            "book_pressure_score": None,
            "move_quality": "insufficient_evidence",
            "move_quality_raw": "insufficient_evidence",
            "opening_price_discovery_window": opening_price_discovery_window(now, sym),
            "explanation_short": "Tape: offline or symbol not active.",
            "explanation_long": reason,
            "active_chips": ["Tape offline"],
            "last_trade_age_sec": None,
            "last_quote_age_sec": None,
            "baseline_sample_count": 0,
            "trades_60s_count": 0,
        }


# global singleton
coordinator = TapeCoordinator()

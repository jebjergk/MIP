"""Server-primary hysteresis for move_quality (and chip set)."""

from __future__ import annotations

from dataclasses import dataclass, field

from app.threshold_profile import HYSTERESIS_SNAPSHOTS


@dataclass
class LabelHysteresis:
    last_emitted: str = "insufficient_evidence"
    pending_raw: str | None = None
    pending_count: int = 0

    def update(self, raw: str) -> str:
        if raw == self.last_emitted:
            self.pending_raw = None
            self.pending_count = 0
            return self.last_emitted
        if raw == self.pending_raw:
            self.pending_count += 1
        else:
            self.pending_raw = raw
            self.pending_count = 1
        if self.pending_count >= HYSTERESIS_SNAPSHOTS:
            self.last_emitted = raw
            self.pending_raw = None
            self.pending_count = 0
        return self.last_emitted


@dataclass
class ChipHysteresis:
    last_chips: tuple[str, ...] = ()
    pending_chips: tuple[str, ...] | None = None
    pending_count: int = 0

    def update(self, raw_chips: list[str]) -> tuple[str, ...]:
        t = tuple(raw_chips[:4])
        if t == self.last_chips:
            self.pending_chips = None
            self.pending_count = 0
            return self.last_chips
        if t == self.pending_chips:
            self.pending_count += 1
        else:
            self.pending_chips = t
            self.pending_count = 1
        if self.pending_count >= HYSTERESIS_SNAPSHOTS:
            self.last_chips = t
            self.pending_chips = None
            self.pending_count = 0
        return self.last_chips

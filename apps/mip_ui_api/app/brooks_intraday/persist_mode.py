"""Phase C persist mode flag. Defaults to legacy until bulk is explicitly accepted."""

from __future__ import annotations

import os

PERSIST_MODE_LEGACY = "legacy"
PERSIST_MODE_BULK = "bulk"
ENV_PERSIST_MODE = "BROOKS_PERSIST_MODE"


def get_persist_mode() -> str:
    raw = (os.environ.get(ENV_PERSIST_MODE) or PERSIST_MODE_LEGACY).strip().lower()
    if raw == PERSIST_MODE_BULK:
        return PERSIST_MODE_BULK
    return PERSIST_MODE_LEGACY


def is_bulk_persist() -> bool:
    return get_persist_mode() == PERSIST_MODE_BULK

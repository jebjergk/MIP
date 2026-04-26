"""
IBKR-facing application services.

These modules provide importable, fail-soft Python APIs over the IBKR
subprocess scripts under cursorfiles/. UI routers should call these
helpers instead of constructing subprocess command-lines directly.
"""

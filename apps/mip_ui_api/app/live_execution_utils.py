from datetime import datetime, timezone


def to_dt_utc(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        try:
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        try:
            parsed = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None
    try:
        parsed = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def is_close_like_execution(action_intent: str | None, action_side: str | None, execution_side: str | None) -> bool:
    intent = str(action_intent or "").upper().strip()
    a_side = str(action_side or "").upper().strip()
    e_side = str(execution_side or "").upper().strip()
    if intent == "EXIT":
        return e_side in {"BUY", "SELL"}
    if intent == "ENTRY" and a_side in {"BUY", "SELL"} and e_side in {"BUY", "SELL"}:
        return a_side != e_side
    return False

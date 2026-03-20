import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root (mip_0.7/), not MIP/
env_path = Path(__file__).resolve().parent.parent.parent.parent.parent / ".env"
load_dotenv(env_path)


def get_snowflake_config():
    auth_method = (os.getenv("SNOWFLAKE_AUTH_METHOD") or "password").strip().lower()
    if auth_method not in ("password", "keypair"):
        auth_method = "password"
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "auth_method": auth_method,
        "private_key_path": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or None,
        "private_key_passphrase": os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }


def training_debug_enabled() -> bool:
    """True if GET /training/status/debug is allowed (dev-only). Set ENABLE_TRAINING_DEBUG=1."""
    return (os.getenv("ENABLE_TRAINING_DEBUG") or "").strip().lower() in ("1", "true", "yes")


def get_askmip_model() -> str:
    """LLM model name for the Ask MIP feature. Default: claude-3-5-sonnet. Set ASKMIP_MODEL to override."""
    return (os.getenv("ASKMIP_MODEL") or "claude-3-5-sonnet").strip()


def askmip_enable_glossary() -> bool:
    return (os.getenv("ASKMIP_ENABLE_GLOSSARY") or "1").strip().lower() in ("1", "true", "yes")


def askmip_enable_web_fallback() -> bool:
    return (os.getenv("ASKMIP_ENABLE_WEB_FALLBACK") or "0").strip().lower() in ("1", "true", "yes")


def askmip_doc_min_confidence() -> float:
    try:
        return float((os.getenv("ASKMIP_DOC_MIN_CONFIDENCE") or "0.65").strip())
    except ValueError:
        return 0.65


def askmip_glossary_min_confidence() -> float:
    try:
        return float((os.getenv("ASKMIP_GLOSSARY_MIN_CONFIDENCE") or "0.60").strip())
    except ValueError:
        return 0.60


def askmip_max_didyoumean() -> int:
    try:
        return max(1, int((os.getenv("ASKMIP_MAX_DIDYOUMEAN") or "5").strip()))
    except ValueError:
        return 5


def askmip_web_allowed_intents() -> set[str]:
    raw = (os.getenv("ASKMIP_WEB_FALLBACK_ALLOWED_INTENTS") or "trading_concept,market_research_concept,term_definition,mixed").strip()
    return {part.strip() for part in raw.split(",") if part.strip()}

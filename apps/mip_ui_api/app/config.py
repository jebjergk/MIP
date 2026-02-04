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

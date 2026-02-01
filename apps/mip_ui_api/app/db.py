import snowflake.connector

from app.config import get_snowflake_config


class SnowflakeAuthError(Exception):
    """Raised when Snowflake authentication fails (e.g. MFA required, invalid creds)."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.original = original


def get_connection():
    """Read-only Snowflake connection from env. No writes from this API."""
    cfg = get_snowflake_config()
    base_params = {
        "account": cfg["account"],
        "user": cfg["user"],
        "role": cfg["role"],
        "warehouse": cfg["warehouse"],
        "database": cfg["database"],
        "schema": cfg["schema"],
    }

    auth_method = cfg.get("auth_method") or "password"
    if auth_method == "keypair":
        key_path = cfg.get("private_key_path")
        if not key_path:
            raise SnowflakeAuthError(
                "SNOWFLAKE_AUTH_METHOD=keypair but SNOWFLAKE_PRIVATE_KEY_PATH is not set."
            )
        base_params["authenticator"] = "SNOWFLAKE_JWT"
        base_params["private_key_file"] = key_path
        passphrase = cfg.get("private_key_passphrase")
        if passphrase:
            base_params["private_key_file_pwd"] = passphrase
    else:
        base_params["password"] = cfg["password"]

    try:
        return snowflake.connector.connect(**base_params)
    except SnowflakeAuthError:
        raise
    except (FileNotFoundError, OSError) as e:
        if auth_method == "keypair":
            raise SnowflakeAuthError(
                f"Private key file not found or unreadable: {key_path}",
                original=e,
            ) from e
        raise
    except Exception as e:
        err_msg = str(e).lower()
        if any(
            x in err_msg
            for x in (
                "authentication",
                "auth",
                "250001",  # Snowflake auth error code
                "mfa",
                "multi-factor",
                "invalid credentials",
                "incorrect username or password",
            )
        ):
            raise SnowflakeAuthError(
                "Snowflake auth failed — MFA users must use keypair or OAuth.",
                original=e,
            ) from e
        raise


def fetch_all(cursor):
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def serialize_row(row):
    """Convert row dict for JSON: handle dates, decimals, etc."""
    if row is None:
        return None
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, bool)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = str(v) if v is not None else None
        else:
            out[k] = v
    return out


def serialize_rows(rows):
    return [serialize_row(r) for r in rows] if rows else []

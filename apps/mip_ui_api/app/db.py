import queue
import threading
import time

import snowflake.connector

from app.config import get_snowflake_config

# ---------------------------------------------------------------------------
# Connection pool settings
# ---------------------------------------------------------------------------
_POOL_MAX_SIZE = 4          # Max idle connections kept in the pool
_IDLE_HEALTH_SEC = 300      # Seconds before an idle connection gets a health-check on checkout


class SnowflakeAuthError(Exception):
    """Raised when Snowflake authentication fails (e.g. MFA required, invalid creds)."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.original = original


# ---------------------------------------------------------------------------
# Internal pool machinery
# ---------------------------------------------------------------------------

class _PoolEntry:
    """Wraps a raw Snowflake connection with a last-used timestamp."""
    __slots__ = ("conn", "last_used")

    def __init__(self, conn):
        self.conn = conn
        self.last_used = time.monotonic()


class _ConnectionPool:
    """
    Simple thread-safe connection pool for Snowflake.

    * acquire() returns a raw connection (reused or freshly created).
    * release() returns a connection to the idle queue (or closes it if full).
    * Idle connections older than _IDLE_HEALTH_SEC get a SELECT 1 ping
      before being handed out; recently used ones skip the check.
    """

    def __init__(self, max_size: int = _POOL_MAX_SIZE):
        self._max_size = max_size
        self._idle: queue.Queue[_PoolEntry] = queue.Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._created = 0

    # -- internal helpers ---------------------------------------------------

    @staticmethod
    def _make_connection():
        """Create a brand-new Snowflake connection (expensive)."""
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

    @staticmethod
    def _is_alive(entry: _PoolEntry) -> bool:
        """Return True if the pooled connection is still usable."""
        # Skip the health-check if the connection was used recently
        if time.monotonic() - entry.last_used < _IDLE_HEALTH_SEC:
            return True
        try:
            entry.conn.cursor().execute("SELECT 1")
            return True
        except Exception:
            return False

    # -- public API ---------------------------------------------------------

    def acquire(self):
        """Get a connection from the pool or create a new one."""
        # Drain idle connections until we find a live one
        while True:
            try:
                entry = self._idle.get_nowait()
            except queue.Empty:
                break
            if self._is_alive(entry):
                entry.last_used = time.monotonic()
                return entry.conn
            # Dead connection - discard
            try:
                entry.conn.close()
            except Exception:
                pass
            with self._lock:
                self._created -= 1

        # No idle connection available - create a new one
        with self._lock:
            self._created += 1
        try:
            return self._make_connection()
        except Exception:
            with self._lock:
                self._created -= 1
            raise

    def release(self, conn):
        """Return a connection to the pool (or close it if the pool is full)."""
        if conn is None:
            return
        entry = _PoolEntry(conn)
        try:
            self._idle.put_nowait(entry)
        except queue.Full:
            try:
                conn.close()
            except Exception:
                pass
            with self._lock:
                self._created -= 1


# Module-level singleton pool (created once, shared across all requests)
_pool = _ConnectionPool()


class _PooledConnection:
    """
    Thin wrapper so that existing code calling ``conn.close()`` returns the
    connection to the pool instead of actually closing it.  All other
    attribute access is forwarded to the real Snowflake connection.
    """
    __slots__ = ("_conn", "_pool", "_closed")

    def __init__(self, conn, pool: _ConnectionPool):
        self._conn = conn
        self._pool = pool
        self._closed = False

    def close(self):
        if not self._closed:
            self._closed = True
            self._pool.release(self._conn)

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __del__(self):
        if not self._closed:
            self.close()


# ---------------------------------------------------------------------------
# Public helpers (unchanged API for callers)
# ---------------------------------------------------------------------------

def get_connection():
    """Read-only Snowflake connection from pool. Callers should still call .close() in finally blocks."""
    raw = _pool.acquire()
    return _PooledConnection(raw, _pool)


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

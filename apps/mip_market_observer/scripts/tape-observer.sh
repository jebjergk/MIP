#!/usr/bin/env bash
#
# Tape observer — start | stop | restart | status | health | init
#
# Run from Git Bash, WSL, or Linux/macOS. From repo root:
#   bash MIP/apps/mip_market_observer/scripts/tape-observer.sh start
#
# Optional env (export before calling):
#   TAPE_HOST          default 127.0.0.1
#   TAPE_PORT          default 8095
#   TAPE_OBSERVER_SIMULATE=1   fake ticks (no TWS)
#   IBKR_HOST, IBKR_PORT, TAPE_IB_CLIENT_ID   live IB (see README)
#   PYTHON=/path/to/python   override interpreter
#
# After start, set on mip_ui_api:  TAPE_OBSERVER_BASE_URL=http://127.0.0.1:8095

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OBSERVER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$OBSERVER_DIR" || exit 1

PID_FILE="$OBSERVER_DIR/.tape-observer.pid"
LOG_FILE="$OBSERVER_DIR/.tape-observer.log"

HOST="${TAPE_HOST:-127.0.0.1}"
PORT="${TAPE_PORT:-8095}"
BASE_URL="http://${HOST}:${PORT}"

usage() {
  echo "Tape observer control (uvicorn on TAPE_HOST:TAPE_PORT, default 127.0.0.1:8095)"
  echo ""
  echo "Usage: $(basename "$0") {start|stop|restart|status|health|init}"
  echo ""
  echo "  start    — background uvicorn; PID → .tape-observer.pid, log → .tape-observer.log"
  echo "  stop     — kill process from PID file"
  echo "  restart  — stop then start"
  echo "  status   — running? + curl /health"
  echo "  health   — print JSON from /health"
  echo "  init     — health + snapshot (TAPE_SYMBOL=AMD optional, default SPY)"
  echo ""
  echo "Env: TAPE_OBSERVER_SIMULATE=1  IBKR_HOST  IBKR_PORT  TAPE_IB_CLIENT_ID  PYTHON"
  exit 0
}

resolve_python() {
  if [[ -n "${PYTHON:-}" ]]; then
    echo "$PYTHON"
    return
  fi
  if [[ -x "$OBSERVER_DIR/.venv/bin/python" ]]; then
    echo "$OBSERVER_DIR/.venv/bin/python"
    return
  fi
  if [[ -x "$OBSERVER_DIR/.venv/Scripts/python.exe" ]]; then
    echo "$OBSERVER_DIR/.venv/Scripts/python.exe"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return
  fi
  echo ""
}

is_running() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(tr -d ' \r\n' <"$PID_FILE" 2>/dev/null || true)"
  [[ -z "${pid:-}" ]] && return 1
  if kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  return 1
}

have_curl() {
  command -v curl >/dev/null 2>&1
}

cmd_start() {
  if is_running; then
    echo "Tape observer already running (PID $(tr -d ' \r\n' <"$PID_FILE"))."
    exit 0
  fi
  local py
  py="$(resolve_python)"
  if [[ -z "$py" ]]; then
    echo "No Python found. Create a venv under:"
    echo "  $OBSERVER_DIR"
    echo "  pip install -r requirements.txt"
    echo "Or set PYTHON=/path/to/python"
    exit 1
  fi

  echo "Using: $py"
  echo "Working dir: $OBSERVER_DIR"
  echo "Binding: $BASE_URL"
  echo "Log file: $LOG_FILE"
  echo ""
  echo "Reminder: mip_ui_api needs  TAPE_OBSERVER_BASE_URL=$BASE_URL"
  echo ""

  touch "$LOG_FILE"
  # shellcheck disable=SC2086
  nohup "$py" -m uvicorn app.main:app --host "$HOST" --port "$PORT" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"

  sleep 2
  if is_running; then
    echo "Started OK (PID $(tr -d ' \r\n' <"$PID_FILE"))."
    if have_curl; then
      echo ""
      echo "--- /health ---"
      curl -sS --max-time 5 "$BASE_URL/health" || true
      echo ""
    fi
  else
    echo "Process exited. Last lines of log:"
    tail -n 30 "$LOG_FILE" 2>/dev/null || true
    rm -f "$PID_FILE"
    exit 1
  fi
}

cmd_stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "Tape observer not running (no PID file)."
    exit 0
  fi
  local pid
  pid="$(tr -d ' \r\n' <"$PID_FILE")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Stale PID file (process $pid gone). Removing."
    rm -f "$PID_FILE"
    exit 0
  fi
  echo "Stopping PID $pid ..."
  kill "$pid" 2>/dev/null || true
  sleep 1
  if kill -0 "$pid" 2>/dev/null; then
    echo "Sending SIGKILL ..."
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "Stopped."
}

cmd_status() {
  if is_running; then
    echo "Tape observer: RUNNING (PID $(tr -d ' \r\n' <"$PID_FILE"))"
  else
    echo "Tape observer: STOPPED"
    [[ -f "$PID_FILE" ]] && rm -f "$PID_FILE"
  fi
  if have_curl; then
    echo ""
    echo "GET $BASE_URL/health"
    if curl -sS --max-time 5 "$BASE_URL/health"; then
      echo ""
    else
      echo "(curl failed — server not listening?)"
    fi
  else
    echo "(install curl for HTTP checks)"
  fi
}

cmd_health() {
  if ! have_curl; then
    echo "curl not found."
    exit 1
  fi
  curl -sS --max-time 5 "$BASE_URL/health"
  echo ""
}

cmd_init() {
  local sym
  sym="${TAPE_SYMBOL:-SPY}"
  if ! have_curl; then
    echo "curl not found."
    exit 1
  fi
  echo "=== GET $BASE_URL/health ==="
  curl -sS --max-time 5 "$BASE_URL/health" || exit 1
  echo ""
  echo ""
  echo "=== GET $BASE_URL/tape/v1/snapshot?symbol=$sym ==="
  curl -sS --max-time 8 "$BASE_URL/tape/v1/snapshot?symbol=${sym}" | head -c 2000
  echo ""
  echo ""
  echo "(Symbol was $sym — set TAPE_SYMBOL=YOURTICKER to test another.)"
}

cmd_restart() {
  cmd_stop || true
  cmd_start
}

main() {
  case "${1:-}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    health)  cmd_health ;;
    init|check) cmd_init ;;
    -h|--help|help) usage ;;
    *) usage ;;
  esac
}

main "$@"

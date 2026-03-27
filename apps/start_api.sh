#!/usr/bin/env bash
# Start the MIP UX API (FastAPI + uvicorn)
# Default --reload watches only the API package so editing the React app does not
# restart the server (avoids ECONNRESET on the Vite proxy mid-page-load).
# For zero auto-reload: MIP_UVICORN_RELOAD=0 ./start_api.sh
cd "$(dirname "$0")/../.."
APP_DIR="MIP/apps/mip_ui_api"
if [ "${MIP_UVICORN_RELOAD:-1}" = "0" ]; then
  exec uvicorn app.main:app --app-dir "$APP_DIR"
else
  exec uvicorn app.main:app --reload --app-dir "$APP_DIR" --reload-dir "$APP_DIR"
fi

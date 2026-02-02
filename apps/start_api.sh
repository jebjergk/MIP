#!/usr/bin/env bash
# Start the MIP UX API (FastAPI + uvicorn)
cd "$(dirname "$0")/../.."
uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api

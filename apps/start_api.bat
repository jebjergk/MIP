@echo off
REM Start the MIP UX API (FastAPI + uvicorn)
cd /d "%~dp0..\.."
uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api

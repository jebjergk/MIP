# Reminder: Tape service bash script (not implemented yet)

When you are ready to operationalize the stack, add a **bash** script (or pair of scripts) that can:

1. **Start** the Tape observer (`uvicorn app.main:app` on the chosen port) with the right env vars.
2. **Stop** it cleanly (PID file or `pkill` pattern).
3. **Initiate / health-check** Tape (optional `curl` to `/health` and `/tape/v1/snapshot?symbol=...`).

Windows users may still want a **PowerShell** equivalent; the original note asked specifically for **bash** (e.g. WSL, Git Bash, CI Linux).

Delete this file after the script exists, or keep it as a pointer in your runbook.

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import runs, portfolios, briefs, training, performance, status, today, live

app = FastAPI(
    title="MIP UI API",
    description="Read-only API for MIP pipeline runs, portfolios, briefs, and training status. No writes to Snowflake.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(status.router)
app.include_router(runs.router)
app.include_router(portfolios.router)
app.include_router(briefs.router)
app.include_router(training.router)
app.include_router(performance.router)
app.include_router(today.router)
app.include_router(live.router)


@app.get("/")
def root():
    return {"service": "MIP UI API", "read_only": True}

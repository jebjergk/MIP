import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import runs, portfolios, briefs, training, performance, status, today, live, signals, market_timeline, digest, training_digest, management, market_pulse, parallel_worlds, ask, decisions

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="MIP UI API",
    description="API for MIP pipeline runs, portfolios, AI digests, training status, and portfolio management.",
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
app.include_router(signals.router)
app.include_router(market_timeline.router)
app.include_router(digest.router)
app.include_router(training_digest.router)
app.include_router(management.router)
app.include_router(market_pulse.router)
app.include_router(parallel_worlds.router)
app.include_router(ask.router)
app.include_router(decisions.router)


@app.get("/")
def root():
    return {"service": "MIP UI API"}

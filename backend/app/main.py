"""Market Data API.

Run from the repo root:

    uvicorn backend.app.main:app --port 8000

Interactive docs (try the endpoints, with an Authorize button) are at /docs.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.api import auth, data
from backend.app.core.config import get_settings
from backend.app.core.db import backend_engine, warehouse_engine


@asynccontextmanager
async def lifespan(_app: FastAPI):
    get_settings()          # fail at startup, not on the first request, if config is bad
    yield
    backend_engine().dispose()
    warehouse_engine().dispose()


app = FastAPI(
    title="Market Data API",
    version="0.1.0",
    description="Hourly OHLCV candles for the crypto and stock symbols tracked by the platform. "
                "Register, log in to get a bearer token, then send it as `Authorization: Bearer <token>`.",
    lifespan=lifespan,
)
app.include_router(auth.router)
app.include_router(data.router)


@app.get("/health", tags=["ops"], summary="Liveness check (no auth)")
def health() -> dict:
    # Public on purpose so a load balancer can poll it. It reveals nothing.
    return {"status": "ok"}

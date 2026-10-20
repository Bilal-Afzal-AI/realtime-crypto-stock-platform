"""Thin client for the Market Data API.

No Streamlit imports here on purpose: it can be unit-tested on its own, and it
turns every way a call can fail into one of four exceptions the pages know how
to present, so a raw traceback or JSON error never reaches the screen.

    SessionExpired   401 on a data call: the token is expired or invalid
    RateLimited      429: too many requests; carries how long to wait
    ApiUnavailable   the API cannot be reached at all (not running, network)
    ApiError         anything else (bad request, unknown symbol, server error)
"""

from __future__ import annotations

import os
from datetime import datetime

import httpx

# 127.0.0.1 rather than "localhost": on Windows "localhost" tries IPv6 (::1) first,
# where nothing is listening, and waiting for that to fail added ~3 s to every request.
BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
TIMEOUT_S = 20.0   # generous: login runs bcrypt, which is slow by design

# Tests inject an httpx.MockTransport here.
_transport: httpx.BaseTransport | None = None


class ApiError(Exception):
    """The API answered, but with an error we can describe to the user."""


class SessionExpired(ApiError):
    pass


class RateLimited(ApiError):
    def __init__(self, retry_after: int) -> None:
        super().__init__(f"Too many requests. Try again in {retry_after}s.")
        self.retry_after = retry_after


class ApiUnavailable(ApiError):
    pass


def _detail(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail", response.text)
    except Exception:
        return response.text[:200] or f"HTTP {response.status_code}"
    if isinstance(detail, list):  # FastAPI validation errors
        return "; ".join(f"{'.'.join(str(p) for p in d.get('loc', [])[1:])}: {d.get('msg')}" for d in detail)
    return str(detail)


def _request(method: str, path: str, *, token: str | None = None, **kwargs) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        with httpx.Client(base_url=BASE_URL, timeout=TIMEOUT_S, transport=_transport) as client:
            response = client.request(method, path, headers=headers, **kwargs)
    except httpx.TimeoutException as exc:
        raise ApiUnavailable(f"The API at {BASE_URL} did not answer within {TIMEOUT_S:.0f}s. "
                             "It may be busy; try again in a moment.") from exc
    except httpx.TransportError as exc:
        raise ApiUnavailable(f"Cannot reach the API at {BASE_URL}. Is it running?") from exc

    if response.status_code == 429:
        raise RateLimited(int(response.headers.get("retry-after", "30")))
    if response.status_code >= 500:
        raise ApiError("The API had an internal error. Please try again shortly.")
    return response


def login(username: str, password: str) -> dict:
    """Returns {"access_token", "token_type", "expires_in"}."""
    r = _request("POST", "/auth/login", data={"username": username, "password": password})
    if r.status_code == 200:
        return r.json()
    raise ApiError("Incorrect username or password." if r.status_code == 401 else _detail(r))


def register(username: str, password: str) -> None:
    r = _request("POST", "/auth/register", json={"username": username, "password": password})
    if r.status_code != 201:
        raise ApiError(_detail(r))


def _get(path: str, token: str, params: dict | None = None):
    r = _request("GET", path, token=token, params=params)
    if r.status_code == 401:
        raise SessionExpired("Your session has expired.")
    if r.status_code != 200:
        raise ApiError(_detail(r))
    return r.json()


def symbols(token: str) -> list[dict]:
    return _get("/symbols", token)


def latest_candles(token: str) -> list[dict]:
    return _get("/candles/latest", token)


def candles(token: str, symbol: str, *, source: str | None = None, start: datetime | None = None,
            end: datetime | None = None, limit: int = 1000, order: str = "desc") -> list[dict]:
    params = {"limit": limit, "order": order}
    if source:
        params["source"] = source
    if start:
        params["start"] = start.isoformat()
    if end:
        params["end"] = end.isoformat()
    return _get(f"/candles/{symbol}", token, params)["candles"]

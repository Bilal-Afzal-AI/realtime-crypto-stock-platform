"""Sliding-window rate limiting, in memory.

How it works: for each caller (a user id, or an IP address for people who are
not logged in yet) we remember the timestamps of their requests in the last
60 seconds. A new request is allowed if fewer than `limit` of those remain;
otherwise it is refused with HTTP 429 and a Retry-After telling the client
when the oldest request ages out. "Sliding" matters: a fixed "per calendar
minute" counter lets a client send a full quota at 12:00:59 and another at
12:01:00, twice the limit in two seconds. A sliding window has no such seam.

Limits of this version, deliberately:
- State is per process. One server process is exact. Run four workers or two
  servers behind a load balancer and each keeps its own count, so the effective
  limit is multiplied. Production shares the counters in Redis (INCR/EXPIRE or
  a sorted set per caller), keeping this same interface.
- Behind a reverse proxy every request appears to come from the proxy's IP.
  There you must read the client address from X-Forwarded-For, but ONLY when
  it was set by your own proxy: a client can send that header itself.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock

from fastapi import Depends, HTTPException, Request, Response

from backend.app.auth.deps import CurrentUser, get_current_user
from backend.app.core.config import Settings, get_settings

WINDOW_SECONDS = 60.0


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    retry_after: int   # seconds until a slot frees up (whole seconds, rounded up)


class SlidingWindowLimiter:
    def __init__(self, window: float = WINDOW_SECONDS) -> None:
        self.window = window
        self._hits: dict[str, deque[float]] = {}
        self._lock = Lock()

    def check(self, key: str, limit: int, now: float | None = None) -> Decision:
        now = time.monotonic() if now is None else now
        with self._lock:
            hits = self._hits.setdefault(key, deque())
            while hits and hits[0] <= now - self.window:
                hits.popleft()
            if len(hits) >= limit:
                retry = max(1, int(hits[0] + self.window - now) + 1)
                return Decision(False, limit, 0, retry)
            hits.append(now)
            if len(self._hits) > 10_000:       # forget callers who have gone quiet
                self._sweep(now)
            reset = int(hits[0] + self.window - now) + 1
            return Decision(True, limit, limit - len(hits), reset)

    def _sweep(self, now: float) -> None:
        for key in [k for k, h in self._hits.items() if not h or h[-1] <= now - self.window]:
            del self._hits[key]

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()


limiter = SlidingWindowLimiter()


def _enforce(decision: Decision, response: Response) -> None:
    headers = {
        "X-RateLimit-Limit": str(decision.limit),
        "X-RateLimit-Remaining": str(decision.remaining),
    }
    if not decision.allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: {decision.limit} requests per minute. "
                   f"Retry in {decision.retry_after}s.",
            headers={**headers, "Retry-After": str(decision.retry_after)},
        )
    response.headers.update(headers)


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def limit_login(request: Request, response: Response,
                settings: Settings = Depends(get_settings)) -> None:
    _enforce(limiter.check(f"login:{client_ip(request)}", settings.rate_limit_login_per_minute),
             response)


def limit_register(request: Request, response: Response,
                   settings: Settings = Depends(get_settings)) -> None:
    _enforce(limiter.check(f"register:{client_ip(request)}",
                           settings.rate_limit_register_per_minute), response)


def limit_data(response: Response, user: CurrentUser = Depends(get_current_user),
               settings: Settings = Depends(get_settings)) -> CurrentUser:
    """Authenticate first (bad token -> 401), then count the request against the USER.

    Keyed by user id, not IP, so one user can't dodge the limit by changing
    address and an office full of users behind one NAT isn't punished together.
    """
    _enforce(limiter.check(f"data:{user.id}", settings.rate_limit_data_per_minute), response)
    return user

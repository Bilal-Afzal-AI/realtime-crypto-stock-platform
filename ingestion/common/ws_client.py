"""A websocket client that survives the real internet.

Both producers (Binance for crypto, Finnhub for stocks) need the same thing:
"stay connected to a stream forever, and if anything goes wrong, get back on
it without hammering the server or silently going quiet". That logic is the
hard part of any streaming ingestion job, so it lives here once, and each
producer only supplies two small callbacks:

    on_connect(ws)    -> send the subscribe messages (runs on EVERY connect)
    on_message(raw)   -> parse one frame from the server

Why each piece exists
---------------------
1. Exponential backoff with full jitter.
   When a connection drops we wait before retrying: 1s, 2s, 4s, 8s ... up to
   a cap. Doubling means a server that is down for an hour gets a handful of
   attempts from us, not thousands. "Full jitter" means we sleep a random
   amount between 0 and that ceiling. Without jitter, if an exchange restarts
   and 10,000 clients were all disconnected at the same instant, they would
   all retry at exactly 1s, 2s, 4s ... in lockstep and knock it over again
   (the "thundering herd"). Randomness spreads them out.
   Reference: AWS Architecture Blog, "Exponential Backoff And Jitter".

2. Backoff reset only after a *stable* connection.
   The naive version resets the retry counter as soon as the handshake
   succeeds. But a common failure is "connect works, then the server drops
   us one second later" (bad subscription, rate limit, auth issue). With a
   naive reset we would reconnect every ~1s forever. So we only reset the
   counter once a connection has stayed up for `stable_after` seconds.

3. Heartbeats and stale-connection detection, at two levels.
   - Protocol level: the `websockets` library sends a ping every
     `ping_interval` seconds and closes the socket if no pong arrives within
     `ping_timeout`. This catches dead TCP connections (Wi-Fi drop, NAT
     timeout) that would otherwise look "open" for many minutes.
   - Application level: a connection can be perfectly alive at the TCP level
     while the *data* has stopped (the server forgot our subscription, a
     proxy is buffering, etc.). So we also track when we last received any
     frame and force a reconnect if nothing arrived for `stale_after`
     seconds. A quiet feed is the most dangerous failure in streaming,
     because nothing errors: dashboards just show old prices.

4. Resubscribe on every reconnect.
   Subscriptions belong to a connection. A fresh socket knows nothing about
   what we asked for last time, so `on_connect` runs after every handshake,
   not just the first one. Forgetting this is the classic bug where the
   reconnect "works" but no data ever flows again.

5. Graceful shutdown.
   Ctrl+C (or SIGTERM from Docker/Kubernetes) cancels the task. Leaving the
   `async with connect(...)` block sends a proper websocket close frame, so
   the server frees our slot immediately. This matters on Finnhub's free
   tier, which allows only one connection per API key: a socket killed
   without a close can block your next start for a while.

6. Fatal vs. retryable errors.
   Retrying is right for network blips and server restarts, and wrong for a
   bad API key. An HTTP 401/403 during the handshake will never fix itself,
   so we stop and say so instead of retrying forever.
"""

from __future__ import annotations

import asyncio
import logging
import random
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import websockets
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

OnConnect = Callable[[ClientConnection], Awaitable[None]]
OnMessage = Callable[[str | bytes], Awaitable[None]]

# HTTP statuses during the handshake that retrying will never fix.
FATAL_HANDSHAKE_STATUSES = {401, 403}


class FatalStreamError(Exception):
    """An error that reconnecting cannot fix (e.g. a rejected API key)."""


class StaleConnectionError(Exception):
    """No frame arrived for longer than `stale_after` seconds."""


@dataclass(frozen=True)
class BackoffPolicy:
    """Exponential backoff with full jitter.

    attempt 0 -> sleep in [0, base]
    attempt 1 -> sleep in [0, base * factor]
    attempt n -> sleep in [0, min(cap, base * factor**n)]
    """

    base: float = 1.0
    factor: float = 2.0
    cap: float = 60.0

    def delay(self, attempt: int) -> float:
        ceiling = min(self.cap, self.base * self.factor**attempt)
        return random.uniform(0, ceiling)


@dataclass(frozen=True)
class StreamConfig:
    name: str  # used in log lines, e.g. "binance"
    url: str
    # A redacted URL for logs. Never log a URL that carries an API token.
    log_url: str | None = None
    backoff: BackoffPolicy = BackoffPolicy()
    ping_interval: float = 20.0
    ping_timeout: float = 20.0
    stale_after: float = 60.0
    stable_after: float = 30.0


async def run_stream(
    config: StreamConfig,
    on_connect: OnConnect,
    on_message: OnMessage,
) -> None:
    """Connect, subscribe, read forever; reconnect with backoff on failure.

    Returns only when cancelled (shutdown) or raises FatalStreamError.
    """
    log = logging.getLogger(f"ingestion.{config.name}")
    attempt = 0
    shown_url = config.log_url or config.url

    while True:
        connected_at: float | None = None
        went_stale = False
        try:
            log.info("connecting to %s (attempt %d)", shown_url, attempt + 1)
            async with connect(
                config.url,
                ping_interval=config.ping_interval,
                ping_timeout=config.ping_timeout,
                open_timeout=15,
                close_timeout=5,
            ) as ws:
                connected_at = time.monotonic()
                log.info("connected")
                await on_connect(ws)  # resubscribe on every (re)connect
                await _read_until_stale(ws, config, on_message, log)

        except asyncio.CancelledError:
            # Ctrl+C / SIGTERM. The `async with` above already sent a close
            # frame on the way out; just let the cancellation propagate.
            log.info("shutdown requested, connection closed cleanly")
            raise
        except InvalidStatus as exc:
            status = exc.response.status_code
            if status in FATAL_HANDSHAKE_STATUSES:
                raise FatalStreamError(
                    f"{config.name}: server rejected the handshake with HTTP "
                    f"{status}. Check the API key; retrying will not help."
                ) from exc
            log.warning("handshake rejected with HTTP %d", status)
        except StaleConnectionError as exc:
            went_stale = True
            log.warning("%s; forcing a reconnect", exc)
        except ConnectionClosed as exc:
            log.warning("connection closed: code=%s reason=%r",
                        exc.rcvd.code if exc.rcvd else None,
                        exc.rcvd.reason if exc.rcvd else "")
        except (OSError, TimeoutError, websockets.WebSocketException) as exc:
            log.warning("connection failed: %s: %s", type(exc).__name__, exc)

        # Only forgive past failures if this connection proved itself stable.
        # A link that sat open but delivered nothing does not count: being
        # "up" for 60 silent seconds is a failure, not stability.
        if connected_at is not None:
            uptime = time.monotonic() - connected_at
            if uptime >= config.stable_after and not went_stale:
                attempt = 0
            log.info("connection was up for %.1fs", uptime)

        delay = config.backoff.delay(attempt)
        log.info("reconnecting in %.2fs (backoff attempt %d)", delay, attempt + 1)
        attempt += 1
        await asyncio.sleep(delay)


async def _read_until_stale(
    ws: ClientConnection,
    config: StreamConfig,
    on_message: OnMessage,
    log: logging.Logger,
) -> None:
    while True:
        try:
            async with asyncio.timeout(config.stale_after):
                raw = await ws.recv()
        except TimeoutError:
            raise StaleConnectionError(
                f"no data for {config.stale_after:.0f}s"
            ) from None
        try:
            await on_message(raw)
        except Exception:
            # One malformed message must not take the whole stream down.
            log.exception("failed to handle message: %.200r", raw)


def run_producer(main: Callable[[], Awaitable[None]]) -> None:
    """Entry point shared by the producers: logging + graceful shutdown.

    asyncio.run() already turns Ctrl+C into task cancellation. On Linux (our
    Docker containers) we also map SIGTERM, which is what `docker stop` sends,
    to the same cancellation so containers shut down just as cleanly.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # The websockets library logs every frame at DEBUG; keep it quiet.
    logging.getLogger("websockets").setLevel(logging.WARNING)

    async def _wrapped() -> None:
        task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
        except (NotImplementedError, AttributeError):
            pass  # Windows: no add_signal_handler; Ctrl+C still works.
        await main()

    try:
        asyncio.run(_wrapped())
    except KeyboardInterrupt:
        pass
    except FatalStreamError as exc:
        logging.getLogger("ingestion").error("%s", exc)
        raise SystemExit(1) from None

"""Finnhub US stock trade stream producer: console log + Kafka topic `stocks.quotes`.

Run:  python -m ingestion.stocks.finnhub_producer
Needs FINNHUB_API_KEY in .env (free key at https://finnhub.io/register).

Finnhub specifics worth knowing
-------------------------------
- Trades only flow during US market hours (09:30-16:00 America/New_York,
  Mon-Fri). Outside those hours the socket stays open and you only receive
  {"type":"ping"} keep-alives. That is normal, not a bug.
- The API key travels in the URL query string, so we log a redacted URL.
  Never print the real one; logs end up in places secrets should not.
- Free tier allows ONE websocket connection per key. Two producers with the
  same key will keep kicking each other off, which looks like a flapping
  connection. This is one reason the reconnect helper only resets backoff
  after a stable connection.
- A bad key fails the handshake with HTTP 401. The helper treats that as
  fatal and exits instead of retrying forever.
- Each "trade" message can batch several trades in `data`, possibly for
  different symbols, so we always iterate the list.
"""

from __future__ import annotations

import json
import logging
import os

from dotenv import load_dotenv

from ingestion.common.events import Trade
from ingestion.common.kafka_publisher import TradePublisher
from ingestion.common.ws_client import StreamConfig, run_producer, run_stream

BASE_URL = "wss://ws.finnhub.io"
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "TSLA"]
DEFAULT_TOPIC = "stocks.quotes"

log = logging.getLogger("ingestion.finnhub")


def parse_trades(msg: dict) -> list[Trade]:
    """Turn a Finnhub trade message into Trades.

    Example payload:
      {"type":"trade","data":[{"s":"AAPL","p":231.5,"v":100,"t":1729300000123,
                                "c":["1","12"]}]}
    `c` holds trade condition codes; kept out of Trade for now.
    """
    if msg.get("type") != "trade":
        return []
    return [
        Trade(
            source="finnhub",
            symbol=t["s"],
            price=float(t["p"]),
            size=float(t["v"]),
            trade_ts_ms=int(t["t"]),
        )
        for t in msg.get("data", [])
    ]


async def main() -> None:
    load_dotenv()
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "FINNHUB_API_KEY is not set. Add it to .env (see .env.example)."
        )
    symbols = [s.strip().upper() for s in
               os.getenv("FINNHUB_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")]
    publisher = TradePublisher(os.getenv("FINNHUB_TOPIC", DEFAULT_TOPIC), "finnhub")
    await publisher.ensure_schema()

    async def on_connect(ws) -> None:
        for symbol in symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        log.info("subscribed to %s", ", ".join(symbols))

    async def on_message(raw: str | bytes) -> None:
        msg = json.loads(raw)
        kind = msg.get("type")
        if kind == "ping":
            log.debug("ping")
            return
        if kind == "error":
            log.error("finnhub error: %s", msg.get("msg"))
            return
        for trade in parse_trades(msg):
            log.info(trade.log_line())
            publisher.publish(trade)

    config = StreamConfig(
        name="finnhub",
        url=f"{BASE_URL}?token={api_key}",
        log_url=f"{BASE_URL}?token=***",
        # Finnhub keep-alive pings count as "data", so a quiet market does not
        # trigger this; a minute or two of total silence does.
        stale_after=120.0,
    )
    try:
        await run_stream(config, on_connect, on_message)
    finally:
        publisher.close()  # deliver whatever is still buffered


if __name__ == "__main__":
    run_producer(main)

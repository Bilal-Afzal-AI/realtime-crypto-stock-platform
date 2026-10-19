"""Binance spot trade stream producer: console log + Kafka topic `crypto.trades`.

Run:  python -m ingestion.crypto.binance_producer

Binance market data is public, so no API key is needed.

Binance specifics worth knowing
-------------------------------
- We connect to the raw `/ws` endpoint and send a SUBSCRIBE message instead of
  baking the streams into the URL. That way the subscription is something we
  *do* on every connect, which is exactly what reconnect needs.
- Binance pings us every ~20s and expects a pong; the websockets library
  answers automatically. We also ping Binance ourselves to detect dead links.
- Binance closes every connection after 24 hours by design. That shows up
  here as a normal disconnect followed by an immediate, well-behaved
  reconnect, which is a nice free test of the reconnect path.
- In some regions (e.g. the US) stream.binance.com is blocked. Set
  BINANCE_WS_URL=wss://data-stream.binance.vision/ws (market-data-only
  mirror) or the binance.us endpoint if you see HTTP 451 or timeouts.
"""

from __future__ import annotations

import json
import logging
import os

from dotenv import load_dotenv

from ingestion.common.events import Trade
from ingestion.common.kafka_publisher import TradePublisher
from ingestion.common.ws_client import StreamConfig, run_producer, run_stream

DEFAULT_URL = "wss://stream.binance.com:9443/ws"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_TOPIC = "crypto.trades"

log = logging.getLogger("ingestion.binance")


def parse_trade(msg: dict) -> Trade | None:
    """Turn a Binance `<symbol>@trade` payload into a Trade.

    Example payload:
      {"e":"trade","E":1729300000123,"s":"BTCUSDT","t":12345,
       "p":"67000.10","q":"0.0012","T":1729300000120,"m":true,...}
    Prices and quantities arrive as strings to avoid float rounding on the
    wire; we convert at the edge. (A money-grade pipeline would use Decimal.)
    """
    if msg.get("e") != "trade":
        return None
    return Trade(
        source="binance",
        symbol=msg["s"],
        price=float(msg["p"]),
        size=float(msg["q"]),
        trade_ts_ms=int(msg["T"]),
        trade_id=str(msg["t"]),
    )


async def main() -> None:
    load_dotenv()
    url = os.getenv("BINANCE_WS_URL", DEFAULT_URL)
    symbols = [s.strip().upper() for s in
               os.getenv("BINANCE_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")]
    streams = [f"{s.lower()}@trade" for s in symbols]
    publisher = TradePublisher(os.getenv("BINANCE_TOPIC", DEFAULT_TOPIC), "binance")
    await publisher.ensure_schema()

    async def on_connect(ws) -> None:
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        log.info("subscribed to %s", ", ".join(streams))

    async def on_message(raw: str | bytes) -> None:
        msg = json.loads(raw)
        if "result" in msg and "id" in msg:
            # Ack for our SUBSCRIBE. result=null means success.
            if msg["result"] is not None:
                log.warning("unexpected subscribe response: %s", msg)
            return
        if "error" in msg:
            log.error("binance error: %s", msg["error"])
            return
        trade = parse_trade(msg)
        if trade:
            log.info(trade.log_line())
            publisher.publish(trade)

    config = StreamConfig(
        name="binance",
        url=url,
        # BTC trades many times per second, so 30s of silence means trouble.
        stale_after=30.0,
    )
    try:
        await run_stream(config, on_connect, on_message)
    finally:
        publisher.close()  # deliver whatever is still buffered


if __name__ == "__main__":
    run_producer(main)

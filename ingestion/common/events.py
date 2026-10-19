"""The one event shape every producer emits, whatever the source.

Binance and Finnhub describe a trade with different field names (Binance's
trade time is "T" and quantity "q"; Finnhub's are "t" and "v"), envelopes and
quirks. Each producer translates its own
format into `Trade` right at the edge, so everything downstream (Kafka, the
lake, the warehouse) only ever sees one schema. When a vendor changes its
format, exactly one parser changes.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Trade:
    source: str        # "binance" | "finnhub"
    symbol: str        # as the source names it, e.g. "BTCUSDT", "AAPL"
    price: float
    size: float        # base-asset quantity (coins or shares)
    trade_ts_ms: int   # when the trade happened, per the exchange
    trade_id: str | None = None
    # When *we* received it. trade_ts vs. ingest_ts is our end-to-end latency,
    # and lets us spot a feed that is delivering old data.
    ingest_ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return asdict(self)

    def log_line(self) -> str:
        lag = self.ingest_ts_ms - self.trade_ts_ms
        # A negative lag means our clock is behind the exchange's. Worth
        # watching: latency metrics are only as good as clock sync (NTP).
        return (f"{self.symbol:<8} price={self.price:<12} size={self.size:<12} "
                f"lag={lag}ms")

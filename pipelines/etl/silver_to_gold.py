"""Silver -> gold: hourly OHLCV candles per symbol.

Gold is what dashboards and the API read, so it holds answers rather than
events: one row per symbol per hour with

    open    price of the first trade in the hour
    high    highest trade price
    low     lowest trade price
    close   price of the last trade in the hour
    volume  total quantity traded (coins or shares)
    trade_count  number of trades (a free sanity check on how busy the hour was)

"First" and "last" are decided by trade time, with Kafka partition and offset
as tie-breakers, so two trades in the same millisecond still have a stable
order and the same input always gives the same candle.

How a run works
---------------
A gold partition (source/symbol/date) is rebuilt from scratch from its whole
silver partition whenever that silver file has changed since gold last read it.
Rebuilding the full day, rather than patching in the new trades, is what makes
this simple to reason about:

- Idempotent: the output depends only on the silver file, never on how many
  times or when the job ran.
- Self-healing: late data that lands in silver (a delayed bronze file, a
  re-processed quarantine) simply changes the silver file, and the next run
  corrects the affected candles.
- The current hour is a partial candle until the hour ends; it is recomputed
  on every run until then.

"Has silver changed?" is answered by recording the silver file's size and
modification time inside the gold file's own metadata. Because the note is
part of the file that is renamed into place atomically, data and note can
never disagree, and a silver update that lands *while* gold is being built is
still noticed on the next run.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipelines.etl import lake

log = logging.getLogger(__name__)

TS = pa.timestamp("ms", tz="UTC")

GOLD_SCHEMA = pa.schema([
    ("source", pa.string()),
    ("symbol", pa.string()),
    ("hour_start", TS),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("trade_count", pa.int64()),
])

def _silver_stamp(path: Path) -> dict[bytes, bytes]:
    st = path.stat()
    return {b"silver_size": str(st.st_size).encode(), b"silver_mtime_ns": str(st.st_mtime_ns).encode()}


def _gold_path(lake_root: Path, source: str, symbol: str, date: str) -> Path:
    return lake.gold_dir(lake_root) / source / symbol / date / lake.GOLD_FILE


def _is_stale(silver_path: Path, gold_path: Path) -> bool:
    if not gold_path.exists():
        return True
    meta = pq.read_schema(gold_path).metadata or {}
    return any(meta.get(k) != v for k, v in _silver_stamp(silver_path).items())


def find_stale_partitions(lake_root: Path | None = None) -> list[dict]:
    """Silver partitions with no gold yet, or whose silver changed since gold was built."""
    lake_root = lake_root or lake.lake_root()
    root = lake.silver_dir(lake_root)
    stale = []
    if not root.is_dir():
        return stale
    for silver_path in sorted(root.glob(f"*/*/*/{lake.SILVER_FILE}")):
        date_dir = silver_path.parent
        symbol_dir, source_dir = date_dir.parent, date_dir.parent.parent
        if source_dir.name.startswith("_"):
            continue
        gold_path = _gold_path(lake_root, source_dir.name, symbol_dir.name, date_dir.name)
        if _is_stale(silver_path, gold_path):
            stale.append({"source": source_dir.name, "symbol": symbol_dir.name, "date": date_dir.name})
    return stale


def compute_ohlcv(silver: pa.Table) -> pa.Table:
    """Hourly candles for one symbol's trades."""
    ordered = silver.sort_by([("trade_ts", "ascending"), ("kafka_partition", "ascending"),
                              ("kafka_offset", "ascending")])
    # Round each timestamp down to the hour with plain integer arithmetic on the
    # epoch milliseconds (UTC). pc.floor_temporal would work too, but on a
    # timezone-aware column it needs a timezone database, which pyarrow does not
    # ship on Windows. Integer division by a positive number floors.
    ms_per_hour = 3_600_000
    epoch_ms = ordered["trade_ts"].cast(pa.int64())
    hour = pc.multiply(pc.divide(epoch_ms, ms_per_hour), ms_per_hour).cast(TS)
    work = ordered.select(["price", "size"]).append_column("hour_start", hour)
    # use_threads=False: "first" and "last" are only meaningful in input order.
    agg = work.group_by("hour_start", use_threads=False).aggregate([
        ("price", "first"), ("price", "max"), ("price", "min"), ("price", "last"),
        ("size", "sum"), ("price", "count"),
    ]).sort_by("hour_start")
    n = agg.num_rows
    return pa.table({
        "source": pa.repeat(ordered["source"][0], n),
        "symbol": pa.repeat(ordered["symbol"][0], n),
        "hour_start": agg["hour_start"],
        "open": agg["price_first"],
        "high": agg["price_max"],
        "low": agg["price_min"],
        "close": agg["price_last"],
        "volume": agg["size_sum"],
        "trade_count": agg["price_count"],
    }, schema=GOLD_SCHEMA)


def check_ohlcv(gold: pa.Table, silver: pa.Table) -> None:
    """Fail loudly if the candles are not self-consistent or do not add up to silver.

    These are the invariants any correct set of candles satisfies. Checking them
    in the pipeline turns a silent wrong number on a dashboard into a failed task.
    """
    rows = gold.select(["open", "high", "low", "close", "volume", "trade_count"]).to_pylist()
    for r in rows:
        if not (r["low"] <= r["open"] <= r["high"] and r["low"] <= r["close"] <= r["high"]):
            raise ValueError(f"inconsistent candle {r}")
        if r["volume"] < 0 or r["trade_count"] <= 0:
            raise ValueError(f"bad volume/trade_count in {r}")
    if sum(r["trade_count"] for r in rows) != silver.num_rows:
        raise ValueError("trade_count does not add up to the silver row count")
    expected, actual = pc.sum(silver["size"]).as_py(), sum(r["volume"] for r in rows)
    if not math.isclose(expected, actual, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"volume {actual} does not match silver total {expected}")


@dataclass
class Stats:
    source: str
    symbol: str
    date: str
    silver_rows: int = 0
    candles: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def build_partition(source: str, symbol: str, date: str, lake_root: Path | None = None) -> dict:
    lake_root = lake_root or lake.lake_root()
    silver_path = lake.silver_dir(lake_root) / source / symbol / date / lake.SILVER_FILE
    gold_path = _gold_path(lake_root, source, symbol, date)

    # Take the stamp BEFORE reading: if silver is replaced mid-run, the stamp
    # no longer matches and the next run rebuilds. Stamping after could hide it.
    stamp = _silver_stamp(silver_path)
    silver = pq.read_table(silver_path)
    stats = Stats(source, symbol, date, silver_rows=silver.num_rows)
    if silver.num_rows == 0:
        return stats.as_dict()

    gold = compute_ohlcv(silver)
    check_ohlcv(gold, silver)
    gold = gold.replace_schema_metadata(stamp)
    lake.atomic_write_parquet(gold, gold_path)
    stats.candles = gold.num_rows
    log.info("gold %s/%s/%s: %d trades -> %d hourly candles", source, symbol, date,
             stats.silver_rows, stats.candles)
    return stats.as_dict()


def summarize(results: list[dict]) -> dict:
    return {
        "partitions": len(results),
        "silver_rows": sum(r["silver_rows"] for r in results),
        "candles": sum(r["candles"] for r in results),
    }

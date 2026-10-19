"""Load gold hourly candles (Parquet) into the warehouse table `ohlcv_1h`.

Called by the `gold_to_warehouse` DAG, but plain Python on purpose: every
function takes an open DB-API connection (psycopg2 inside Airflow), so it can
be run and tested without Airflow.

Why upsert instead of insert
----------------------------
A plain INSERT is only correct if it runs exactly once per row, and pipelines
do not give you that. Tasks retry after a crash, a failed run is re-run by hand,
a backfill replays last week, and the current hour's candle is *meant* to be
loaded again every hour as its trades arrive. With INSERT, each of those either
fails on the primary key (and the load dies) or, with no key, silently stores a
duplicate candle that doubles that hour's volume in every chart and sum.

An upsert (`INSERT ... ON CONFLICT ... DO UPDATE`) says "make the table look
like this": a new candle is inserted, a known one is updated to the new values,
and running the same load ten times leaves the same table as running it once.
It is also a single atomic statement. The alternative, "SELECT to see whether it
exists, then INSERT or UPDATE", has a race: two loaders can both find nothing
and both insert.

Two details here:
- The update only fires WHERE a value actually differs, so re-loading an
  unchanged candle writes nothing (no new row version, no index churn, and
  `updated_at` keeps meaning "last time the data changed").
- The whole file loads in ONE statement via unnest() over arrays, not one
  INSERT per row: one round trip instead of hundreds.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import pyarrow.parquet as pq

from pipelines.etl import lake

log = logging.getLogger(__name__)

PRICE_PLACES = Decimal("1e-8")  # NUMERIC(18,8) and NUMERIC(28,8) both keep 8 decimals

UPSERT_SQL = """
INSERT INTO ohlcv_1h AS t (source, symbol, open_time, open, high, low, close, volume, trade_count)
SELECT * FROM unnest(
    %(source)s::text[], %(symbol)s::text[], %(open_time)s::timestamptz[],
    %(open)s::numeric[], %(high)s::numeric[], %(low)s::numeric[], %(close)s::numeric[],
    %(volume)s::numeric[], %(trade_count)s::integer[]
) AS u(source, symbol, open_time, open, high, low, close, volume, trade_count)
ON CONFLICT (source, symbol, open_time) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    volume = EXCLUDED.volume, trade_count = EXCLUDED.trade_count, updated_at = now()
WHERE (t.open, t.high, t.low, t.close, t.volume, t.trade_count)
      IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close,
                        EXCLUDED.volume, EXCLUDED.trade_count)
RETURNING (xmax = 0) AS inserted
"""

COUNT_SQL = """
SELECT count(*) FROM ohlcv_1h t
JOIN unnest(%(source)s::text[], %(symbol)s::text[], %(open_time)s::timestamptz[])
     AS k(source, symbol, open_time) USING (source, symbol, open_time)
"""


def to_decimal(value: float) -> Decimal:
    """Float from Parquet -> exact Decimal with 8 decimal places.

    `repr(float)` is the shortest string that round-trips, so 82714.76 becomes
    Decimal("82714.76"), not 82714.759999999995. Rounding to 8 places then
    removes float noise such as a summed volume of 4.041800000000001.
    """
    return Decimal(repr(float(value))).quantize(PRICE_PLACES, rounding=ROUND_HALF_EVEN)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_schema(conn) -> None:
    """Fail with an actionable message if the DDL has not been applied."""
    with conn.cursor() as cur:
        for table in ("symbols", "ohlcv_1h", "ohlcv_1h_load_state"):
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"table {table} does not exist; apply warehouse/ddl/*.sql first "
                    "(see warehouse/README.md)")
    conn.rollback()


def gold_files(lake_root: Path | None = None) -> list[Path]:
    root = lake.gold_dir(lake_root or lake.lake_root())
    return sorted(root.glob(f"*/*/*/{lake.GOLD_FILE}")) if root.is_dir() else []


def find_changed_files(conn, lake_root: Path | None = None) -> list[dict]:
    """Gold files whose content differs from what was last loaded (or never loaded)."""
    lake_root = lake_root or lake.lake_root()
    root = lake.gold_dir(lake_root)
    with conn.cursor() as cur:
        cur.execute("SELECT gold_file, content_sha256 FROM ohlcv_1h_load_state")
        loaded = dict(cur.fetchall())
    conn.rollback()
    changed = []
    for path in gold_files(lake_root):
        rel = path.relative_to(root).as_posix()
        sha = _sha256(path)
        if loaded.get(rel) != sha:
            changed.append({"file": rel, "sha256": sha})
    return changed


def _infer_symbol_row(source: str, symbol: str) -> tuple:
    """Best-effort metadata for a symbol nobody registered yet."""
    asset_class = "crypto" if source == "binance" or ":" in symbol else "equity"
    quote = "USDT" if symbol.upper().endswith("USDT") else "USD"
    return source, symbol, asset_class, quote


def load_file(conn, rel_path: str, expected_sha256: str, lake_root: Path | None = None) -> dict:
    """Upsert one gold file's candles and record it as loaded, in one transaction."""
    lake_root = lake_root or lake.lake_root()
    path = lake.gold_dir(lake_root) / rel_path
    # Hash again: if gold was rebuilt after discovery we load what is on disk now
    # and record THAT hash, so state never claims more than was loaded.
    sha = _sha256(path)
    table = pq.read_table(path)
    cols = {name: table[name].to_pylist() for name in table.column_names}
    n = table.num_rows

    params = {
        "source": cols["source"],
        "symbol": cols["symbol"],
        "open_time": cols["hour_start"],
        "open": [to_decimal(v) for v in cols["open"]],
        "high": [to_decimal(v) for v in cols["high"]],
        "low": [to_decimal(v) for v in cols["low"]],
        "close": [to_decimal(v) for v in cols["close"]],
        "volume": [to_decimal(v) for v in cols["volume"]],
        "trade_count": cols["trade_count"],
    }
    assert all(isinstance(t, datetime) and t.tzinfo for t in params["open_time"])

    try:
        with conn.cursor() as cur:
            for source, symbol in sorted(set(zip(params["source"], params["symbol"]))):
                cur.execute("""INSERT INTO symbols (source, symbol, asset_class, quote_currency)
                               VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                            _infer_symbol_row(source, symbol))
                if cur.rowcount:
                    log.warning("registered unknown symbol %s/%s with inferred metadata; "
                                "review it in the symbols table", source, symbol)

            cur.execute(UPSERT_SQL, params)
            flags = [row[0] for row in cur.fetchall()]
            inserted, updated = sum(flags), len(flags) - sum(flags)

            # Verify, don't assume: every candle in the file must now be in the table.
            cur.execute(COUNT_SQL, {k: params[k] for k in ("source", "symbol", "open_time")})
            present = cur.fetchone()[0]
            if present != n:
                raise RuntimeError(f"{rel_path}: {n} candles in file but {present} found in "
                                   "ohlcv_1h after upsert; rolling back")

            cur.execute("""INSERT INTO ohlcv_1h_load_state (gold_file, content_sha256, row_count)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (gold_file) DO UPDATE SET
                               content_sha256 = EXCLUDED.content_sha256,
                               row_count = EXCLUDED.row_count, loaded_at = now()""",
                        (rel_path, sha, n))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    stats = {"file": rel_path, "candles": n, "inserted": inserted, "updated": updated,
             "unchanged": n - inserted - updated}
    log.info("warehouse %s: %d candles (%d inserted, %d updated, %d unchanged)",
             rel_path, n, inserted, updated, stats["unchanged"])
    return stats


def summarize(results: list[dict]) -> dict:
    return {
        "files": len(results),
        "candles": sum(r["candles"] for r in results),
        "inserted": sum(r["inserted"] for r in results),
        "updated": sum(r["updated"] for r in results),
        "unchanged": sum(r["unchanged"] for r in results),
    }

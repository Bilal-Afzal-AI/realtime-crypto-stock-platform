"""Shared helpers for the medallion data lake (bronze -> silver -> gold).

Two things here matter for correctness, not just convenience:

1. Atomic writes. Every file is written to a temporary name in the same
   folder, flushed to disk, and then renamed over the final name. A rename is
   all-or-nothing, so a reader (the gold job, a dashboard, you in a notebook)
   sees either the complete old file or the complete new one, never half of
   one. It also means a crashed task leaves at most a stray `.tmp` file, never
   a corrupt partition.

2. One place that knows the lake layout, so the DAGs, the ETL code and the
   tests cannot disagree about where things live.

       data_lake/bronze/{source}/{symbol}/{date}/trades_*.json     raw, JSON Lines
       data_lake/silver/{source}/{symbol}/{date}/trades.parquet    cleaned, deduped
       data_lake/gold/ohlcv_1h/{source}/{symbol}/{date}/ohlcv_1h.parquet
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Inside the Airflow containers the lake is bind-mounted at /opt/airflow/data_lake
# and DATA_LAKE_DIR says so. On the host it defaults to <repo>/data_lake.
_DEFAULT_LAKE = Path(__file__).resolve().parents[2] / "data_lake"

SILVER_FILE = "trades.parquet"
SILVER_MANIFEST = "_processed_files.json"
GOLD_TABLE = "ohlcv_1h"
GOLD_FILE = "ohlcv_1h.parquet"


def lake_root() -> Path:
    return Path(os.getenv("DATA_LAKE_DIR", str(_DEFAULT_LAKE)))


def safe_path_part(value: str) -> str:
    """Folder-safe form of a symbol, e.g. "BINANCE:BTCUSDT" -> "BINANCE_BTCUSDT".

    Must stay identical to `_safe_path_part` in streaming/consumers/bronze_writer.py,
    because bronze folder names are produced by that function. (It is copied, not
    imported, because the writer imports the Kafka client, which Airflow's image
    does not have.)
    """
    return "".join("_" if c in '<>:"/\\|?*' else c for c in value)


def bronze_dir(lake: Path) -> Path:
    return lake / "bronze"


def silver_dir(lake: Path) -> Path:
    return lake / "silver"


def gold_dir(lake: Path) -> Path:
    return lake / "gold" / GOLD_TABLE


def _tmp_path(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_json(path: Path, obj) -> None:
    atomic_write_text(path, json.dumps(obj, sort_keys=True, separators=(",", ":")))


def atomic_write_parquet(table: pa.Table, path: Path) -> None:
    """Write Parquet atomically.

    zstd gives better compression than the default snappy at similar speed.
    Dictionary encoding stays on: `source` and `symbol` repeat on every row,
    so they cost almost nothing on disk.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    pq.write_table(table, tmp, compression="zstd")
    # Reopen to flush to disk. "r+b" rather than "rb": Windows refuses fsync on
    # a read-only handle.
    with tmp.open("r+b") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default

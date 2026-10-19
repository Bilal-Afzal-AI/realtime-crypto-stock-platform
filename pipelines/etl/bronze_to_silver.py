"""Bronze -> silver: clean, type, and de-duplicate raw trades.

Bronze is raw and append-only (see streaming/consumers/bronze_writer.py).
Silver is the version analysts can trust: every column has one type, every
timestamp is a real UTC timestamp, every Kafka message appears exactly once,
and records that are not usable are set aside instead of silently dropped.

How a run works (per bronze partition = source/symbol/date)
-----------------------------------------------------------
1. "New" files = bronze files not yet listed in the silver partition's
   `_processed_files.json`. State lives next to the output it describes.
2. Parse each new file line by line. Good records are cleaned and typed; bad
   ones are written to `silver/_quarantine/...` with the reason and the
   original text, and processing carries on. One corrupt line must never fail
   a pipeline that is handling a million good ones.
3. Merge with what silver already holds for the partition, drop duplicates on
   (kafka_topic, kafka_partition, kafka_offset), sort, write the whole
   partition file atomically.
4. Only then record the new bronze files as processed.

Why this is idempotent
----------------------
- The output is a pure function of the set of valid bronze records: dedupe key
  + deterministic sort order. Same input, same bytes out.
- Running again with nothing new is a no-op (nothing listed as new).
- A crash between step 3 and step 4 just means those files are processed
  again next time. They merge into the same rows and the dedupe key removes
  the overlap, so the result is identical. The same holds if you delete the
  manifest to force a full rebuild.
- The quarantine file for a bronze file is named after that file, so
  re-processing overwrites it instead of appending a second copy.

Why (topic, partition, offset) is the dedupe key
------------------------------------------------
Bronze is at-least-once: after a crash the writer may write a batch twice.
A Kafka message's (topic, partition, offset) is unique and never reused, so
two records with the same triple are the same message. Keying on trade fields
instead (symbol, price, time) would wrongly merge two genuine trades that
happen to match, which happens constantly in fast markets.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipelines.etl import lake

log = logging.getLogger(__name__)

TS = pa.timestamp("ms", tz="UTC")

SILVER_SCHEMA = pa.schema([
    ("source", pa.string()),
    ("symbol", pa.string()),
    ("trade_ts", TS),
    ("ingest_ts", TS),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("trade_id", pa.string()),
    ("kafka_topic", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
])

DEDUPE_KEY = ["kafka_topic", "kafka_partition", "kafka_offset"]
SORT_ORDER = [("trade_ts", "ascending"), ("kafka_partition", "ascending"),
              ("kafka_offset", "ascending")]

# Trades before this are certainly garbage (epoch 0, seconds-instead-of-ms...).
MIN_TS_MS = int(datetime(2015, 1, 1, tzinfo=UTC).timestamp() * 1000)
# A trade may not claim to be more than a day newer than when we ingested it.
MAX_FUTURE_SKEW_MS = 24 * 3600 * 1000


class RejectError(ValueError):
    """A record that cannot go into silver. The message is the reason code."""


# ---------------------------------------------------------------------------
# Cleaning one record
# ---------------------------------------------------------------------------

def _number(record: dict, field_name: str) -> float:
    value = record.get(field_name)
    if value is None:
        raise RejectError(f"missing_field:{field_name}")
    if isinstance(value, bool):
        raise RejectError(f"bad_type:{field_name}")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise RejectError(f"bad_type:{field_name}") from None
    if not math.isfinite(number):
        raise RejectError(f"bad_value:{field_name}")
    return number


def _int(container: dict, field_name: str, label: str | None = None) -> int:
    label = label or field_name
    value = container.get(field_name)
    if value is None:
        raise RejectError(f"missing_field:{label}")
    if isinstance(value, bool):
        raise RejectError(f"bad_type:{label}")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise RejectError(f"bad_type:{label}") from None
    if not math.isfinite(number) or number != int(number):
        raise RejectError(f"bad_type:{label}")
    return int(number)


def _text(record: dict, field_name: str) -> str:
    value = record.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise RejectError(f"missing_field:{field_name}")
    return value.strip()


def clean_record(raw: object, symbol_dir: str, date_dir: str) -> dict:
    """Validate and standardise one bronze record, or raise RejectError.

    `symbol_dir` / `date_dir` are the bronze folder the record was found in. A
    record whose own symbol or trade date disagrees with its folder is
    suspicious (bad writer, hand-edited file), so it is rejected rather than
    quietly filed under the wrong partition.
    """
    if not isinstance(raw, dict):
        raise RejectError("not_an_object")
    kafka = raw.get("_kafka")
    if not isinstance(kafka, dict):
        raise RejectError("missing_field:_kafka")

    source = _text(raw, "source").lower()
    symbol = _text(raw, "symbol")
    price = _number(raw, "price")
    size = _number(raw, "size")
    trade_ms = _int(raw, "trade_ts_ms")
    ingest_ms = _int(raw, "ingest_ts_ms")
    topic = _text(kafka, "topic")
    partition = _int(kafka, "partition", "_kafka.partition")
    offset = _int(kafka, "offset", "_kafka.offset")

    if price <= 0:
        raise RejectError("bad_value:price")
    if size < 0:
        raise RejectError("bad_value:size")
    if trade_ms < MIN_TS_MS or trade_ms > ingest_ms + MAX_FUTURE_SKEW_MS:
        raise RejectError("bad_timestamp:trade_ts_ms")
    if ingest_ms < MIN_TS_MS:
        raise RejectError("bad_timestamp:ingest_ts_ms")
    if partition < 0 or offset < 0:
        raise RejectError("bad_value:kafka_position")
    if lake.safe_path_part(symbol) != symbol_dir:
        raise RejectError("symbol_mismatch")
    if datetime.fromtimestamp(trade_ms / 1000, UTC).strftime("%Y-%m-%d") != date_dir:
        raise RejectError("date_mismatch")

    trade_id = raw.get("trade_id")
    return {
        "source": source,
        "symbol": symbol,
        "trade_ts": trade_ms,
        "ingest_ts": ingest_ms,
        "price": price,
        "size": size,
        "trade_id": None if trade_id is None else str(trade_id),
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }


def _to_table(rows: list[dict]) -> pa.Table:
    columns = {name: [r[name] for r in rows] for name in SILVER_SCHEMA.names}
    return pa.table(columns, schema=SILVER_SCHEMA)


def _dedupe_and_sort(table: pa.Table) -> pa.Table:
    """Keep the first row per Kafka message, then sort deterministically."""
    if table.num_rows == 0:
        return table
    indexed = table.append_column("__row", pa.array(range(table.num_rows), pa.int64()))
    # use_threads=False keeps "first row wins" stable and the output reproducible.
    firsts = indexed.group_by(DEDUPE_KEY, use_threads=False).aggregate([("__row", "min")])
    keep = pc.sort_indices(firsts["__row_min"])
    rows = pc.take(firsts["__row_min"], keep)
    return table.take(rows).sort_by(SORT_ORDER)


# ---------------------------------------------------------------------------
# Finding work
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Partition:
    source: str
    symbol: str  # folder-safe name
    date: str

    def as_dict(self) -> dict:
        return {"source": self.source, "symbol": self.symbol, "date": self.date}


def _bronze_partition_dirs(lake_root: Path):
    root = lake.bronze_dir(lake_root)
    if not root.is_dir():
        return
    for source_dir in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith("_")):
        for symbol_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
            for date_dir in sorted(p for p in symbol_dir.iterdir() if p.is_dir()):
                yield Partition(source_dir.name, symbol_dir.name, date_dir.name), date_dir


def _manifest_path(lake_root: Path, p: Partition) -> Path:
    return lake.silver_dir(lake_root) / p.source / p.symbol / p.date / lake.SILVER_MANIFEST


def _new_files(lake_root: Path, p: Partition, bronze_partition: Path) -> list[str]:
    done = set(lake.read_json(_manifest_path(lake_root, p), {"bronze_files": []})["bronze_files"])
    present = {f.name for f in bronze_partition.glob("trades_*.json")}
    return sorted(present - done)


def find_partitions_with_new_files(lake_root: Path | None = None) -> list[dict]:
    """Partitions that have bronze files silver has not seen yet."""
    lake_root = lake_root or lake.lake_root()
    found = []
    for partition, path in _bronze_partition_dirs(lake_root):
        new = _new_files(lake_root, partition, path)
        if new:
            found.append({**partition.as_dict(), "new_files": len(new)})
    return found


# ---------------------------------------------------------------------------
# Processing one partition
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    source: str
    symbol: str
    date: str
    new_files: int = 0
    lines_read: int = 0
    rejected: int = 0
    duplicates_dropped: int = 0
    rows_added: int = 0
    silver_rows_total: int = 0
    reject_reasons: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _read_bronze_file(path: Path, partition: Partition):
    """Yield (clean_row | None, reject | None) for every non-blank line."""
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = clean_record(json.loads(text), partition.symbol, partition.date)
            except json.JSONDecodeError:
                yield None, {"reason": "invalid_json", "line_no": line_no, "raw": text[:2000]}
            except RejectError as exc:
                yield None, {"reason": str(exc), "line_no": line_no, "raw": text[:2000]}
            else:
                yield row, None


def process_partition(source: str, symbol: str, date: str,
                      lake_root: Path | None = None) -> dict:
    lake_root = lake_root or lake.lake_root()
    partition = Partition(source, symbol, date)
    bronze_partition = lake.bronze_dir(lake_root) / source / symbol / date
    stats = Stats(source, symbol, date)

    new_files = _new_files(lake_root, partition, bronze_partition)
    stats.new_files = len(new_files)

    silver_path = lake.silver_dir(lake_root) / source / symbol / date / lake.SILVER_FILE
    existing = pq.read_table(silver_path, schema=SILVER_SCHEMA) if silver_path.exists() else None
    stats.silver_rows_total = existing.num_rows if existing is not None else 0
    if not new_files:
        return stats.as_dict()

    good_rows: list[dict] = []
    for name in new_files:
        rejects = []
        for row, reject in _read_bronze_file(bronze_partition / name, partition):
            stats.lines_read += 1
            if row is not None:
                good_rows.append(row)
            else:
                rejects.append({**reject, "bronze_file": f"{source}/{symbol}/{date}/{name}"})
                reason = reject["reason"]
                stats.reject_reasons[reason] = stats.reject_reasons.get(reason, 0) + 1
        if rejects:
            stats.rejected += len(rejects)
            qpath = (lake.silver_dir(lake_root) / "_quarantine" / source / symbol / date
                     / f"{name}.rejects.jsonl")
            lake.atomic_write_text(
                qpath, "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rejects))

    new_table = _to_table(good_rows)
    combined = new_table if existing is None else pa.concat_tables([existing, new_table])
    merged = _dedupe_and_sort(combined)

    stats.duplicates_dropped = combined.num_rows - merged.num_rows
    stats.rows_added = merged.num_rows - stats.silver_rows_total
    # Only rewrite the partition if it really changed. Rewriting identical data
    # would bump the file's mtime and make the gold job rebuild for nothing.
    if stats.rows_added > 0:
        lake.atomic_write_parquet(merged, silver_path)
    stats.silver_rows_total = merged.num_rows

    # Last step: remember these files are done. If we crash before this line the
    # files are simply reprocessed next run and dedupe makes that harmless.
    manifest = _manifest_path(lake_root, partition)
    done = set(lake.read_json(manifest, {"bronze_files": []})["bronze_files"])
    lake.atomic_write_json(manifest, {"bronze_files": sorted(done | set(new_files))})

    log.info("silver %s/%s/%s: %d new files, %d lines, %d rejected, %d duplicates, "
             "+%d rows, %d total", source, symbol, date, stats.new_files, stats.lines_read,
             stats.rejected, stats.duplicates_dropped, stats.rows_added, stats.silver_rows_total)
    return stats.as_dict()


def summarize(results: list[dict]) -> dict:
    """Roll the per-partition stats up into one line for the DAG's final task."""
    total = {"partitions": len(results)}
    for key in ("new_files", "lines_read", "rejected", "duplicates_dropped", "rows_added"):
        total[key] = sum(r.get(key, 0) for r in results)
    return total

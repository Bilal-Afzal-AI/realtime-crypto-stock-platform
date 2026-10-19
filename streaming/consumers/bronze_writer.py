"""Kafka -> bronze layer of the data lake.

Run:  python -m streaming.consumers.bronze_writer
      python -m streaming.consumers.bronze_writer --topics crypto.trades

Reads `crypto.trades` and `stocks.quotes`, decodes each message with Schema
Registry, and appends it unchanged to

    data_lake/bronze/{source}/{symbol}/{date}/trades_{timestamp}_p{partition}_o{offset}.json

Why bronze stores raw, untransformed data
-----------------------------------------
Bronze is the system's memory of what the outside world actually sent us.
Every later layer is a *function* of bronze: silver = clean(bronze),
gold = aggregate(silver). As long as bronze is complete and untouched, any
mistake downstream can be fixed by fixing the code and re-running it.

If we cleaned here instead:
- A bug in the cleaning logic destroys data permanently. Say we "drop trades
  with size == 0" and later learn those were valid odd-lot prints, or we
  dedupe on the wrong key and silently merge two real trades. With raw bronze
  you fix silver's code and rebuild; with cleaned bronze the evidence is gone
  and the only copy of the original stream (Kafka) has already expired.
- Rules change. Today's definition of "valid" or "duplicate" is a business
  decision that will be revised. Raw history lets you apply tomorrow's rules
  to last year's data.
- Debugging and audit. When a number in a dashboard looks wrong, you trace it
  back to the exact bytes the exchange sent, with Kafka partition and offset.
  "We cleaned it" is not an answer an auditor or a trader accepts.
- Ingestion stays dumb and fast. Cleaning involves judgment calls and can
  fail; bronze writing must never fail or fall behind, or we lose data while
  the source keeps streaming.

So the payload below is written exactly as the producer sent it. We only ADD
a `_kafka` block (topic, partition, offset, key, schema id, broker timestamp)
so every row can be traced to its exact position in the stream.

Offsets: at-least-once, resume where we left off
------------------------------------------------
Auto-commit is OFF. The loop is: poll -> buffer -> write files (atomically)
-> THEN commit offsets. If the process dies before the commit, the next start
re-reads from the last committed offset and rewrites those messages: at worst
a few duplicates, never a gap. Duplicates are cheap to remove in silver
(dedupe on source+symbol+trade id/time); a gap is unrecoverable. Committing
before writing would invert that trade-off and silently lose data on a crash.

The consumer group `bronze-writer` stores its offsets in Kafka, so a restart
continues exactly after the last flushed batch. `auto.offset.reset=earliest`
only applies the very first time the group runs (no offsets yet): bronze
should hold the topic's full history, not just what arrives after we start.

Why micro-batched files, not one file per message
-------------------------------------------------
BTCUSDT alone trades ~10 times a second, which would be ~860k files a day for
one symbol. Huge numbers of tiny files make every downstream read slow (each
file is an open/close, and object stores charge per request). This is the
"small files problem". So each file holds up to a few seconds of messages,
one JSON object per line (JSON Lines, what Spark's and pandas' JSON readers
expect for lakes), and is written to a temp name then renamed, so a reader
never sees a half-written file.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

DEFAULT_TOPICS = ["crypto.trades", "stocks.quotes"]
GROUP_ID = "bronze-writer"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_DIR = REPO_ROOT / "data_lake" / "bronze"

# Flush whichever comes first: this many seconds, or this many messages.
FLUSH_EVERY_S = 10.0
FLUSH_EVERY_N = 5_000

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

log = logging.getLogger("streaming.bronze")


def _to_epoch_ms(value):
    """Undo the Avro deserializer's timestamp-millis -> datetime conversion.

    The producer sent epoch milliseconds; bronze stores exactly that. Integer
    arithmetic on timedelta keeps it exact (no float rounding).
    """
    if isinstance(value, datetime):
        return (value - _EPOCH) // timedelta(milliseconds=1)
    return value


def _safe_path_part(value: str) -> str:
    """Make a symbol usable as a folder name (e.g. "BINANCE:BTCUSDT").

    Only the folder name changes; the record keeps the original symbol.
    `:` and `/` are illegal or meaningful in Windows/POSIX paths.
    """
    return "".join("_" if c in '<>:"/\\|?*' else c for c in value)


class BronzeWriter:
    def __init__(self, topics: list[str], bronze_dir: Path) -> None:
        self.topics = topics
        self.bronze_dir = bronze_dir
        registry = SchemaRegistryClient(
            {"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
        self._deserializer = AvroDeserializer(registry)
        self._consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": GROUP_ID,
            "client.id": "bronze-writer",
            # We commit ourselves, only after data is safely on disk.
            "enable.auto.commit": False,
            # Only used when the group has no committed offset yet.
            "auto.offset.reset": "earliest",
            "logger": log,
        })
        # (source, symbol, date, partition) -> list of records
        self._buffer: dict[tuple[str, str, str, int], list[dict]] = defaultdict(list)
        self._buffered = 0
        self._last_flush = time.monotonic()
        self._running = True
        self.written_total = 0

    # -- lifecycle ---------------------------------------------------------

    def stop(self, *_args) -> None:
        self._running = False

    def run(self) -> None:
        self._consumer.subscribe(self.topics, on_assign=self._on_assign,
                                 on_revoke=self._on_revoke)
        log.info("consuming %s as group %s -> %s", ", ".join(self.topics),
                 GROUP_ID, self.bronze_dir)
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is not None:
                    self._handle(msg)
                if (self._buffered >= FLUSH_EVERY_N
                        or time.monotonic() - self._last_flush >= FLUSH_EVERY_S):
                    self._flush_and_commit()
        except KeyboardInterrupt:
            pass
        finally:
            log.info("shutting down: flushing %d buffered messages", self._buffered)
            self._flush_and_commit()
            self._consumer.close()  # leaves the group cleanly -> fast rebalance
            log.info("stopped; %d messages written this run", self.written_total)

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Show where we resume from: the committed offset per partition.
        committed = consumer.committed(partitions, timeout=10)
        desc = ", ".join(
            f"{tp.topic}[{tp.partition}]@"
            f"{tp.offset if tp.offset >= 0 else 'none->earliest'}"
            for tp in committed)
        log.info("assigned: %s", desc)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Another consumer is about to own these partitions. Write and commit
        # what we hold first, or it would re-read (and duplicate) it.
        log.info("partitions revoked; flushing before handing them over")
        self._flush_and_commit()

    # -- per message -------------------------------------------------------

    def _handle(self, msg: Message) -> None:
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("consumer error: %s", msg.error())
            return
        raw = msg.value()
        kafka_meta = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode() if msg.key() else None,
            "timestamp_ms": msg.timestamp()[1],
            "schema_id": int.from_bytes(raw[1:5], "big") if raw and len(raw) >= 5 else None,
        }
        try:
            payload = self._deserializer(
                raw, SerializationContext(msg.topic(), MessageField.VALUE))
        except Exception as exc:  # noqa: BLE001 - poison message
            self._quarantine(msg, raw, kafka_meta, exc)
            return
        payload = {k: _to_epoch_ms(v) for k, v in payload.items()}

        date = datetime.fromtimestamp(payload["trade_ts_ms"] / 1000, UTC).strftime("%Y-%m-%d")
        key = (payload["source"], payload["symbol"], date, msg.partition())
        self._buffer[key].append({**payload, "_kafka": kafka_meta})
        self._buffered += 1

    def _quarantine(self, msg: Message, raw: bytes | None, meta: dict,
                    exc: Exception) -> None:
        """Never drop a message we can't decode: park the raw bytes.

        Bronze must be complete. A bad message goes to _quarantine with the
        exact bytes, so it can be inspected and replayed once fixed.
        """
        qdir = self.bronze_dir / "_quarantine" / msg.topic()
        qdir.mkdir(parents=True, exist_ok=True)
        record = {"error": f"{type(exc).__name__}: {exc}", "_kafka": meta,
                  "raw_base64": base64.b64encode(raw or b"").decode()}
        path = qdir / f"p{msg.partition()}_o{msg.offset()}.json"
        path.write_text(json.dumps(record), encoding="utf-8")
        log.error("could not decode %s[%d]@%d, quarantined to %s",
                  msg.topic(), msg.partition(), msg.offset(), path)

    # -- writing -----------------------------------------------------------

    def _flush_and_commit(self) -> None:
        self._last_flush = time.monotonic()
        if self._buffered:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            for (source, symbol, date, partition), records in self._buffer.items():
                folder = self.bronze_dir / source / _safe_path_part(symbol) / date
                folder.mkdir(parents=True, exist_ok=True)
                first = records[0]["_kafka"]["offset"]
                path = folder / f"trades_{stamp}_p{partition}_o{first}.json"
                self._write_atomic(path, records)
                log.info("wrote %4d records -> %s", len(records),
                         path.relative_to(self.bronze_dir.parent.parent))
            self.written_total += self._buffered
            self._buffer.clear()
            self._buffered = 0
        # Commit only now that everything consumed so far is on disk.
        # (Also fine when nothing was buffered: commits nothing new.)
        try:
            self._consumer.commit(asynchronous=False)
        except Exception as exc:  # noqa: BLE001
            # "No offset stored" just means nothing new since the last commit.
            if "NO_OFFSET" not in str(exc):
                log.warning("offset commit failed (will retry next flush): %s", exc)

    @staticmethod
    def _write_atomic(path: Path, records: list[dict]) -> None:
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, separators=(",", ":")))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())  # on disk BEFORE we commit the offset
        os.replace(tmp, path)     # atomic rename: readers never see half a file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topics", default=",".join(DEFAULT_TOPICS))
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    writer = BronzeWriter([t.strip() for t in args.topics.split(",")], args.bronze_dir)
    # `docker stop` sends SIGTERM: finish the current loop, flush, commit, exit.
    signal.signal(signal.SIGTERM, writer.stop)
    writer.run()


if __name__ == "__main__":
    main()

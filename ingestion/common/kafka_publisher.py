"""Publish Trade events to Kafka, Avro-encoded via Schema Registry.

Why Schema Registry instead of raw JSON
---------------------------------------
With raw JSON every consumer has to *guess* the shape of each message, and
nothing stops a producer from changing it. One day someone renames `price` to
`px`, or starts sending the price as a string, and the Spark job, the lake
loader and the dashboard all break at 3am, possibly hours later, after bad
rows are already in the warehouse.

With Schema Registry:
- The schema (ingestion/common/schemas/trade.avsc) is registered once under
  the subject `<topic>-value`. Each message carries only a 5-byte header
  (magic byte + schema id) plus compact binary Avro, so messages are much
  smaller than JSON with its repeated field names.
- The registry enforces compatibility rules (BACKWARD by default). Adding an
  optional field with a default is accepted; deleting or retyping a field is
  REJECTED at registration time, i.e. on the producer's machine, before a
  single bad message reaches the topic. Breaking changes fail loudly and early.
- Consumers look the schema up by id, so they can always decode any message,
  including old ones written with an earlier schema version. Replaying a month
  of history after a schema change just works.
- The schema is documentation that cannot drift from reality: it IS what is on
  the wire. Kafka UI uses it to show you decoded messages.

Why the producer never crashes when Kafka is down
-------------------------------------------------
`produce()` does not send anything. It appends to an in-memory queue inside
librdkafka (the C client) and returns immediately. A background thread sends
batches, and if the broker is unreachable it keeps retrying for up to
`message.timeout.ms` (2 minutes here). That queue IS our short buffer: a broker
restart or a brief network blip loses nothing. Only if Kafka stays down longer
than that, or the queue fills up, do we drop messages, and we count and log
every drop instead of crashing. For a live price feed that is the right
trade-off: blocking the websocket reader would make the *exchange* drop us,
and a stale price is worth little anyway. (A system that must never lose an
event would spill to local disk instead; that is a later phase.)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ingestion.common.events import Trade
from ingestion.common.ws_client import BackoffPolicy

TRADE_SCHEMA_PATH = Path(__file__).parent / "schemas" / "trade.avsc"
STATS_EVERY_S = 30.0


class TradePublisher:
    def __init__(self, topic: str, name: str) -> None:
        self.topic = topic
        self.log = logging.getLogger(f"ingestion.{name}.kafka")
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        self._registry = SchemaRegistryClient({"url": registry_url})
        self._schema_str = TRADE_SCHEMA_PATH.read_text(encoding="utf-8")
        self._serializer = AvroSerializer(
            self._registry,
            self._schema_str,
            to_dict=lambda trade, ctx: trade.to_dict(),
        )
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": f"{name}-producer",
            # Wait for the broker to persist each batch, and let the client
            # dedupe its own retries so a retry never writes a trade twice.
            "acks": "all",
            "enable.idempotence": True,
            # Batch for up to 20 ms: far fewer requests, invisible latency.
            "linger.ms": 20,
            "compression.type": "lz4",
            # The buffer: keep retrying for 2 minutes before giving up on a
            # message, holding at most 100k messages in memory meanwhile.
            "message.timeout.ms": 120_000,
            "queue.buffering.max.messages": 100_000,
            # Route librdkafka's own warnings ("connection refused" etc.)
            # into our log format instead of raw stderr.
            "logger": self.log,
        })
        self.delivered = 0
        self.failed = 0
        self.dropped = 0
        self._last_stats = time.monotonic()
        self._last_error: str | None = None
        self.log.info("publishing to topic %s on %s (schema registry %s)",
                      topic, bootstrap, registry_url)

    async def ensure_schema(self) -> None:
        """Register the Trade schema for `<topic>-value` (idempotent).

        Doing this up front, rather than on the first message, means a
        missing or incompatible schema is reported at startup in one clear
        line. If the registry is down we wait for it with backoff: without a
        schema id we cannot encode anything.
        """
        subject = f"{self.topic}-value"
        backoff = BackoffPolicy(base=1.0, cap=30.0)
        attempt = 0
        while True:
            try:
                schema_id = await asyncio.to_thread(
                    self._registry.register_schema,
                    subject, Schema(self._schema_str, "AVRO"),
                )
                self.log.info("schema registered: subject=%s id=%d", subject, schema_id)
                self._last_stats = time.monotonic()
                return
            except Exception as exc:  # noqa: BLE001 - network or registry error
                delay = backoff.delay(attempt)
                attempt += 1
                self.log.warning("schema registry unavailable (%s); retrying in %.1fs",
                                 exc, delay)
                await asyncio.sleep(delay)

    def publish(self, trade: Trade) -> None:
        """Queue one trade. Never raises and never blocks the caller."""
        try:
            value = self._serializer(
                trade, SerializationContext(self.topic, MessageField.VALUE))
        except Exception:
            self.dropped += 1
            self.log.exception("could not serialize %r; dropped", trade)
            return
        try:
            self._producer.produce(
                self.topic,
                # Key by symbol: Kafka routes equal keys to the same partition,
                # so all BTCUSDT trades stay in order relative to each other.
                key=trade.symbol.encode(),
                value=value,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # Local queue full: Kafka has been unreachable for a while.
            self.dropped += 1
            if self.dropped % 1000 == 1:
                self.log.error("local buffer full, Kafka unreachable? dropped %d so far",
                               self.dropped)
        # Serve delivery callbacks for earlier messages. Non-blocking.
        self._producer.poll(0)
        self._maybe_log_stats()

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        # Runs on our thread, inside poll()/flush(), once per message.
        if err is None:
            self.delivered += 1
            return
        self.failed += 1
        text = str(err)
        if text != self._last_error:  # don't print the same error 10,000 times
            self.log.error("delivery failed: %s (failed so far: %d)", text, self.failed)
            self._last_error = text

    def _maybe_log_stats(self) -> None:
        now = time.monotonic()
        if now - self._last_stats < STATS_EVERY_S:
            return
        self._last_stats = now
        self.log.info("kafka stats: delivered=%d failed=%d dropped=%d in_flight=%d",
                      self.delivered, self.failed, self.dropped, len(self._producer))

    def close(self, timeout: float = 10.0) -> None:
        """Flush what is still buffered. Called on shutdown."""
        remaining = self._producer.flush(timeout)
        self.log.info("flushed on shutdown: delivered=%d failed=%d dropped=%d unsent=%d",
                      self.delivered, self.failed, self.dropped, remaining)

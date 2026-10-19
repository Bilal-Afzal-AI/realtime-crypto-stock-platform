-- Fact: individual trades, mirroring the silver layer
-- (data_lake/silver/{source}/{symbol}/{date}/trades.parquet), so raw trades can
-- be queried with SQL instead of reading Parquet files.
--
-- Money columns are NUMERIC, never FLOAT: see 03_ohlcv_1h.sql for why.
--
-- The primary key is the Kafka position (topic, partition, offset), the same
-- key silver de-duplicates on. A message can therefore be loaded any number of
-- times and ends up as exactly one row.
--
-- NOTE: this table is created now so the structure is settled, but nothing
-- loads it yet. At production volume (millions of rows per day) it would be
-- partitioned by time (PARTITION BY RANGE (trade_ts), one partition per month)
-- so old data can be dropped or archived without a slow DELETE. That needs the
-- partition key in the primary key, so it is a deliberate later change.

CREATE TABLE IF NOT EXISTS trades_raw (
    kafka_topic      TEXT          NOT NULL,
    kafka_partition  INTEGER       NOT NULL,
    kafka_offset     BIGINT        NOT NULL,

    source           TEXT          NOT NULL,
    symbol           TEXT          NOT NULL,
    trade_ts         TIMESTAMPTZ   NOT NULL,   -- when the trade happened (exchange clock)
    ingest_ts        TIMESTAMPTZ   NOT NULL,   -- when our producer received it
    price            NUMERIC(18,8) NOT NULL,
    size             NUMERIC(28,8) NOT NULL,   -- base-asset quantity (coins or shares)
    trade_id         TEXT,                     -- exchange trade id; Finnhub has none
    loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset),
    FOREIGN KEY (source, symbol) REFERENCES symbols (source, symbol),
    CONSTRAINT trades_raw_price_chk CHECK (price > 0),
    CONSTRAINT trades_raw_size_chk  CHECK (size >= 0)
);

-- "Trades for this symbol in this time range" is the dominant query.
CREATE INDEX IF NOT EXISTS trades_raw_symbol_ts_idx
    ON trades_raw (source, symbol, trade_ts);

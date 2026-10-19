-- Bookkeeping for the warehouse loader DAG: which gold files were loaded, and
-- what they contained. The loader skips a file whose content hash is unchanged,
-- so hourly runs only touch what silver_to_gold actually rebuilt.
--
-- This table is an optimisation, not the source of truth. Delete its rows and
-- the next run reloads everything; the upsert makes that harmless.

CREATE TABLE IF NOT EXISTS ohlcv_1h_load_state (
    gold_file       TEXT        PRIMARY KEY,   -- path relative to data_lake/gold/ohlcv_1h
    content_sha256  TEXT        NOT NULL,
    row_count       INTEGER     NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

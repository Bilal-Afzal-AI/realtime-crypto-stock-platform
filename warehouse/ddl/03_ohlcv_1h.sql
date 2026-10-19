-- Fact: hourly OHLCV candles, loaded from the gold layer
-- (data_lake/gold/ohlcv_1h/...). One row per symbol per hour.
--
-- Why NUMERIC(18,8) and not DOUBLE PRECISION / REAL
-- -------------------------------------------------
-- Floats are binary fractions, and most decimal prices (0.1, 82714.76) have no
-- exact binary form. Every float carries a tiny hidden error, and sums and
-- averages accumulate it: in float, 0.1 + 0.2 = 0.30000000000000004, and
-- "does this equal 0.3?" is false. Prices that should match then don't, totals
-- drift by fractions of a cent over millions of rows, and reconciliation
-- against an exchange never quite ties out. NUMERIC stores decimal digits
-- exactly. (18,8) means 18 digits in total, 8 after the point, so up to
-- 9,999,999,999.99999999: eight places is the finest unit crypto trades in (a
-- satoshi), and ten whole digits covers any price we will see.
-- Volume is wider (28,8) because share counts and total traded size are
-- far bigger than any single price.
--
-- The natural key: (source, symbol, open_time)
-- ---------------------------------------------
-- A candle IS "this symbol, from this feed, for the hour starting at this
-- instant". The primary key says so, which gives the loader something to
-- upsert against, and makes duplicates impossible rather than merely unlikely.
-- Its index also serves the most common query: one symbol, a time range.

CREATE TABLE IF NOT EXISTS ohlcv_1h (
    source       TEXT          NOT NULL,
    symbol       TEXT          NOT NULL,
    open_time    TIMESTAMPTZ   NOT NULL,   -- start of the hour, UTC
    open         NUMERIC(18,8) NOT NULL,
    high         NUMERIC(18,8) NOT NULL,
    low          NUMERIC(18,8) NOT NULL,
    close        NUMERIC(18,8) NOT NULL,
    volume       NUMERIC(28,8) NOT NULL,
    trade_count  INTEGER       NOT NULL,
    loaded_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),   -- first inserted
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),   -- last time values changed

    PRIMARY KEY (source, symbol, open_time),
    FOREIGN KEY (source, symbol) REFERENCES symbols (source, symbol),

    -- Constraints are the last line of defence: a bug upstream cannot put an
    -- impossible candle in the table, the load fails instead.
    CONSTRAINT ohlcv_1h_hour_chk   CHECK (date_trunc('hour', open_time AT TIME ZONE 'UTC')
                                          = open_time AT TIME ZONE 'UTC'),
    CONSTRAINT ohlcv_1h_range_chk  CHECK (low <= open AND low <= close
                                          AND high >= open AND high >= close),
    CONSTRAINT ohlcv_1h_price_chk  CHECK (low > 0),
    CONSTRAINT ohlcv_1h_volume_chk CHECK (volume >= 0 AND trade_count > 0)
);

COMMENT ON TABLE ohlcv_1h IS 'Hourly OHLCV candles per (source, symbol). The latest hour is partial until it ends and is overwritten by later loads.';

-- Indexes. The primary key above already provides (source, symbol, open_time).
--
-- Same symbol across feeds, over time ("BTC from every source this week"):
CREATE INDEX IF NOT EXISTS ohlcv_1h_symbol_time_idx
    ON ohlcv_1h (symbol, open_time DESC);
-- Time first, across all symbols ("the latest candle of everything", "what
-- changed in the last hour"), which the two indexes above cannot serve:
CREATE INDEX IF NOT EXISTS ohlcv_1h_time_idx
    ON ohlcv_1h (open_time DESC);

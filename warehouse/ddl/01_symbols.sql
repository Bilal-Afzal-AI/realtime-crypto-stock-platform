-- Dimension: the symbols this platform tracks.
--
-- One row per (source, symbol), because the same instrument can arrive from
-- more than one feed (e.g. BTC from Binance directly and via Finnhub) and each
-- feed names it differently. Facts (candles, trades) reference this table, so a
-- typo like 'BTCUDST' is rejected at write time instead of becoming a new
-- phantom symbol with one row.
--
-- Safe to re-run: every statement is idempotent.

CREATE TABLE IF NOT EXISTS symbols (
    source          TEXT        NOT NULL,   -- feed: 'binance' | 'finnhub'
    symbol          TEXT        NOT NULL,   -- as the feed names it, e.g. 'BTCUSDT', 'AAPL'
    asset_class     TEXT        NOT NULL,   -- 'crypto' | 'equity'
    quote_currency  TEXT        NOT NULL DEFAULT 'USD',
    display_name    TEXT,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (source, symbol),
    CONSTRAINT symbols_asset_class_chk CHECK (asset_class IN ('crypto', 'equity'))
);

COMMENT ON TABLE  symbols IS 'Tracked instruments, one row per (feed, symbol).';
COMMENT ON COLUMN symbols.quote_currency IS 'Currency prices are quoted in. USDT is treated as its own quote asset, not as USD.';

-- Everything the producers currently subscribe to. ON CONFLICT DO NOTHING makes
-- re-running this file harmless and never overwrites edits made later.
INSERT INTO symbols (source, symbol, asset_class, quote_currency, display_name) VALUES
    ('binance', 'BTCUSDT',         'crypto', 'USDT', 'Bitcoin / Tether'),
    ('binance', 'ETHUSDT',         'crypto', 'USDT', 'Ethereum / Tether'),
    ('finnhub', 'AAPL',            'equity', 'USD',  'Apple Inc.'),
    ('finnhub', 'MSFT',            'equity', 'USD',  'Microsoft Corporation'),
    ('finnhub', 'TSLA',            'equity', 'USD',  'Tesla, Inc.'),
    ('finnhub', 'BINANCE:BTCUSDT', 'crypto', 'USDT', 'Bitcoin / Tether (via Finnhub)'),
    ('finnhub', 'BINANCE:ETHUSDT', 'crypto', 'USDT', 'Ethereum / Tether (via Finnhub)')
ON CONFLICT (source, symbol) DO NOTHING;

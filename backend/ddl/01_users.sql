-- Backend database (`backend`): accounts for the API.
--
-- This lives in its OWN database, separate from the warehouse (`market`), with
-- its own login role. Credentials and market data have different owners,
-- different sensitivity and different failure modes: the warehouse is rebuilt
-- from the data lake whenever needed and is read-only to the API, while this
-- table is the only copy of who can log in and must never be wiped by a
-- warehouse reload. Keeping them apart also means a SQL-injection bug in a
-- data endpoint cannot reach password hashes, because the connection that
-- serves candles has no access to this database at all.
--
-- Apply as the `backend` role (see backend/README.md). Safe to re-run.

CREATE TABLE IF NOT EXISTS users (
    id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username       TEXT        NOT NULL,
    -- bcrypt hash ("$2b$12$..."): includes its own random salt and cost factor.
    -- The password itself is never stored, logged or returned.
    password_hash  TEXT        NOT NULL,
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at  TIMESTAMPTZ,

    -- Usernames are stored lowercase, so the plain UNIQUE constraint also makes
    -- "Alice" and "alice" the same account.
    CONSTRAINT users_username_key UNIQUE (username),
    CONSTRAINT users_username_format_chk CHECK (username ~ '^[a-z0-9_]{3,32}$')
);

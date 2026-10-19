-- A read-only login for the API. Run as the warehouse owner:
--
--   psql -U market -d market -v api_pw="$API_READER_PASSWORD" -f warehouse/ddl/05_api_reader.sql
--
-- Least privilege: the API only ever reads candles and symbols, so it connects
-- as a role that CAN'T write, can't see the loader's bookkeeping or raw
-- trades, and can't alter tables. A bug or injection in an API endpoint is
-- then limited to reading two tables, not corrupting the warehouse.
--
-- The password comes in as a psql variable so it never lives in a file in git.
-- Re-running resets the password to the value given.

SELECT format('CREATE ROLE api_reader LOGIN PASSWORD %L', :'api_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader')
\gexec

SELECT format('ALTER ROLE api_reader PASSWORD %L', :'api_pw')
\gexec

-- Belt and braces: even if someone later grants more, this role's sessions
-- are read-only and a runaway query is cut off after 5 seconds.
ALTER ROLE api_reader SET default_transaction_read_only = on;
ALTER ROLE api_reader SET statement_timeout = '5s';

GRANT CONNECT ON DATABASE market TO api_reader;
GRANT USAGE ON SCHEMA public TO api_reader;
GRANT SELECT ON symbols, ohlcv_1h TO api_reader;

# Warehouse

PostgreSQL stand-in for the analytics warehouse (database `market`, container
`warehouse-postgres`, host port 5433).

| File | Creates |
|---|---|
| `ddl/01_symbols.sql` | `symbols` dimension + seed rows for the tracked symbols |
| `ddl/02_trades_raw.sql` | `trades_raw`, mirrors the silver layer (not loaded yet) |
| `ddl/03_ohlcv_1h.sql` | `ohlcv_1h` hourly candles, with indexes and sanity constraints |
| `ddl/04_load_state.sql` | `ohlcv_1h_load_state`, the loader's bookkeeping |
| `ddl/05_api_reader.sql` | `api_reader`, a read-only login for the API (needs a password, see below) |
| `loader.py` | Upserts gold Parquet candles into `ohlcv_1h` (used by the `gold_to_warehouse` DAG) |

Every DDL file is idempotent (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`), so
applying them again is harmless. Apply files 01 to 04 in order, from the repo root:

```bash
for f in warehouse/ddl/0[1-4]*.sql; do
  docker exec -i warehouse-postgres psql -U market -d market -v ON_ERROR_STOP=1 -q < "$f"
done
```

`05_api_reader.sql` takes the role's password as a psql variable, so it never
sits in a file in git. Put `API_READER_PASSWORD` in your git-ignored `.env`, then:

```bash
docker exec -i warehouse-postgres psql -U market -d market -v ON_ERROR_STOP=1 -q \
  -v api_pw="$(grep ^API_READER_PASSWORD .env | cut -d= -f2)" < warehouse/ddl/05_api_reader.sql
```

Open a SQL prompt:

```bash
docker exec -it warehouse-postgres psql -U market -d market
```

Prices are `NUMERIC(18,8)`, never floating point; the reasoning is at the top of
`ddl/03_ohlcv_1h.sql`, and the reasoning for upserts is at the top of `loader.py`.

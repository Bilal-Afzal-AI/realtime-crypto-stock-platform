# Backend API

FastAPI service that serves warehouse candles to the dashboard, behind JWT login.

## Run it

One-time setup, from the repo root (the stack from `infra/` must be running):

```bash
# 1. users table, in the separate `backend` database, as the backend role
docker exec -i warehouse-postgres psql -U backend -d backend -v ON_ERROR_STOP=1 -q < backend/ddl/01_users.sql
# 2. read-only warehouse login for the API: see warehouse/README.md (05_api_reader.sql)
# 3. .env: BACKEND_DATABASE_URL, WAREHOUSE_READONLY_URL, JWT_SECRET_KEY (see .env.example)
```

Start it:

```bash
uvicorn backend.app.main:app --port 8000
```

Open http://localhost:8000/docs for interactive docs. Register, log in, click
**Authorize**, and try the endpoints.

## Endpoints

| Method and path | Auth | What |
|---|---|---|
| `POST /auth/register` | no | Create an account (`{"username", "password"}`, password 10+ chars) |
| `POST /auth/login` | no | Form fields `username`, `password`; returns `{"access_token", "token_type", "expires_in"}` |
| `GET /symbols` | token | Tracked symbols |
| `GET /candles/latest` | token | Newest hourly candle for every active symbol |
| `GET /candles/{symbol}` | token | Candles for one symbol. Query: `source`, `start` (inclusive), `end` (exclusive), `limit` (1-1000, default 100), `order` (`desc` default, or `asc`) |
| `GET /health` | no | Liveness |

Send the token as `Authorization: Bearer <token>`. Prices and volumes are JSON
**strings** (`"82714.76000000"`), so no client turns them into binary floats by accident.

## Design notes

- **Two databases.** Accounts live in `backend` and are touched only by `/auth/*`.
  Market data is read from `market` through the read-only role `api_reader`,
  which cannot write, cannot read the loader's tables and cannot even connect
  to `backend`.
- **Stateless tokens.** `get_current_user` verifies the signature, expiry and
  issuer and never queries a database. Tokens last 30 minutes and cannot be
  revoked earlier; see `backend/app/auth/deps.py` for the trade-off.
- **Rate limits** (`backend/app/core/rate_limit.py`): data routes 60/min per user,
  login 10/min and register 5/min per IP. In memory and per process; behind several
  workers or servers move the counters to Redis.
- **Config** is validated at startup. A placeholder or short `JWT_SECRET_KEY` stops the app.

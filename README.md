# Real-Time Crypto and Stock Data Platform

An end-to-end data platform that ingests live cryptocurrency and stock market
data, streams it through Kafka, lands it in a layered data lake, models it in a
data warehouse, and serves it through an authenticated API and a live
dashboard.

> Status: initial scaffold. Components are built out incrementally; see the
> [Roadmap](#roadmap).

---

## 1. The business problem

Traders, analysts and fintech products need market data that is **fresh**,
**trustworthy** and **queryable**, and those three goals pull against each
other:

- **Fresh**: crypto trades 24/7 and prices move in milliseconds. A dashboard
  that refreshes from a nightly batch job is useless for monitoring.
- **Trustworthy**: raw API feeds drop messages, duplicate them, arrive out of
  order and change schema without warning. Decisions made on bad data cost
  money.
- **Queryable**: beyond "what is the price now", people ask historical
  questions ("30-day volatility of BTC vs. NVDA", "which assets moved together
  last quarter"). Those need clean, modelled, historical data.

This platform delivers all three by combining a **streaming path** (seconds of
latency, for live views and alerts) with a **batch path** (minutes to hours,
for clean history and analytics). This split is often called a *Lambda-style*
architecture.

---

## 2. High-level architecture

```
   Market data sources              Streaming                    Storage & modelling               Serving
 ┌────────────────────┐      ┌───────────────────┐      ┌──────────────────────────────┐   ┌──────────────────┐
 │ Crypto exchanges   │      │                   │      │  Data lake (Parquet)          │   │ FastAPI backend  │
 │ (websocket/REST)   ├─────►│   Apache Kafka    ├─────►│  bronze → silver → gold       │   │  + JWT auth      │
 │                    │      │                   │      │                              │   │                  │
 │ Stock APIs (REST)  ├─────►│  topics:          │      └──────────────┬───────────────┘   └────────┬─────────┘
 └────────────────────┘      │  crypto.trades    │                     │ ETL / ELT                   │
     ingestion/              │  stocks.quotes    │                     ▼                             ▼
                             └─────────┬─────────┘      ┌──────────────────────────────┐   ┌──────────────────┐
                                       │ consumers      │  Data warehouse (PostgreSQL)  ├──►│ Dashboard        │
                                       └───────────────►│  staging → marts (star schema)│   │ (Streamlit)      │
                                         live metrics   └──────────────────────────────┘   └──────────────────┘

                        Apache Airflow orchestrates the batch jobs (orchestration/dags)
```

### Data flow

1. **Ingestion** (`ingestion/`): Python producers connect to exchange
   websockets (crypto) and REST APIs (stocks), validate each event, and
   publish it to Kafka. They retry with backoff and never block on downstream
   systems.
2. **Streaming** (`streaming/`): Kafka buffers events durably and lets many
   independent consumers read the same stream. One consumer writes raw events
   to the lake; another computes live metrics for the dashboard.
3. **Data lake** (`data_lake/`): the *medallion* pattern.
   - `bronze`: raw events exactly as received, append-only. Lets us replay
     history if a bug is found later.
   - `silver`: deduplicated, typed, validated, time-ordered.
   - `gold`: business-level aggregates (OHLCV candles, daily returns).
4. **Pipelines** (`pipelines/`):
   - `etl/`: transform in Python *before* loading (bronze → silver cleaning).
   - `elt/`: load into the warehouse first, then transform with SQL inside it.
5. **Warehouse** (`warehouse/`): dimensional model (fact table of prices,
   dimension tables for assets, exchanges, dates) optimised for analytics.
6. **Orchestration** (`orchestration/`): Airflow DAGs schedule the batch
   jobs, handle retries and dependencies, and alert on failure.
7. **Backend** (`backend/`): FastAPI service exposing prices, history and
   analytics, protected by JWT authentication.
8. **Dashboard** (`dashboard/`): Streamlit app showing live prices and
   historical analytics by calling the backend.

---

## 3. Technology choices and trade-offs

| Layer | Choice | Why | Alternatives considered |
|---|---|---|---|
| Message broker | **Apache Kafka** (`confluent-kafka` client) | Durable, replayable log; decouples producers from consumers; industry standard for event streaming. `confluent-kafka` uses the C library librdkafka, so it is fast and battle-tested. | **Redpanda** (Kafka-compatible, simpler to run, a good local swap). **AWS Kinesis / GCP Pub/Sub** (managed, less ops, but cloud lock-in). **RabbitMQ** (great for task queues, not built for replayable high-throughput streams). |
| Orchestrator | **Apache Airflow** | Most widely used orchestrator in data engineering job postings; mature UI, retries, backfills, huge provider ecosystem. | **Dagster** (asset-centric, better local dev and typing). **Prefect** (lighter, more Pythonic). Both are excellent; Airflow chosen for market relevance. |
| Lake format | **Parquet** files, medallion layout | Columnar and compressed, so analytical scans are cheap; readable by every engine. | **Delta Lake / Apache Iceberg** add ACID transactions and time travel; a natural upgrade once the lake grows. |
| Warehouse | **PostgreSQL** (local) | Free, runs anywhere, real SQL. Keeps the project runnable on a laptop. | **Snowflake / BigQuery / Redshift** in production. The SQL models are written to port with minimal change. |
| Backend | **FastAPI** + JWT | Async (good for I/O-heavy APIs), automatic OpenAPI docs, request validation via Pydantic. | **Flask** (simpler, but sync-first and no built-in validation). **Django REST** (batteries included, heavier than needed). |
| Dashboard | **Streamlit** | A working data app in pure Python, fast to iterate. | **React** (full control and better UX at scale, but a second language and build chain). **Grafana** (excellent for time-series ops dashboards, less flexible for custom analytics). |

### Why Airflow has its own requirements file

Airflow pins hundreds of transitive dependencies through an official
constraints file. Installing it alongside the app's dependencies causes version
conflicts, and it does not run natively on Windows. Production teams almost
always run Airflow in its own container or a managed service (AWS MWAA, GCP
Cloud Composer, Astronomer), so this project mirrors that:
`requirements.txt` for the application, `requirements-airflow.txt` for the
orchestrator.

---

## 4. Repository layout

```
realtime-crypto-stock-platform/
├── ingestion/          # Producers: pull from APIs/websockets, publish to Kafka
│   ├── crypto/
│   ├── stocks/
│   └── common/         # shared clients, retry logic, event models
├── streaming/          # Kafka topic definitions, event schemas, consumers
│   ├── topics/
│   ├── schemas/
│   └── consumers/
├── pipelines/
│   ├── etl/            # transform-then-load jobs (Python)
│   └── elt/            # load-then-transform jobs (SQL in the warehouse)
├── orchestration/      # Airflow DAGs and plugins
│   ├── dags/
│   └── plugins/
├── data_lake/          # Local lake: bronze / silver / gold (data is git-ignored)
├── warehouse/
│   ├── ddl/            # table definitions
│   └── models/         # staging and mart SQL models
├── backend/            # FastAPI service
│   └── app/
│       ├── api/        # route handlers
│       ├── auth/       # JWT issue/verify, password hashing
│       ├── core/       # settings, logging
│       └── models/     # Pydantic / DB models
├── dashboard/          # Streamlit app
├── infra/              # docker-compose for Kafka, Postgres, Airflow
├── docs/               # architecture notes, decisions
├── tests/
│   ├── unit/
│   └── integration/
├── requirements.txt
├── requirements-airflow.txt
└── .env.example        # template for local secrets (.env is git-ignored)
```

The layout separates code **by responsibility**, so each part could later
become its own service, container or repository without untangling imports.

---

## 5. Getting started

Requires Python 3.12, Git and Docker Desktop. Kafka, PostgreSQL and Airflow
run in Docker; see [infra/README.md](infra/README.md) to start them.

```bash
git clone <repo-url>
cd realtime-crypto-stock-platform

python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env    # then fill in API keys
```

---

## 6. Roadmap

- [x] Project scaffold, dependencies, architecture documentation
- [x] Local infra in Docker: ZooKeeper + Kafka + Schema Registry + Kafka UI, PostgreSQL, Airflow (CeleryExecutor). See [infra/README.md](infra/README.md)
- [x] Crypto websocket producer (Binance) → `crypto.trades` topic
- [x] Stock websocket producer (Finnhub) → `stocks.quotes` topic
- [x] Lake writer consumer (bronze, raw JSON Lines): `streaming/consumers/bronze_writer.py`
- [x] Bronze → silver (every 15 min) and silver → gold hourly OHLCV (hourly) Airflow DAGs, Parquet output: `orchestration/dags/`, `pipelines/etl/`
- [ ] Warehouse star schema and ELT models
- [x] FastAPI endpoints with JWT auth and rate limiting: [backend/README.md](backend/README.md)
- [ ] Streamlit dashboard (live + historical)
- [ ] Data quality checks, CI with tests and linting

---

## 7. Production considerations

Things this project is designed to grow into, and how teams handle them at
scale:

- **Schema evolution**: a Schema Registry (Avro/Protobuf) so producers cannot
  break consumers by changing an event's shape.
- **Exactly-once / idempotency**: Kafka idempotent producers plus
  deduplication keys in silver, so retries never double-count a trade.
- **Observability**: consumer lag, freshness and row-count checks with alerts.
- **Data quality**: automated checks (e.g. Great Expectations, dbt tests) as
  gates between lake layers.
- **Secrets**: a secrets manager (AWS Secrets Manager, Vault) instead of
  `.env` files.
- **Infrastructure as code**: Terraform for cloud resources; CI/CD that runs
  tests and deploys containers.

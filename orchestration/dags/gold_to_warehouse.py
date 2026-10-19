"""Gold -> warehouse: upsert hourly candles into PostgreSQL after silver_to_gold.

    check_warehouse_schema -> find_changed_gold_files -> load_gold_file (one per file) -> summarize

Triggered by data, not by a clock. silver_to_gold publishes the `gold_ohlcv_1h`
asset when it finishes successfully, and this DAG runs when that happens. That
is what "every hour, after silver_to_gold finishes" means in practice, and it
is more reliable than a cron guess like "15 minutes past": if gold runs late
the load waits for it, and if gold fails the load does not run on stale files.
(The asset definition must match the one in silver_to_gold.py.)

Each file loads in its own transaction, and a file is recorded as loaded only
in the same transaction as its candles. Either both happen or neither does.
Re-running is safe: see warehouse/loader.py for why the upsert is idempotent.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

GOLD_ASSET = Asset(name="gold_ohlcv_1h", uri="file:///opt/airflow/data_lake/gold/ohlcv_1h")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _connect():
    """A psycopg2 connection to the warehouse, via the `market_warehouse` connection."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="market_warehouse").get_conn()


with DAG(
    dag_id="gold_to_warehouse",
    description="Upsert gold hourly candles into the PostgreSQL ohlcv_1h table",
    schedule=[GOLD_ASSET],
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    tags=["warehouse", "load"],
):

    @task
    def check_warehouse_schema() -> str:
        """Fail fast and clearly if the DDL has not been applied."""
        from warehouse import loader

        conn = _connect()
        try:
            loader.check_schema(conn)
        finally:
            conn.close()
        return "ok"

    @task
    def find_changed_gold_files(_schema: str) -> list[dict]:
        """Gold files that are new, or whose content changed since they were loaded."""
        from warehouse import loader

        conn = _connect()
        try:
            changed = loader.find_changed_files(conn)
        finally:
            conn.close()
        print(f"{len(changed)} gold file(s) to load")
        return changed

    @task(max_active_tis_per_dagrun=2)
    def load_gold_file(item: dict) -> dict:
        from warehouse import loader

        conn = _connect()
        try:
            return loader.load_file(conn, item["file"], item["sha256"])
        finally:
            conn.close()

    @task(trigger_rule="none_failed")
    def summarize(results: list[dict]) -> dict:
        from warehouse import loader

        total = loader.summarize(list(results))
        print(f"gold_to_warehouse summary: {total}")
        return total

    changed = find_changed_gold_files(check_warehouse_schema())
    summarize(load_gold_file.expand(item=changed))

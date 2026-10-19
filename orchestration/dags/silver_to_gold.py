"""Silver -> gold hourly OHLCV candles, every hour.

    check_silver_available -> find_stale_partitions -> build_ohlcv (one per partition) -> summarize

A partition is "stale" when it has no gold file yet or its silver file changed
since gold last read it. Each stale partition is rebuilt in full from silver,
so the result never depends on how many times the job has run (idempotent) and
late-arriving silver data corrects the affected candles on the next run.

See pipelines/etl/silver_to_gold.py for the logic and the reasoning, and
bronze_to_silver.py in this folder for why the DAG file stays thin.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="silver_to_gold",
    description="Aggregate silver trades into hourly OHLCV candles in gold",
    schedule="0 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args=default_args,
    tags=["etl", "gold"],
):

    @task
    def check_silver_available() -> str:
        from pipelines.etl import lake

        silver = lake.silver_dir(lake.lake_root())
        if not silver.is_dir():
            raise FileNotFoundError(f"silver layer not found at {silver}; is data_lake mounted?")
        return str(silver)

    @task
    def find_stale_partitions(_silver: str) -> list[dict]:
        from pipelines.etl import silver_to_gold

        partitions = silver_to_gold.find_stale_partitions()
        print(f"{len(partitions)} partition(s) need their candles (re)built")
        return partitions

    @task(max_active_tis_per_dagrun=2)
    def build_ohlcv(partition: dict) -> dict:
        """Rebuild one partition's hourly candles; raises if they fail the sanity checks."""
        from pipelines.etl import silver_to_gold

        return silver_to_gold.build_partition(
            partition["source"], partition["symbol"], partition["date"])

    @task(trigger_rule="none_failed")
    def summarize(results: list[dict]) -> dict:
        from pipelines.etl import silver_to_gold

        total = silver_to_gold.summarize(list(results))
        print(f"silver_to_gold summary: {total}")
        return total

    stale = find_stale_partitions(check_silver_available())
    summarize(build_ohlcv.expand(partition=stale))

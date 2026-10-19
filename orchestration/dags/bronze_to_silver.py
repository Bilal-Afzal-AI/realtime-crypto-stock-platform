"""Bronze -> silver, every 15 minutes.

    check_bronze_available -> find_new_files -> transform_partition (one per partition) -> summarize

Each arrow means "only if the previous task succeeded" (Airflow's default
all_success rule). In particular, `summarize` does not run if any partition
failed, so a half-processed run is never reported as healthy.

The cleaning logic lives in pipelines/etl/bronze_to_silver.py, not here. A DAG
file is parsed by Airflow every minute, so it should only describe *what runs
in what order*. Keeping the logic in a plain module also lets the unit tests
call it without an Airflow install. The heavy imports (pyarrow) happen inside
the tasks for the same reason: parsing this file stays fast and cheap.

The tasks do not use the schedule's data interval. They work out "what has not
been processed yet" from the lake itself. That is what makes a skipped or
delayed run harmless: the next run simply has more to do, and re-running any
run is a no-op once the files are processed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="bronze_to_silver",
    description="Clean, type and de-duplicate raw bronze trades into silver Parquet",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,           # we process "what's new", so past intervals add nothing
    max_active_runs=1,       # two runs rewriting the same partition would race
    dagrun_timeout=timedelta(minutes=14),  # must finish before the next run is due
    default_args=default_args,
    tags=["etl", "silver"],
):

    @task
    def check_bronze_available() -> str:
        """Fail fast, with a clear message, if the lake is not mounted."""
        from pipelines.etl import lake

        bronze = lake.bronze_dir(lake.lake_root())
        if not bronze.is_dir():
            raise FileNotFoundError(f"bronze layer not found at {bronze}; is data_lake mounted?")
        return str(bronze)

    @task
    def find_new_files(_bronze: str) -> list[dict]:
        """One entry per partition (source/symbol/date) with unprocessed bronze files."""
        from pipelines.etl import bronze_to_silver

        partitions = bronze_to_silver.find_partitions_with_new_files()
        print(f"{len(partitions)} partition(s) have new bronze files")
        return partitions

    @task(max_active_tis_per_dagrun=2)
    def transform_partition(partition: dict) -> dict:
        """Clean + dedupe one partition. Bad records go to silver/_quarantine."""
        from pipelines.etl import bronze_to_silver

        return bronze_to_silver.process_partition(
            partition["source"], partition["symbol"], partition["date"])

    # none_failed: an empty run (nothing new, so every mapped task is skipped)
    # still reports "0 partitions", but a real failure upstream still blocks it.
    @task(trigger_rule="none_failed")
    def summarize(results: list[dict]) -> dict:
        from pipelines.etl import bronze_to_silver

        total = bronze_to_silver.summarize(list(results))
        print(f"bronze_to_silver summary: {total}")
        return total

    new_partitions = find_new_files(check_bronze_available())
    summarize(transform_partition.expand(partition=new_partitions))

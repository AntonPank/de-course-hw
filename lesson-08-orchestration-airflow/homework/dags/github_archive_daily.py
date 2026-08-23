from __future__ import annotations

import logging
from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from gh_sensor import GHArchiveSensor

from include.gh_etl import (
    download,
    load_to_duckdb,
    summarize,
    validate,
)

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


logger = logging.getLogger(__name__)


def download_archive_task(**context):
    ds = context["ds"]
    path = download(ds, LANDING_DIR)
    return path


def validate_file_task(**context):
    path = context["ti"].xcom_pull(task_ids="download_archive")

    validate(path)


def load_to_duckdb_task(**context):
    ds = context["ds"]
    path = context["ti"].xcom_pull(task_ids="download_archive")

    return load_to_duckdb(
        path=path,
        ds=ds,
        db_path=DB_PATH,
    )


def notify_completion_task(**context):
    ds = context["ds"]

    result = summarize(
        ds=ds,
        db_path=DB_PATH,
    )

    logger.info("Summary: %s", result)


with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["arch"],
) as dag:
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=download_archive_task,
    )

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file_task,
    )

    load_to_duckdb_t = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb_task,
    )

    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion_task,
    )

    (
        check_availability
        >> download_archive
        >> validate_file
        >> load_to_duckdb_t
        >> notify_completion
    )

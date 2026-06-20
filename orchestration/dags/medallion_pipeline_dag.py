"""Daily batch DAG: ingest -> Bronze -> Silver -> Gold -> data quality gate.

Each task shells out to the exact same module/script a developer would run
by hand (see docs/RUNBOOK.md) — this DAG only adds scheduling, dependency
ordering, and a failure boundary; it doesn't reimplement any pipeline logic.

Two env vars locate the project, mirroring the LAKEHOUSE_BASE_PATH /
KAFKA_BOOTSTRAP_SERVERS convention already used by the pipeline itself:
  FIFA_REPO_HOME  - path to the fifa-stats-streaming checkout (default: repo
                    root inferred from this file's location)
  FIFA_PYTHON_BIN - python executable with pyspark/delta-spark/etc installed
                    (default: "python3", i.e. whatever's on Airflow's PATH)

The producers run with `--once` against mock data by default (see
ingestion/config/settings.yaml's `mock: true`) — flip that to false once a
RapidAPI key is configured for live ingestion; the DAG itself doesn't need
to change.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_HOME = os.environ.get(
    "FIFA_REPO_HOME", str(Path(__file__).resolve().parents[2])
)
PYTHON_BIN = os.environ.get("FIFA_PYTHON_BIN", "python3")

BASH_ENV = {
    "PYTHONPATH": REPO_HOME,
    "LAKEHOUSE_BASE_PATH": os.environ.get("LAKEHOUSE_BASE_PATH", "file:///tmp/fifa-lakehouse"),
    "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}

default_args = {
    "owner": "fifa-stats-streaming",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_pipeline",
    description="Kafka ingest -> Bronze -> Silver -> Gold -> data quality gate",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fifa-stats-streaming", "medallion"],
) as dag:

    ingest_fixtures = BashOperator(
        task_id="ingest_fixtures",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} -m ingestion.producers.fixtures_producer --once",
        env=BASH_ENV,
    )
    ingest_standings = BashOperator(
        task_id="ingest_standings",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} -m ingestion.producers.standings_producer --once",
        env=BASH_ENV,
    )
    ingest_live_events = BashOperator(
        task_id="ingest_live_events",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} -m ingestion.producers.live_events_producer --once",
        env=BASH_ENV,
    )
    ingest_lineups = BashOperator(
        task_id="ingest_lineups",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} -m ingestion.producers.lineups_producer --once",
        env=BASH_ENV,
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} streaming/jobs/bronze_ingest.py",
        env=BASH_ENV,
    )
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} streaming/jobs/silver_transform.py",
        env=BASH_ENV,
    )
    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} streaming/jobs/gold_aggregate.py",
        env=BASH_ENV,
    )
    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} streaming/jobs/data_quality.py",
        env=BASH_ENV,
    )

    [ingest_fixtures, ingest_standings, ingest_live_events, ingest_lineups] >> bronze_ingest
    bronze_ingest >> silver_transform >> gold_aggregate >> data_quality

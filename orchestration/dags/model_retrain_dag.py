"""Weekly DAG: retrain the match odds model (App 2) and re-deploy it if it
backtests better than what's currently in models/match_odds_model.joblib.

train.py already does the "pick a winner" comparison (ELO-only baseline vs.
calibrated GBM, by walk-forward log-loss — see ml/match_odds/README.md) and
overwrites the deployed model unconditionally. Runs on the synthetic
data bridge until enough real Gold history exists to swap in real
historical fixtures (see ml/match_odds/src/synthetic_data.py docstring).

Weekly cadence is a placeholder, not a tuned SLA — there's no real
training data accumulating yet for this to meaningfully react to.
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
}

default_args = {
    "owner": "fifa-stats-streaming",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="match_odds_model_retrain",
    description="Retrain + re-backtest App 2's match odds model",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fifa-stats-streaming", "ml"],
) as dag:

    train_match_odds_model = BashOperator(
        task_id="train_match_odds_model",
        bash_command=f"cd {REPO_HOME} && {PYTHON_BIN} -m ml.match_odds.src.train",
        env=BASH_ENV,
    )

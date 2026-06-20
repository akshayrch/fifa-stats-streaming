# Orchestration (Airflow)

Two DAGs turn the manual CLI commands from [`docs/RUNBOOK.md`](../docs/RUNBOOK.md)
into scheduled, dependency-ordered, failure-bounded pipelines. Each task is a
`BashOperator` that shells out to the *exact same* module/script a developer
would run by hand — Airflow only adds scheduling and ordering on top, it
doesn't reimplement any pipeline or ML logic.

| DAG | Schedule | What it does |
|---|---|---|
| `medallion_pipeline` | `@daily` | 4 parallel ingestion producers (`--once`) -> `bronze_ingest` -> `silver_transform` -> `gold_aggregate` -> `data_quality` (acts as a quality gate: the last task fails the whole DAG run if any check fails). |
| `match_odds_model_retrain` | `@weekly` | Retrains + re-backtests App 2's match odds model (`ml.match_odds.src.train`), which deploys the better-performing model unconditionally. |

DAG source: [`orchestration/dags/`](dags/).

## Why a separate Airflow venv

Airflow's dependency tree is large and conflict-prone, so it lives in its
own virtualenv (`/opt/airflow-venv` in local dev) rather than the one used
for Spark/ML (`/opt/spark-venv`, see `streaming/README.md`). Airflow itself
never imports `pyspark`/`sklearn`/etc — its `BashOperator` tasks just invoke
the *other* venv's Python via the `FIFA_PYTHON_BIN` env var below. This keeps
the two dependency trees from ever touching.

## Setup

```bash
# 1. Dedicated venv, installed via Airflow's official constraints file
#    (avoids the dependency-resolution nightmare of `pip install apache-airflow`
#    on its own)
python3 -m venv /opt/airflow-venv
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION="$(/opt/airflow-venv/bin/python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
/opt/airflow-venv/bin/pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 2. Point Airflow at this repo's DAGs + metadata DB, instead of the default
#    ~/airflow
export AIRFLOW_HOME=/opt/airflow-home
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/fifa-stats-streaming/orchestration/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
/opt/airflow-venv/bin/airflow db migrate

# 3. Tell the DAGs where the repo lives and which Python has pyspark/sklearn/etc
export FIFA_REPO_HOME=/path/to/fifa-stats-streaming
export FIFA_PYTHON_BIN=/opt/spark-venv/bin/python3

# 4. Also export the env vars the pipeline scripts themselves need
#    (same convention as running them by hand -- see docs/RUNBOOK.md)
export LAKEHOUSE_BASE_PATH="file:///tmp/fifa-lakehouse"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

`FIFA_REPO_HOME`/`FIFA_PYTHON_BIN` default to the repo root (inferred from
the DAG file's own location) and `python3` respectively if unset, but for a
venv-based setup like this you'll want to set both explicitly.

## Running locally

```bash
# All-in-one local scheduler + webserver + executor, for trying the DAGs out
# (http://localhost:8080, default login admin/admin printed on first run)
/opt/airflow-venv/bin/airflow standalone
```

Then unpause `medallion_pipeline` / `match_odds_model_retrain` in the UI, or
trigger a run manually:

```bash
/opt/airflow-venv/bin/airflow dags trigger medallion_pipeline
```

## Verifying without a full scheduler

```bash
# Confirm both DAGs parse with no import errors
/opt/airflow-venv/bin/airflow dags list-import-errors

# Confirm they're registered
/opt/airflow-venv/bin/airflow dags list

# Dry-run a single task end-to-end (runs the real BashOperator command)
/opt/airflow-venv/bin/airflow tasks test medallion_pipeline ingest_fixtures 2025-01-01
/opt/airflow-venv/bin/airflow tasks test match_odds_model_retrain train_match_odds_model 2025-01-01
```

These were used to verify both DAGs during development — see
[`docs/progress.md`](../docs/progress.md) Phase 7 for the results.

## Notes

- Tasks run Spark in one-shot mode (`trigger(availableNow=True)`, the
  default for `bronze_ingest.py`/`silver_transform.py`/`gold_aggregate.py`)
  — no `--continuous` flag, since a DAG run is itself the scheduled trigger.
- `data_quality` exits non-zero on any failed check (see
  `streaming/jobs/data_quality.py`), so wiring it as the last task in
  `medallion_pipeline` is the entire "data quality gate" — no extra DAG
  logic needed.
- Producers run against mock data by default (`ingestion/config/settings.yaml`'s
  `mock: true`). Flip that to `false` once a RapidAPI key is configured for
  live ingestion — the DAG itself doesn't need to change.

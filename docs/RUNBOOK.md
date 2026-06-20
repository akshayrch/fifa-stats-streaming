# Runbook

A from-scratch guide to running, testing, and extending every phase of this
project locally: ingestion -> streaming -> medallion lakehouse -> the 3 AI
apps -> the Streamlit UI -> Airflow orchestration. Each section is also
runnable standalone if you only care about one phase.

For *why* each piece is built the way it is, see [`docs/progress.md`](progress.md)
(per-phase build log) and the per-component `README.md` files it links to.
This doc is the *how*, not the *why*.

## 0. Prerequisites

- Python 3.11+ (3.11.15 used in development)
- Java 17+ (`java -version`) — required by both Kafka and Spark
- ~2GB free disk for Kafka + the local Delta lakehouse

Two isolated virtualenvs, kept separate deliberately (see "Why two venvs"
below):

```bash
# Spark + ML + Streamlit UI
python3 -m venv /opt/spark-venv
/opt/spark-venv/bin/pip install \
  -r ingestion/requirements.txt \
  -r streaming/requirements.txt \
  -r ml/match_odds/requirements.txt \
  -r ml/squad_optimizer/requirements.txt \
  -r ml/tournament_predictor/requirements.txt \
  -r app/requirements.txt

# Airflow (only needed for orchestration/, see orchestration/README.md)
python3 -m venv /opt/airflow-venv
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION="$(/opt/airflow-venv/bin/python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
/opt/airflow-venv/bin/pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

**Why two venvs**: Airflow's dependency tree is large and conflict-prone.
Its DAGs only ever *shell out* to scripts run by the Spark/ML venv's Python
(via `FIFA_PYTHON_BIN`, see `orchestration/README.md`) — Airflow itself
never imports `pyspark`/`sklearn`. Keeping it in its own venv means an
Airflow upgrade can never break the verified Spark/ML stack, and vice versa.

Every command below assumes you're in the repo root with these env vars set
(adjust paths/hosts for your machine):

```bash
export PYTHONPATH=$PWD
export LAKEHOUSE_BASE_PATH="file:///tmp/fifa-lakehouse"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

## 1. Start Kafka

```bash
cd infra && docker compose up -d
# If Docker Hub's anonymous pull rate limit blocks that:
bash infra/run_local_kafka.sh        # runs Kafka 3.8.1 directly, KRaft mode, no Docker

cd ingestion/kafka && ./create_topics.sh --no-docker   # drop --no-docker if using Compose
```

Verify: `nc -z localhost 9092` should succeed, or check `infra/README.md`
for the Kafka UI at `http://localhost:8080` (Compose only).

## 2. Ingestion: Kafka producers (Phase 1)

Mock mode (`mock: true` in `ingestion/config/settings.yaml`, the default) —
no API key needed:

```bash
cp ingestion/config/settings.example.yaml ingestion/config/settings.yaml  # first time only

python -m ingestion.producers.fixtures_producer --once
python -m ingestion.producers.standings_producer --once
python -m ingestion.producers.live_events_producer --once
python -m ingestion.producers.lineups_producer --once
```

**Test**: confirm messages actually landed on Kafka:

```bash
python -m ingestion.consumers.sanity_check_consumer --max-messages 10 --timeout 15
```

Each line shows `[topic] key=... ingest_ts=... endpoint=... payload_size=...`.

**Going live**: set `api_football.rapidapi_key` and `mock: false` in
`settings.yaml`, then drop `--once` to poll continuously. See
`ingestion/README.md`.

## 3. Streaming: Bronze -> Silver -> Gold (Phases 2-3)

```bash
python streaming/jobs/bronze_ingest.py     # Kafka -> bronze.* Delta tables
python streaming/jobs/silver_transform.py  # bronze.* -> silver dims/facts
python streaming/jobs/gold_aggregate.py    # silver.* -> gold feature tables
```

Each defaults to one-shot mode (`trigger(availableNow=True)`: process
everything currently available, then exit) — add `--continuous` to
`bronze_ingest.py`/`silver_transform.py` to run forever, picking up new
Kafka messages as they arrive.

**Test**: read any table back directly:

```bash
python3 - <<'EOF'
from streaming.jobs.spark_session import get_spark, lakehouse_path
spark = get_spark("check")
spark.read.format("delta").load(lakehouse_path("gold", "elo_ratings")).show()
EOF
```

Or run the full data quality suite (52 checks across all three layers):

```bash
python streaming/jobs/data_quality.py
```

Prints a `PASS`/`FAIL` line per check, writes a JSON snapshot to
`$LAKEHOUSE_BASE_PATH/gold/data_quality_report.json` (read directly by the
Streamlit "Pipeline Health" page — no Spark session needed there), and logs
a warning per failure (plus an optional Slack webhook post if
`SLACK_WEBHOOK_URL` is set). Exits non-zero if anything failed — this exit
code is what makes it work as a quality gate in the `medallion_pipeline`
Airflow DAG.

## 4. The 3 ML apps (CLI)

All three read from the Gold tables built in step 3 (with synthetic-data
bridges where real mock data doesn't yet cover the problem shape — see each
app's README for why).

**App 2 — Match Odds Predictor** (train once, then predict any matchup):

```bash
python -m ml.match_odds.src.train
python -m ml.match_odds.src.predict --home "Manchester City" --away Arsenal
python -m ml.match_odds.src.predict --home 50 --away 42   # numeric team_id also works
```

**App 1 — Squad Optimizer** (needs App 2's trained model):

```bash
python -m ml.squad_optimizer.src.recommend --opponent Arsenal
python -m ml.squad_optimizer.src.recommend --opponent 42 --formation 4-3-3
python -m ml.squad_optimizer.src.recommend --opponent Arsenal --budget 650
```

**App 3 — Live Tournament Predictor** (needs App 2's trained model; no
Spark/Gold dependency — fictional teams, in-memory ELO):

```bash
python -m ml.tournament_predictor.src.simulate --trials 10000

# Live-trigger demo (two terminals):
python -m ml.tournament_predictor.src.live_consumer            # terminal 1
python -m ml.tournament_predictor.src.live_consumer \
  --publish-test-result 9001 9002 3 0                           # terminal 2

# Re-simulate from whatever's been recorded so far:
python -m ml.tournament_predictor.src.simulate --from-state
```

## 5. Streamlit app

A web UI over all three apps plus a Pipeline Health page, sharing the same
Spark/ML venv (no separate install beyond `app/requirements.txt`, already
included in step 0).

```bash
streamlit run app/streamlit_app.py
```

Open `http://localhost:8501`. Pages (sidebar nav):

| Page | What it does | Needs |
|---|---|---|
| Match Odds Predictor | Pick two teams (from `silver.dim_team`, or type a name/ID) -> win/draw/loss bar chart + explanation | Gold ELO/form tables, trained App 2 model |
| Squad Optimizer | Opponent + formation (+ optional budget) -> recommended XI table + win-probability uplift vs. a naive lineup | Same as above |
| Tournament Predictor | Run the Monte Carlo simulation interactively (adjustable trial count); a "Record a live result" form drives the same `state.record_result()` + re-simulate path `live_consumer.py` uses on a real Kafka FT event, so you can demo the live-trigger effect without a running broker | Trained App 2 model only — no Spark |
| Pipeline Health | Renders the latest `data_quality_report.json`; a button re-runs `streaming/jobs/data_quality.py` as a subprocess and refreshes | The JSON report (step 3); the re-run button needs `FIFA_PYTHON_BIN` pointed at a Python with pyspark installed |

The first two pages cache the Spark session and loaded model
(`st.cache_resource`, see `app/shared.py`) so only the *first* interaction
on a fresh server pays Spark startup cost.

**Testing without a browser**: this repo's own verification (no GUI
available in CI/dev-container contexts) used
[`streamlit.testing.v1.AppTest`](https://docs.streamlit.io/develop/api-reference/app-testing)
to execute every page's script headlessly and assert no exception was
raised — e.g.:

```python
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("app/pages/1_Match_Odds_Predictor.py", default_timeout=120)
at.run()
assert not at.exception
```

This confirms the page logic runs end-to-end against a real Spark session
and the real lakehouse, but isn't a substitute for opening it in an actual
browser to check layout/UX — do that too before treating a UI change as done.

## 6. Airflow orchestration (Phase 7)

Two DAGs (`medallion_pipeline` @daily, `match_odds_model_retrain` @weekly)
wrap steps 2-4 above into scheduled, dependency-ordered runs. Full setup,
verification commands, and design notes: [`orchestration/README.md`](../orchestration/README.md).

Quick version:

```bash
export AIRFLOW_HOME=/opt/airflow-home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/orchestration/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export FIFA_REPO_HOME=$PWD
export FIFA_PYTHON_BIN=/opt/spark-venv/bin/python3
/opt/airflow-venv/bin/airflow db migrate
/opt/airflow-venv/bin/airflow standalone   # http://localhost:8080
```

## End-to-end smoke test

Everything above, in order, from a clean lakehouse:

```bash
export PYTHONPATH=$PWD
export LAKEHOUSE_BASE_PATH="file:///tmp/fifa-lakehouse"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

bash infra/run_local_kafka.sh
(cd ingestion/kafka && ./create_topics.sh --no-docker)

python -m ingestion.producers.fixtures_producer --once
python -m ingestion.producers.standings_producer --once
python -m ingestion.producers.live_events_producer --once
python -m ingestion.producers.lineups_producer --once

python streaming/jobs/bronze_ingest.py
python streaming/jobs/silver_transform.py
python streaming/jobs/gold_aggregate.py
python streaming/jobs/data_quality.py        # expect ~52 PASS (a few freshness
                                              # checks may fail if Bronze data is
                                              # older than the 25h threshold)

python -m ml.match_odds.src.train
python -m ml.match_odds.src.predict --home 50 --away 42
python -m ml.squad_optimizer.src.recommend --opponent 42
python -m ml.tournament_predictor.src.simulate --trials 2000

streamlit run app/streamlit_app.py   # http://localhost:8501
```

## Troubleshooting

- **`No trained model found at .../match_odds_model.joblib`** — run
  `python -m ml.match_odds.src.train` first; App 1 and App 3 both depend on
  App 2's saved model.
- **Squad Optimizer / Match Odds Predictor show cold-start defaults** —
  expected if `gold.elo_ratings`/`gold.team_form_features` don't have a row
  for that `team_id` yet (no Gold history). Run the full pipeline (step 3)
  against more fixtures, or accept the default-ELO behavior for a brand-new
  team.
- **`data_quality.py` freshness checks fail** — Silver/Gold checks require
  `ingest_ts` within 25h; re-run the ingestion + streaming steps to refresh
  it, or ignore if you're intentionally testing against stale data.
- **Airflow `dags list-import-errors` shows nothing but tasks don't run** —
  confirm `FIFA_PYTHON_BIN` points at the venv with pyspark/sklearn/etc
  installed (`/opt/spark-venv/bin/python3`), not Airflow's own venv.

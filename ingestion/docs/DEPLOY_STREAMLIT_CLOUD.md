# Deploying the public demo (Streamlit Community Cloud)

Goal: a URL anyone — a friend, a Medium/LinkedIn reader — can open with zero
install and no API key, showing the Match Odds Predictor, Squad Optimizer,
and Tournament Predictor live in the browser. Pipeline Health is
intentionally unavailable there, since it needs a real running Kafka/Spark
pipeline that a free hosted demo doesn't have.

## How the demo works

A plain Streamlit Community Cloud container has no Java, no Kafka, and no
live lakehouse. Rather than fake that or skip the deployment, `app/shared.py`
auto-detects this: `get_cached_spark()` returns `None` whenever a real Spark
session can't be started, and every page falls back to a static synthetic
snapshot (`app/demo_data/teams.csv` + `team_stats.json`) built by the exact
same ELO/form rules as the real Gold tables (`app/demo_data/build_snapshot.py`
replays `ml.match_odds.src.synthetic_data`'s ELO update loop with the same
seed). The match-odds model itself isn't committed to the repo — on first
load with no Spark session, `get_cached_model()` trains it in-process from
synthetic data (same `ml.match_odds.src.train.run()` the CLI uses), which
takes ~10-20 seconds once per container and is cached after that.

Net effect: `app/requirements.txt` only needs `streamlit`, `pandas`, `numpy`,
`scikit-learn`, `joblib`, `pulp` — no `pyspark`, no `delta-spark`, no
`confluent-kafka`, no Java. The same `app/streamlit_app.py` entrypoint runs
unmodified locally (against the real pipeline) and on Streamlit Cloud
(against the synthetic snapshot) — no separate demo build to maintain.

## Preview the demo locally first (no Kafka/Spark needed)

```bash
export PYTHONPATH=$PWD
export FIFA_FORCE_DEMO_MODE=true   # forces the same fallback path Streamlit Cloud will use
streamlit run app/streamlit_app.py
```

You should see a "You're viewing the hosted demo" banner and a working
Match Odds Predictor / Squad Optimizer / Tournament Predictor — this is
exactly what Streamlit Cloud will show, fully offline.

## Deploy steps

1. Push this repo to GitHub under your account (`app/` must include
   `shared.py`, `streamlit_app.py`, `pages/`, `demo_data/`, `requirements.txt`
   from this delivery).
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with
   GitHub; authorize access to the repo when prompted.
3. **New app** -> pick the repo and branch -> **Main file path**:
   `app/streamlit_app.py`.
4. Under **Advanced settings**, set the Python version to **3.11** (matches
   local dev). No secrets or environment variables are required — demo mode
   is detected automatically, not configured.
5. Click **Deploy**. First load trains the model (~10-20s); later visits are
   instant until the container restarts.
6. Share the resulting `https://<your-app-name>.streamlit.app` URL.

The app auto-redeploys on every push to the watched branch.

## Limitations of the hosted demo (by design)

- **Pipeline Health is disabled** — it needs a real `data_quality.py` run
  against a real lakehouse.
- **Team data is the synthetic snapshot**, not real fixtures — by design,
  since the real mock pipeline only has a couple of real teams so far (see
  `docs/retrospective.md`'s synthetic-data-bridge pattern). Swapping in real
  Gold-layer data is a `LAKEHOUSE_BASE_PATH` + Spark/Java availability change,
  not a code change — `shared.py` already prefers the real path whenever
  Spark can start.
- **Free-tier container sleeps after inactivity**; the next visit triggers a
  ~30s cold start. Fine for a demo link, not for a real SLA.

For the full live pipeline (Kafka, Spark Structured Streaming, real Bronze/
Silver/Gold, Airflow), see [`RUNBOOK.md`](RUNBOOK.md) — that's a local or
self-hosted run, not part of this hosted demo.

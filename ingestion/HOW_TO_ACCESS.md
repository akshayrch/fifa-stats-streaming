# How to access this project

This repo can be reached in five different ways depending on who you are and
how much you want to install. This doc is a map to the right path — the
actual instructions for each one already live in another doc; this page just
tells you which one to open.

| You are... | Do this | Install needed |
|---|---|---|
| A friend / recruiter / Medium reader who just wants to click around | Open the [live demo](#1-live-demo-zero-install) | None |
| A developer who wants the demo running on your own laptop | [Run the app in demo mode locally](#2-run-the-app-in-demo-mode-locally) | Python + `app/requirements.txt` |
| An engineer who wants the real Kafka/Spark pipeline running | [Follow the full RUNBOOK](#3-run-the-full-pipeline-locally) | Python, Java, ~2GB disk |
| Someone who wants their own public link to share | [Deploy your own copy](#4-deploy-your-own-copy-of-the-demo) | A free Streamlit Cloud account |
| Someone who wants to read about the build instead of running it | [Read the write-ups](#5-read-the-write-ups-instead) | None |

---

## 1. Live demo (zero install)

▶️ **[Live demo](#)** *(placeholder — replace with the actual
`https://<app-name>.streamlit.app` URL once deployed; see §4)*

Opens straight in the browser. No install, no API key, no signup. Runs:

- **Match Odds Predictor**, **Squad Optimizer**, **Tournament Predictor** —
  fully working, against a static synthetic snapshot (20 fictional clubs)
  built with the *same* ELO/form math as the real Gold layer (see
  `app/demo_data/build_snapshot.py`) — not a separate fake.
- **Pipeline Health** — intentionally disabled here, since it needs a real
  running Kafka/Spark pipeline that a free hosted container doesn't have.
  You'll see an explanation in the app instead of a crash.

A banner at the top of the demo always tells you you're in this mode, so
there's never any ambiguity about whether you're looking at real or
synthetic data.

The container sleeps after a period of inactivity — the first visit after a
while takes ~30 seconds to wake up. Normal for the free tier; not indicative
of a bug.

## 2. Run the app in demo mode locally

Same experience as the live demo, but on your own machine, with no Kafka,
Spark, or Java required:

```bash
git clone <this-repo-url>
cd fifa-stats-streaming
python3 -m venv .venv && source .venv/bin/activate
pip install -r app/requirements.txt

export FIFA_FORCE_DEMO_MODE=true
streamlit run app/streamlit_app.py   # http://localhost:8501
```

Full explanation of how the fallback works:
[`docs/DEPLOY_STREAMLIT_CLOUD.md`](DEPLOY_STREAMLIT_CLOUD.md).

## 3. Run the full pipeline locally

For the real thing — live Kafka ingestion, Spark Structured Streaming,
a real Bronze/Silver/Gold lakehouse, Airflow orchestration, and the
Streamlit app reading real Gold data instead of the synthetic snapshot.

This needs Python 3.11+, Java 17+, and two virtualenvs (Spark/ML and
Airflow, kept separate on purpose). Full step-by-step setup, verification
commands, and troubleshooting: **[`docs/RUNBOOK.md`](RUNBOOK.md)**.

## 4. Deploy your own copy of the demo

Want your own shareable link (e.g. to put in your own Medium article, or to
fill in the `[Live demo](#)` placeholder above and in the
[README](../README.md))? It's a free Streamlit Community Cloud deployment,
no secrets or environment variables required — demo mode auto-detects.
Step-by-step: **[`docs/DEPLOY_STREAMLIT_CLOUD.md`](DEPLOY_STREAMLIT_CLOUD.md)**.

## 5. Read the write-ups instead

Eight Medium-article drafts (one per build phase) plus their LinkedIn
teaser versions, covering the architecture decisions, the synthetic-data
bridge pattern, the walk-forward backtest that picked an ELO-only model
over gradient boosting, and the end-to-end retrospective:
**[`docs/articles/`](articles/)** (start at
[`articles/README.md`](articles/README.md) for the index and suggested
reading order).

## 6. Source code

The repo itself — browse it on GitHub, or `git clone` it to read or modify
the code directly. See the main [`README.md`](../README.md) for the
architecture overview and repo structure.

"""FIFA Stats Streaming — Streamlit front end for the 3 ML apps + pipeline health.

Run with: streamlit run app/streamlit_app.py
See docs/RUNBOOK.md for environment setup (Kafka, Spark venv, lakehouse path).

This same entrypoint also runs as the public Streamlit Community Cloud demo
(see docs/DEPLOY_STREAMLIT_CLOUD.md) -- shared.get_cached_spark() returns
None there (no Java/Kafka in that environment) and every page falls back to
a static synthetic snapshot automatically.
"""

import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent
for _p in (str(REPO_ROOT), str(APP_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st

from shared import get_cached_spark, is_demo_mode

st.set_page_config(page_title="FIFA Stats Streaming", page_icon="⚽", layout="wide")

st.title("FIFA Stats Streaming")
st.caption(
    "Real-time football data platform -- Kafka -> Spark Structured Streaming -> "
    "Medallion lakehouse -> 3 AI apps."
)

if is_demo_mode(get_cached_spark()):
    st.warning(
        "**You're viewing the hosted demo.** Match Odds Predictor and Squad "
        "Optimizer run against a static synthetic snapshot (20 fictional "
        "clubs, built with the same ELO/form rules as the real Gold layer -- "
        "see `app/demo_data/`) instead of a live Kafka/Spark pipeline, and "
        "Pipeline Health is disabled here. For the full real-time pipeline, "
        "clone the repo and follow `docs/RUNBOOK.md`."
    )

st.markdown(
    """
Use the sidebar to navigate:

- **Match Odds Predictor** -- win/draw/loss probabilities for any two teams (App 2)
- **Squad Optimizer** -- recommended starting XI vs. an opponent (App 1)
- **Tournament Predictor** -- live qualification/bracket odds for a fictional 8-team tournament (App 3)
- **Pipeline Health** -- latest Bronze/Silver/Gold data quality results

All three apps read from the same Gold-layer feature store built by the medallion pipeline
(`streaming/jobs/bronze_ingest.py` -> `silver_transform.py` -> `gold_aggregate.py`). See
`docs/RUNBOOK.md` for how to run the pipeline and this app end to end.
"""
)

st.info(
    "First time here? Run the medallion pipeline and `streaming/jobs/data_quality.py` "
    "at least once so the Gold tables (and this app's pages) have data to read."
)

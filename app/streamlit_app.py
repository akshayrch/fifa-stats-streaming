"""FIFA Stats Streaming — Streamlit front end for the 3 ML apps + pipeline health.

Run with: streamlit run app/streamlit_app.py
See docs/RUNBOOK.md for environment setup (Kafka, Spark venv, lakehouse path).
"""

import streamlit as st

st.set_page_config(page_title="FIFA Stats Streaming", page_icon="⚽", layout="wide")

st.title("FIFA Stats Streaming")
st.caption(
    "Real-time football data platform -- Kafka -> Spark Structured Streaming -> "
    "Medallion lakehouse -> 3 AI apps."
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

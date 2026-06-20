"""Cached Spark session, model, and Gold/Silver lookups shared across pages.

Streamlit reruns the active page's script on every interaction, so anything
expensive (Spark session, joblib model, table scans) is wrapped in
st.cache_resource / st.cache_data instead of being recreated on every rerun.
A leading underscore on a cached-function param tells Streamlit to skip
hashing it (SparkSession objects aren't hashable in a meaningful way).

Demo-mode fallback: a plain Streamlit Community Cloud deployment has no
Java/Kafka/live lakehouse, and doesn't need one -- get_cached_spark() returns
None whenever a real Spark session can't be started, and every lookup below
falls back to the static synthetic snapshot in app/demo_data/ (same ELO/form
rules as the real Gold tables; see app/demo_data/build_snapshot.py) instead
of failing. `spark is None` is the single signal the rest of the app uses to
detect demo mode -- see is_demo_mode().
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

DEMO_DATA_DIR = Path(__file__).resolve().parent / "demo_data"


@st.cache_resource(show_spinner="Starting Spark session...")
def get_cached_spark():
    if os.environ.get("FIFA_FORCE_DEMO_MODE", "false").lower() in ("1", "true", "yes"):
        return None
    try:
        from streaming.jobs.spark_session import get_spark
        return get_spark("streamlit_app")
    except Exception:
        return None


def is_demo_mode(_spark) -> bool:
    return _spark is None


@st.cache_resource(show_spinner="Loading match odds model...")
def get_cached_model(_spark):
    from ml.match_odds.src.predict import load_model
    from ml.match_odds.src.train import MODEL_PATH

    if _spark is None and not MODEL_PATH.exists():
        from ml.match_odds.src.train import run as train_model
        train_model()  # synthetic data only, no Spark needed -- once per container
    return load_model()


@st.cache_data(ttl=300, show_spinner="Reading latest team stats...")
def get_team_stats(_spark) -> dict:
    if _spark is None:
        with open(DEMO_DATA_DIR / "team_stats.json") as f:
            return {int(k): v for k, v in json.load(f).items()}
    from ml.match_odds.src.features import latest_team_stats
    return latest_team_stats(_spark)


@st.cache_data(ttl=300, show_spinner="Loading team list...")
def get_team_list(_spark) -> pd.DataFrame:
    if _spark is None:
        return pd.read_csv(DEMO_DATA_DIR / "teams.csv")
    from streaming.jobs.spark_session import lakehouse_path
    df = _spark.read.format("delta").load(lakehouse_path("silver", "dim_team"))
    return (
        df.select("team_id", "name")
        .toPandas()
        .sort_values("name")
        .reset_index(drop=True)
    )


def resolve_team_id(_spark, team_list: pd.DataFrame, name_or_id: str) -> int | None:
    """Demo-safe team resolution -- defers to the real silver.dim_team-backed
    resolver whenever a real Spark session is available."""
    if _spark is None:
        s = name_or_id.strip()
        if s.lstrip("-").isdigit():
            return int(s)
        matches = team_list[team_list["name"].str.lower().str.contains(s.lower())]
        return int(matches.iloc[0]["team_id"]) if not matches.empty else None
    from ml.match_odds.src.features import resolve_team_id as _resolve
    return _resolve(_spark, name_or_id)

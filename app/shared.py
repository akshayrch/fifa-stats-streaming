"""Cached Spark session, model, and Gold/Silver lookups shared across pages.

Streamlit reruns the active page's script on every interaction, so anything
expensive (Spark session, joblib model, table scans) is wrapped in
st.cache_resource / st.cache_data instead of being recreated on every rerun.
A leading underscore on the `spark` param tells st.cache_data to skip hashing
it (SparkSession objects aren't hashable in a meaningful way).
"""

from __future__ import annotations

import streamlit as st


@st.cache_resource(show_spinner="Starting Spark session...")
def get_cached_spark():
    from streaming.jobs.spark_session import get_spark
    return get_spark("streamlit_app")


@st.cache_resource(show_spinner="Loading match odds model...")
def get_cached_model():
    from ml.match_odds.src.predict import load_model
    return load_model()


@st.cache_data(ttl=300, show_spinner="Reading latest Gold stats...")
def get_team_stats(_spark) -> dict:
    from ml.match_odds.src.features import latest_team_stats
    return latest_team_stats(_spark)


@st.cache_data(ttl=300, show_spinner="Loading team list...")
def get_team_list(_spark):
    from streaming.jobs.spark_session import lakehouse_path
    df = _spark.read.format("delta").load(lakehouse_path("silver", "dim_team"))
    return (
        df.select("team_id", "name")
        .toPandas()
        .sort_values("name")
        .reset_index(drop=True)
    )

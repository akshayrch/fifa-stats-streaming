"""Match Odds Predictor -- pick two teams, get win/draw/loss probabilities.

Thin Streamlit wrapper around ml.match_odds.src.predict's reusable core
(get_match_probabilities); no prediction logic lives here.
"""

import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
for _p in (str(REPO_ROOT), str(APP_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
import streamlit as st

from shared import get_cached_model, get_cached_spark, get_team_list, get_team_stats, resolve_team_id

from ml.match_odds.src.predict import explain_prediction, get_match_probabilities

st.set_page_config(page_title="Match Odds Predictor", page_icon="⚽")
st.title("Match Odds Predictor")
st.caption("App 2 -- win/draw/loss probabilities from ELO + recent form (Silver/Gold features).")

spark = get_cached_spark()
model = get_cached_model(spark)
team_stats = get_team_stats(spark)
team_list = get_team_list(spark)

col1, col2 = st.columns(2)
if team_list.empty:
    st.warning(
        "No teams found in silver.dim_team yet -- run the medallion pipeline first "
        "(see docs/RUNBOOK.md). Falling back to manual entry."
    )
    home_input = col1.text_input("Home team (name or team_id)", value="50")
    away_input = col2.text_input("Away team (name or team_id)", value="42")
else:
    names = team_list["name"].tolist()
    home_input = col1.selectbox("Home team", names, index=0)
    away_input = col2.selectbox("Away team", names, index=min(1, len(names) - 1))

if st.button("Predict", type="primary"):
    home_id = resolve_team_id(spark, team_list, str(home_input))
    away_id = resolve_team_id(spark, team_list, str(away_input))
    if home_id is None or away_id is None:
        st.error("Could not resolve one of the teams against silver.dim_team.")
    elif home_id == away_id:
        st.error("Pick two different teams.")
    else:
        prob_by_class, feature_row = get_match_probabilities(
            spark, home_id, away_id, model=model, team_stats=team_stats,
        )
        labels = {"H": "Home win", "D": "Draw", "A": "Away win"}
        probs_df = pd.DataFrame(
            {
                "Outcome": [labels[c] for c in ["H", "D", "A"]],
                "Probability": [prob_by_class.get(c, 0.0) for c in ["H", "D", "A"]],
            }
        ).set_index("Outcome")
        st.bar_chart(probs_df)

        c1, c2, c3 = st.columns(3)
        c1.metric(labels["H"], f"{prob_by_class.get('H', 0):.0%}")
        c2.metric(labels["D"], f"{prob_by_class.get('D', 0):.0%}")
        c3.metric(labels["A"], f"{prob_by_class.get('A', 0):.0%}")

        st.info(f"**Top contributing factors:** {explain_prediction(feature_row)}")

        for tid, label in [(home_id, home_input), (away_id, away_input)]:
            if tid not in team_stats:
                st.caption(f"Note: no Gold history for {label} -- used cold-start defaults.")

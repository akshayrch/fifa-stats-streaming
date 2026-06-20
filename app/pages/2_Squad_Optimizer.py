"""Squad Optimizer -- opponent + formation (+ optional budget) -> recommended XI.

Thin Streamlit wrapper around ml.squad_optimizer.src.recommend's pieces; no
optimization logic lives here.
"""

import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
for _p in (str(REPO_ROOT), str(APP_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st

from shared import get_cached_model, get_cached_spark, get_team_list, get_team_stats

from ml.match_odds.src.features import resolve_team_id
from ml.match_odds.src.predict import get_match_probabilities
from ml.squad_optimizer.src.contribution import score_players
from ml.squad_optimizer.src.optimizer import FORMATIONS, naive_xi, select_best_xi
from ml.squad_optimizer.src.recommend import ELO_POINTS_PER_CONTRIBUTION_POINT, describe_swaps
from ml.squad_optimizer.src.synthetic_squad_data import SQUAD_TEAM_ID, generate_squad

st.set_page_config(page_title="Squad Optimizer", page_icon="🧩")
st.title("Squad Optimizer")
st.caption(
    "App 1 -- pick the starting XI (from a synthetic 23-player squad for team_id=50) "
    "that maximizes predicted win probability vs. an opponent."
)

spark = get_cached_spark()
model = get_cached_model()
team_stats = get_team_stats(spark)
team_list = get_team_list(spark)

names = team_list["name"].tolist() if not team_list.empty else []
col1, col2, col3 = st.columns(3)
if names:
    opponent_input = col1.selectbox("Opponent", names, index=0)
else:
    opponent_input = col1.text_input("Opponent (name or team_id)", value="42")
formation = col2.selectbox("Formation", list(FORMATIONS))
use_budget = col3.checkbox("Cap total cost")
budget = col3.number_input("Budget", min_value=0.0, value=650.0, step=10.0) if use_budget else None

if st.button("Recommend XI", type="primary"):
    opponent_id = resolve_team_id(spark, str(opponent_input))
    if opponent_id is None:
        st.error("Could not resolve opponent against silver.dim_team.")
    else:
        squad = score_players(generate_squad())
        try:
            optimized = select_best_xi(squad, formation, budget)
        except ValueError as e:
            st.error(str(e))
        else:
            baseline = naive_xi(squad, formation)
            avg_delta = optimized["contribution_score"].mean() - baseline["contribution_score"].mean()
            elo_offset = avg_delta * ELO_POINTS_PER_CONTRIBUTION_POINT

            opt_proba, _ = get_match_probabilities(
                spark, SQUAD_TEAM_ID, opponent_id, model=model, team_stats=team_stats,
                home_elo_offset=elo_offset,
            )
            base_proba, _ = get_match_probabilities(
                spark, SQUAD_TEAM_ID, opponent_id, model=model, team_stats=team_stats,
                home_elo_offset=0.0,
            )

            st.subheader(f"Recommended XI ({formation})")
            display_cols = ["position", "player_name", "avg_rating", "contribution_score", "cost", "available"]
            st.dataframe(optimized[display_cols].reset_index(drop=True), width='stretch')

            st.subheader("Predicted win probability")
            c1, c2 = st.columns(2)
            c1.metric("Optimized XI -- Home win", f"{opt_proba.get('H', 0):.0%}")
            c2.metric("Naive XI -- Home win", f"{base_proba.get('H', 0):.0%}")
            st.write(
                f"Uplift from optimization: "
                f"**{opt_proba.get('H', 0) - base_proba.get('H', 0):+.1%}** win probability"
            )

            swaps = describe_swaps(optimized, baseline)
            if swaps:
                st.subheader("Key swap explanations")
                for s in swaps:
                    st.write(f"- {s}")
            else:
                st.caption("Optimized XI matches the naive baseline -- no swaps to explain.")

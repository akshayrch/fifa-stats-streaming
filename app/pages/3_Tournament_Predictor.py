"""Live Tournament Predictor -- Monte Carlo qualification/bracket odds for the
fictional 8-team tournament (ml/tournament_predictor/src/structure.py).

The "Record a live result" form below substitutes for a running Kafka
consumer (ml.tournament_predictor.src.live_consumer) so the live-trigger
flow can be demoed without a broker: it calls the same state.record_result()
+ simulate.run_simulation() path the consumer calls on a real FT transition.
No Spark/model caching needed here -- simulate.py loads its own model via
joblib and has no Gold/Spark dependency (fictional teams, in-memory ELO).
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

from ml.tournament_predictor.src.simulate import run_simulation
from ml.tournament_predictor.src.state import load_state, record_result, save_state
from ml.tournament_predictor.src.structure import TEAMS, team_name

st.set_page_config(page_title="Tournament Predictor", page_icon="🏆")
st.title("Live Tournament Predictor")
st.caption(
    "App 3 -- Monte Carlo qualification/bracket odds for a fictional 8-team tournament, "
    "built on App 2's match odds model."
)

st.info(
    "In production this re-simulates automatically when "
    "`python -m ml.tournament_predictor.src.live_consumer` sees a tracked fixture hit "
    "full-time on `football.fixtures.raw`. The form below drives the same "
    "`state.record_result()` + `simulate.run_simulation()` path directly, so you can demo "
    "the live-trigger effect here without a running Kafka consumer."
)

trials = st.slider("Simulation trials", min_value=500, max_value=10_000, value=2_000, step=500)

if "tournament_result" not in st.session_state:
    st.session_state.tournament_result = None


def _simulate() -> None:
    completed = load_state()["completed_results"]
    st.session_state.tournament_result = run_simulation(completed, n_trials=trials)


col_run, col_reset = st.columns(2)
if col_run.button("Run simulation", type="primary"):
    with st.spinner(f"Running {trials:,} trials..."):
        _simulate()

if col_reset.button("Reset tournament state"):
    save_state([])
    st.session_state.tournament_result = None
    st.success("Cleared completed_results -- tournament reset to kickoff.")

st.subheader("Record a live result")
with st.form("record_result_form"):
    team_ids = sorted(TEAMS)
    c1, c2, c3, c4 = st.columns(4)
    home_id = c1.selectbox("Home", team_ids, format_func=team_name)
    away_id = c2.selectbox("Away", team_ids, format_func=team_name, index=1)
    home_goals = c3.number_input("Home goals", min_value=0, max_value=15, value=1, step=1)
    away_goals = c4.number_input("Away goals", min_value=0, max_value=15, value=0, step=1)
    submitted = st.form_submit_button("Record + re-simulate")
    if submitted:
        if home_id == away_id:
            st.error("Pick two different teams.")
        else:
            record_result(int(home_id), int(away_id), int(home_goals), int(away_goals))
            st.success(f"Recorded {team_name(home_id)} {home_goals}-{away_goals} {team_name(away_id)}.")
            with st.spinner(f"Re-simulating ({trials:,} trials)..."):
                _simulate()

result = st.session_state.tournament_result
if result is None:
    completed = load_state()["completed_results"]
    if completed:
        st.caption(
            f"{len(completed)} result(s) already recorded -- click \"Run simulation\" "
            "to see current odds."
        )
else:
    probs = result["probabilities"]
    for group, rows in result["live_standings"].items():
        st.subheader(f"Group {group}")
        table = pd.DataFrame(
            [
                {
                    "Team": team_name(r["team_id"]),
                    "Pld": r["played"], "W": r["won"], "D": r["draw"], "L": r["lost"],
                    "GD": r["gd"], "Pts": r["points"],
                    "Qualify %": f"{probs[r['team_id']]['qualify']:.0%}",
                    "Win group %": f"{probs[r['team_id']]['win_group']:.0%}",
                }
                for r in rows
            ]
        )
        st.dataframe(table, width='stretch', hide_index=True)

    st.subheader("Tournament winner probabilities")
    ranked = sorted(TEAMS, key=lambda tid: -probs[tid]["win_tournament"])
    winner_df = pd.DataFrame(
        {
            "Team": [team_name(tid) for tid in ranked],
            "Win tournament %": [probs[tid]["win_tournament"] for tid in ranked],
        }
    ).set_index("Team")
    st.bar_chart(winner_df)

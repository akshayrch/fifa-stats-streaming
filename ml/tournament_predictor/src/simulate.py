"""Monte Carlo tournament simulation, built on App 2's win-probability model.

For each of N trials: simulate every remaining group fixture (sampling a
result from the calibrated match_odds model, updating each team's in-trial
ELO/form after every match so later fixtures — including the knockout stage
— reflect results earlier in *that* trial), compute group standings,
determine the knockout bracket from those standings, simulate the
semifinals + final, and record who qualified / won the group / won it all.
Aggregating across trials gives each team's probability of reaching each
stage.

No Spark/Gold dependency: these are fictional teams (structure.py) that
don't exist in the real lakehouse, so ELO/form is tracked purely in-memory,
seeded from structure.py's base ELOs and `completed_results` (already-played
matches, empty for a full-tournament simulation, populated by state.py for a
live re-simulation mid-tournament).
"""

from __future__ import annotations

import argparse
import random
from collections import defaultdict

import joblib
import pandas as pd

from ml.match_odds.src.features import FEATURE_COLUMNS, build_feature_row
from ml.match_odds.src.train import MODEL_PATH
from ml.tournament_predictor.src.structure import (
    GROUP_FIXTURES, GROUPS, KNOCKOUT_SEEDING, TEAMS, compute_group_standings, team_name,
)

ELO_K = 32
N_SIMULATIONS_DEFAULT = 10_000


def _load_model():
    if not MODEL_PATH.exists():
        raise SystemExit(
            f"No trained model found at {MODEL_PATH}. Run "
            f"`python -m ml.match_odds.src.train` first."
        )
    return joblib.load(MODEL_PATH)


def _initial_team_state() -> dict[int, dict]:
    return {
        tid: {"elo": info["base_elo"], "recent": []}
        for tid, info in TEAMS.items()
    }


def _apply_result(state: dict, home_id: int, away_id: int, home_goals: int, away_goals: int) -> None:
    """Update both teams' in-trial ELO + rolling form after a match. No home
    advantage term — these are neutral-venue tournament fixtures."""
    r_h, r_a = state[home_id]["elo"], state[away_id]["elo"]
    exp_h = 1.0 / (1.0 + 10.0 ** ((r_a - r_h) / 400.0))

    if home_goals > away_goals:
        act_h, pts_h, pts_a = 1.0, 3, 0
    elif home_goals == away_goals:
        act_h, pts_h, pts_a = 0.5, 1, 1
    else:
        act_h, pts_h, pts_a = 0.0, 0, 3

    state[home_id]["elo"] = r_h + ELO_K * (act_h - exp_h)
    state[away_id]["elo"] = r_a + ELO_K * ((1.0 - act_h) - (1.0 - exp_h))
    state[home_id]["recent"] = (state[home_id]["recent"] + [(pts_h, home_goals, away_goals)])[-5:]
    state[away_id]["recent"] = (state[away_id]["recent"] + [(pts_a, away_goals, home_goals)])[-5:]


def _team_stats_for_features(state: dict) -> dict[int, dict[str, float]]:
    out = {}
    for tid, s in state.items():
        recent = s["recent"]
        out[tid] = {
            "elo": s["elo"],
            "ppg_last5": (sum(m[0] for m in recent) / len(recent)) if recent else 1.0,
            "avg_gf_last5": (sum(m[1] for m in recent) / len(recent)) if recent else 1.2,
        }
    return out


def _match_outcome_probs(model, state: dict, team_a: int, team_b: int) -> dict:
    """P(team_a wins) / P(team_b wins) / P(draw) for a *neutral-venue* match.

    The model was trained on club fixtures with a real home side, so it
    bakes in a home-advantage effect. For a neutral-venue tournament match
    we cancel that out by averaging team_a-as-home and team_b-as-home framings.
    """
    team_stats = _team_stats_for_features(state)
    row_a_home = build_feature_row(team_a, team_b, team_stats)
    row_b_home = build_feature_row(team_b, team_a, team_stats)
    X = pd.DataFrame([row_a_home, row_b_home])[FEATURE_COLUMNS]
    proba = model.predict_proba(X)
    classes = list(model.classes_)
    p_a_home = dict(zip(classes, proba[0]))
    p_b_home = dict(zip(classes, proba[1]))

    p_a = (p_a_home.get("H", 0.0) + p_b_home.get("A", 0.0)) / 2.0
    p_b = (p_a_home.get("A", 0.0) + p_b_home.get("H", 0.0)) / 2.0
    p_d = (p_a_home.get("D", 0.0) + p_b_home.get("D", 0.0)) / 2.0
    total = p_a + p_b + p_d
    return {"a": p_a / total, "b": p_b / total, "d": p_d / total}


def _sample_scoreline(rng: random.Random, outcome: str) -> tuple[int, int]:
    """The match_odds model only predicts win/draw/loss, not exact goals —
    layer on a simple scoreline generator (winner's/loser's goal counts drawn
    from hand-picked-but-realistic distributions) purely so group-stage goal
    difference has something to tiebreak on."""
    if outcome == "draw":
        g = rng.choices([0, 1, 2], weights=[0.45, 0.35, 0.20])[0]
        return g, g
    winner_goals = rng.choices([1, 2, 3], weights=[0.50, 0.35, 0.15])[0]
    loser_goals = rng.choices([0, 1], weights=[0.65, 0.35])[0]
    return winner_goals, loser_goals


def _simulate_match(model, state: dict, team_a: int, team_b: int, rng: random.Random,
                     allow_draw: bool = True) -> tuple[int, int, int]:
    """Returns (winner_team_id_or_0_for_draw, team_a_goals, team_b_goals)."""
    probs = _match_outcome_probs(model, state, team_a, team_b)
    roll = rng.random()
    if roll < probs["a"]:
        outcome = "a"
    elif roll < probs["a"] + probs["b"]:
        outcome = "b"
    else:
        outcome = "draw"

    if outcome == "draw" and not allow_draw:
        # Knockout: no draws allowed — simplified as a fair coin flip
        # (stand-in for penalty shootout / extra time, which the model has
        # no signal to predict).
        outcome = "a" if rng.random() < 0.5 else "b"

    if outcome == "a":
        ga, gb = _sample_scoreline(rng, "win")
        _apply_result(state, team_a, team_b, ga, gb)
        return team_a, ga, gb
    elif outcome == "b":
        gb, ga = _sample_scoreline(rng, "win")
        _apply_result(state, team_a, team_b, ga, gb)
        return team_b, ga, gb
    else:
        ga, gb = _sample_scoreline(rng, "draw")
        _apply_result(state, team_a, team_b, ga, gb)
        return 0, ga, gb


def _run_one_trial(model, completed_results: list[dict], rng: random.Random) -> dict:
    state = _initial_team_state()
    for r in completed_results:
        _apply_result(state, r["home_id"], r["away_id"], r["home_goals"], r["away_goals"])

    played_pairs = {frozenset((r["home_id"], r["away_id"])) for r in completed_results}
    group_results = {g: list(completed_results) for g in GROUPS}

    for group, fixtures in GROUP_FIXTURES.items():
        for home_id, away_id in fixtures:
            if frozenset((home_id, away_id)) in played_pairs:
                continue
            winner, ga, gb = _simulate_match(model, state, home_id, away_id, rng, allow_draw=True)
            group_results[group].append({
                "home_id": home_id, "away_id": away_id, "home_goals": ga, "away_goals": gb,
            })

    standings = {
        g: compute_group_standings(team_ids, group_results[g])
        for g, team_ids in GROUPS.items()
    }
    qualifiers = {g: [row["team_id"] for row in rows[:2]] for g, rows in standings.items()}

    bracket_winners = {}
    finalists = []
    for round_name, group_a, rank_a, group_b, rank_b in KNOCKOUT_SEEDING:
        team_a = qualifiers[group_a][rank_a]
        team_b = qualifiers[group_b][rank_b]
        winner, _, _ = _simulate_match(model, state, team_a, team_b, rng, allow_draw=False)
        bracket_winners[round_name] = winner
        finalists.append(winner)

    champion, _, _ = _simulate_match(model, state, finalists[0], finalists[1], rng, allow_draw=False)

    return {
        "group_winners": {g: rows[0]["team_id"] for g, rows in standings.items()},
        "qualifiers": qualifiers,
        "finalists": set(finalists),
        "champion": champion,
    }


def run_simulation(completed_results: list[dict] | None = None,
                    n_trials: int = N_SIMULATIONS_DEFAULT,
                    seed: int = 42) -> dict:
    """Runs the Monte Carlo simulation and returns aggregated probabilities:
    {team_id: {"qualify": p, "win_group": p, "reach_final": p, "win_tournament": p}}
    plus the live standings (from completed_results only, not simulated).
    """
    model = _load_model()
    completed_results = completed_results or []
    rng = random.Random(seed)

    counts = {tid: defaultdict(int) for tid in TEAMS}
    for _ in range(n_trials):
        result = _run_one_trial(model, completed_results, rng)
        for group, qids in result["qualifiers"].items():
            for tid in qids:
                counts[tid]["qualify"] += 1
        for tid in result["group_winners"].values():
            counts[tid]["win_group"] += 1
        for tid in result["finalists"]:
            counts[tid]["reach_final"] += 1
        counts[result["champion"]]["win_tournament"] += 1

    probabilities = {
        tid: {
            "qualify": counts[tid]["qualify"] / n_trials,
            "win_group": counts[tid]["win_group"] / n_trials,
            "reach_final": counts[tid]["reach_final"] / n_trials,
            "win_tournament": counts[tid]["win_tournament"] / n_trials,
        }
        for tid in TEAMS
    }

    live_standings = {
        g: compute_group_standings(team_ids, completed_results)
        for g, team_ids in GROUPS.items()
    }

    return {"probabilities": probabilities, "live_standings": live_standings, "n_trials": n_trials}


def print_report(sim_result: dict) -> None:
    probabilities = sim_result["probabilities"]
    for group, rows in sim_result["live_standings"].items():
        print(f"\nGroup {group} standings + qualification probabilities "
              f"({sim_result['n_trials']:,} simulations)")
        for row in rows:
            tid = row["team_id"]
            p = probabilities[tid]
            print(f"  {team_name(tid):<10} Pld {row['played']}, Pts {row['points']:<3} "
                  f"-> Qualify: {p['qualify']:.0%}  | Win group: {p['win_group']:.0%}")

    print("\nTournament winner probabilities (top 5)")
    ranked = sorted(TEAMS, key=lambda tid: -probabilities[tid]["win_tournament"])[:5]
    print("  " + "  ".join(f"{team_name(tid)}: {probabilities[tid]['win_tournament']:.1%}" for tid in ranked))


def main() -> None:
    parser = argparse.ArgumentParser(description="Monte Carlo tournament simulation.")
    parser.add_argument("--trials", type=int, default=N_SIMULATIONS_DEFAULT)
    parser.add_argument("--from-state", action="store_true",
                         help="Load completed results from gold/tournament_state.json "
                              "instead of simulating the full tournament from scratch.")
    args = parser.parse_args()

    completed_results = []
    if args.from_state:
        from ml.tournament_predictor.src.state import load_state
        completed_results = load_state()["completed_results"]

    result = run_simulation(completed_results, n_trials=args.trials)
    print_report(result)


if __name__ == "__main__":
    main()

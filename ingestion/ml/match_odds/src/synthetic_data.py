"""Synthetic historical match generator — stand-in training data for Phase 4.

Why this exists: the design doc (docs/apps/02_match_odds_predictor.md) calls
for training on "historical fixtures" with walk-forward backtesting across
seasons. The real pipeline (Phase 1-3) has only run in mock mode so far, so
`silver.fact_match` has exactly 1 finished match — nowhere near enough to fit
or validate a classifier. Rather than block Phase 4 on a live API key, this
module simulates several seasons of a round-robin league using the same ELO
update rule as `gold_aggregate.build_elo_ratings`, so the feature columns
(`elo_diff`, `*_ppg_last5`, `*_avg_gf_last5`) are computed identically to the
real Gold table and the resulting dataset has genuine, learnable structure
(stronger teams win more, not pure noise).

This is clearly a bridge: once the live API key is producing real fixtures,
`features.py`'s `load_gold_training_data()` reads straight from
`gold.match_prediction_features`-equivalent historical results, and this
module becomes unnecessary. Swapping the training source in `train.py` is a
one-line change.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

ELO_K = 32
ELO_BASE = 1500.0
HOME_ADVANTAGE_ELO = 60.0  # points of effective ELO boost for the home side


def generate_synthetic_seasons(
    n_teams: int = 20,
    n_seasons: int = 6,
    seed: int = 42,
) -> pd.DataFrame:
    """Simulate `n_seasons` double round-robin seasons among `n_teams`.

    Each team has a hidden, fixed `true_strength` that drives goal-scoring
    rates (Poisson). ELO and rolling form are tracked and recorded *before*
    each match (no leakage), using the same update rule as gold_aggregate.py.

    Returns one row per match with columns matching the Gold feature schema
    plus the actual result label.
    """
    rng = np.random.default_rng(seed)
    team_ids = list(range(1, n_teams + 1))
    true_strength = {tid: rng.normal(0, 1) for tid in team_ids}
    elo = {tid: ELO_BASE for tid in team_ids}
    recent: dict[int, list[tuple[int, int, int]]] = {tid: [] for tid in team_ids}

    rows = []
    seq = 0
    for season in range(n_seasons):
        fixtures = [(h, a) for h in team_ids for a in team_ids if h != a]
        rng.shuffle(fixtures)

        for home, away in fixtures:
            seq += 1
            r_h, r_a = elo[home], elo[away]
            exp_h = 1.0 / (1.0 + 10.0 ** ((r_a - (r_h + HOME_ADVANTAGE_ELO)) / 400.0))

            h_last5 = recent[home][-5:]
            a_last5 = recent[away][-5:]
            home_ppg_last5 = float(np.mean([m[0] for m in h_last5])) if h_last5 else 1.0
            away_ppg_last5 = float(np.mean([m[0] for m in a_last5])) if a_last5 else 1.0
            home_avg_gf_last5 = float(np.mean([m[1] for m in h_last5])) if h_last5 else 1.2
            away_avg_gf_last5 = float(np.mean([m[1] for m in a_last5])) if a_last5 else 1.2

            home_lambda = max(0.3, 1.35 + 0.5 * (true_strength[home] - true_strength[away]))
            away_lambda = max(0.3, 1.05 + 0.5 * (true_strength[away] - true_strength[home]))
            home_goals = int(rng.poisson(home_lambda))
            away_goals = int(rng.poisson(away_lambda))

            if home_goals > away_goals:
                result, act_h, pts_h, pts_a = "H", 1.0, 3, 0
            elif home_goals == away_goals:
                result, act_h, pts_h, pts_a = "D", 0.5, 1, 1
            else:
                result, act_h, pts_h, pts_a = "A", 0.0, 0, 3

            rows.append({
                "match_seq": seq,
                "season": season,
                "home_team_id": home,
                "away_team_id": away,
                "elo_diff": round(r_h - r_a, 2),
                "home_ppg_last5": round(home_ppg_last5, 2),
                "away_ppg_last5": round(away_ppg_last5, 2),
                "home_avg_gf_last5": round(home_avg_gf_last5, 2),
                "away_avg_gf_last5": round(away_avg_gf_last5, 2),
                "home_goals": home_goals,
                "away_goals": away_goals,
                "result": result,
            })

            elo[home] = r_h + ELO_K * (act_h - exp_h)
            elo[away] = r_a + ELO_K * ((1.0 - act_h) - (1.0 - exp_h))
            recent[home].append((pts_h, home_goals, away_goals))
            recent[away].append((pts_a, away_goals, home_goals))

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = generate_synthetic_seasons()
    print(df.shape, "matches across", df["season"].nunique(), "seasons")
    print(df["result"].value_counts(normalize=True).round(3))
    print(df.head())

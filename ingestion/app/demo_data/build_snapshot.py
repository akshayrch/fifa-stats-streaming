"""One-off generator for the Streamlit Cloud demo's static data snapshot.

Why this exists: a plain Streamlit Community Cloud deployment has no Kafka,
no Spark/Java, and no live lakehouse -- but app/shared.py still needs *some*
team list + ELO/form numbers to drive the Match Odds Predictor and Squad
Optimizer pages. Rather than fail or fake the constraint away, this script
replays the exact same ELO-update loop and seed as
ml.match_odds.src.synthetic_data.generate_synthetic_seasons (the same data
the deployed model is trained on) far enough to capture each team's *final*
state, and writes it to two flat files the demo build reads with plain
pandas/json -- no Spark needed. Same bridge pattern as every other synthetic
generator in this repo (see docs/retrospective.md).

Re-run this only if synthetic_data.py's n_teams/n_seasons/seed change:
    python -m app.demo_data.build_snapshot
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np

from ml.match_odds.src.synthetic_data import ELO_BASE, ELO_K, HOME_ADVANTAGE_ELO

OUT_DIR = Path(__file__).resolve().parent

TEAM_NAMES = [
    "Northbridge United", "Castellan FC", "Port Meridian", "Vale Athletic",
    "Stonegate Rovers", "Ashford Town", "Brackenfield FC", "Solway Harbour",
    "Kestrel City", "Meadowlark United", "Granite Park", "Oakhurst FC",
    "Silverlake Rovers", "Thornbury Athletic", "Westmere United", "Calderwood FC",
    "Foxglen Town", "Ridgeway Albion", "Marlow Harbour", "Brindle City",
]


def build(n_teams: int = 20, n_seasons: int = 6, seed: int = 42) -> None:
    """Mirrors generate_synthetic_seasons' loop (same seed/order) but keeps
    each team's final ELO + recent results instead of discarding them."""
    rng = np.random.default_rng(seed)
    team_ids = list(range(1, n_teams + 1))
    true_strength = {tid: rng.normal(0, 1) for tid in team_ids}
    elo = {tid: ELO_BASE for tid in team_ids}
    recent: dict[int, list[tuple[int, int, int]]] = {tid: [] for tid in team_ids}

    for _season in range(n_seasons):
        fixtures = [(h, a) for h in team_ids for a in team_ids if h != a]
        rng.shuffle(fixtures)
        for home, away in fixtures:
            r_h, r_a = elo[home], elo[away]
            exp_h = 1.0 / (1.0 + 10.0 ** ((r_a - (r_h + HOME_ADVANTAGE_ELO)) / 400.0))
            home_lambda = max(0.3, 1.35 + 0.5 * (true_strength[home] - true_strength[away]))
            away_lambda = max(0.3, 1.05 + 0.5 * (true_strength[away] - true_strength[home]))
            home_goals = int(rng.poisson(home_lambda))
            away_goals = int(rng.poisson(away_lambda))

            if home_goals > away_goals:
                act_h, pts_h, pts_a = 1.0, 3, 0
            elif home_goals == away_goals:
                act_h, pts_h, pts_a = 0.5, 1, 1
            else:
                act_h, pts_h, pts_a = 0.0, 0, 3

            elo[home] = r_h + ELO_K * (act_h - exp_h)
            elo[away] = r_a + ELO_K * ((1.0 - act_h) - (1.0 - exp_h))
            recent[home].append((pts_h, home_goals, away_goals))
            recent[away].append((pts_a, away_goals, home_goals))

    team_stats = {}
    for tid in team_ids:
        last5 = recent[tid][-5:]
        team_stats[str(tid)] = {
            "elo": round(elo[tid], 2),
            "ppg_last5": round(float(np.mean([m[0] for m in last5])), 2) if last5 else 1.0,
            "avg_gf_last5": round(float(np.mean([m[1] for m in last5])), 2) if last5 else 1.2,
        }

    with open(OUT_DIR / "team_stats.json", "w") as f:
        json.dump(team_stats, f, indent=2)

    with open(OUT_DIR / "teams.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["team_id", "name"])
        for tid, name in zip(team_ids, TEAM_NAMES):
            writer.writerow([tid, name])

    print(f"Wrote {OUT_DIR / 'teams.csv'} and {OUT_DIR / 'team_stats.json'} ({n_teams} teams)")


if __name__ == "__main__":
    build()

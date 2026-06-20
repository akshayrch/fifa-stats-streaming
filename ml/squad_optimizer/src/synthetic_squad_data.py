"""Synthetic squad generator — stand-in player pool for Phase 5.

Why this exists: the design doc (docs/apps/01_squad_optimizer.md) needs a
full squad (GK/DEF/MID/FWD, ~20+ players) with season stats *and* positions
to run a lineup optimization against. The real mock pipeline's
`silver.dim_player` (4 players, from one lineup payload) and
`gold.player_season_stats` (2 players, from one player-stats payload) don't
even overlap — there's no single team with enough real, positioned,
stat-bearing players to select an XI from yet.

This module generates a realistic 23-player squad for **team_id=50**
(Manchester City in the mock Silver/Gold data from Phase 3) so the optimizer
has something to optimize, while the *opponent* side of the win-probability
calculation still uses team_id=50's and the opponent's real ELO/form from
Phase 3-4. Same bridge pattern as ml/match_odds/src/synthetic_data.py — swap
this for a real loader over `gold.player_season_stats` + `silver.dim_player`
once a real squad's worth of data exists.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SQUAD_TEAM_ID = 50  # Manchester City, matches the Phase 3 mock Gold data

# (position, count, rating_mean, rating_std, goals_lambda, assists_lambda)
POSITION_PROFILE = {
    "GK": dict(count=3, rating_mean=6.8, rating_std=0.3, goals_lambda=0.0, assists_lambda=0.05),
    "DEF": dict(count=8, rating_mean=6.7, rating_std=0.4, goals_lambda=1.2, assists_lambda=1.5),
    "MID": dict(count=7, rating_mean=6.9, rating_std=0.4, goals_lambda=3.5, assists_lambda=4.0),
    "FWD": dict(count=5, rating_mean=7.1, rating_std=0.5, goals_lambda=9.0, assists_lambda=3.0),
}

# Minimum *available* players per position any supported formation might
# need (see optimizer.py FORMATIONS) — generation retries/fixes availability
# flags so the optimizer always has a feasible squad to pick from.
MIN_AVAILABLE = {"GK": 1, "DEF": 4, "MID": 3, "FWD": 2}

FIRST_NAMES = ["Marcus", "Diego", "Kwame", "Lukas", "Mateo", "Yusuf", "Theo",
               "Nikolaj", "Tomas", "Andre", "Bilal", "Sven", "Pierre", "Rafa",
               "Cole", "Hugo", "Idris", "Joao", "Erik", "Malik", "Sebastian",
               "Noah", "Aaron"]
LAST_NAMES = ["Reyes", "Okafor", "Lindgren", "Costa", "Adeyemi", "Brandt",
              "Silva", "Novak", "Hassan", "Lindqvist", "Dubois", "Moreno",
              "Akinola", "Bergstrom", "Tanaka", "Mensah", "Olsen", "Fernandez",
              "Kovac", "Webb", "Larsson", "Dahl", "Sousa"]


def generate_squad(seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    player_id = 90001
    name_pool = list(zip(FIRST_NAMES, LAST_NAMES))
    rng.shuffle(name_pool)

    for position, profile in POSITION_PROFILE.items():
        for _ in range(profile["count"]):
            first, last = name_pool.pop()
            rating = float(np.clip(rng.normal(profile["rating_mean"], profile["rating_std"]), 5.5, 9.2))
            appearances = int(rng.integers(10, 35))
            goals = int(rng.poisson(profile["goals_lambda"] * appearances / 30))
            assists = int(rng.poisson(profile["assists_lambda"] * appearances / 30))
            minutes_played = appearances * int(rng.integers(60, 90))
            shots_total = goals * int(rng.integers(3, 7)) + int(rng.integers(0, 10))
            shots_on_target = int(shots_total * rng.uniform(0.35, 0.55))
            passes_total = int(rng.integers(400, 2200))
            cost = round(max(1.0, rating * 8 + goals * 1.5 + assists * 1.0 + rng.normal(0, 3)), 1)

            rows.append({
                "player_id": player_id,
                "player_name": f"{first} {last}",
                "team_id": SQUAD_TEAM_ID,
                "position": position,
                "appearances": appearances,
                "goals": goals,
                "assists": assists,
                "minutes_played": minutes_played,
                "avg_rating": round(rating, 2),
                "shots_total": shots_total,
                "shots_on_target": shots_on_target,
                "passes_total": passes_total,
                "cost": cost,
                "available": bool(rng.random() > 0.15),  # ~15% injured/suspended
            })
            player_id += 1

    df = pd.DataFrame(rows)

    # Guarantee feasibility: if availability flags leave a position short of
    # what any formation needs, flip the highest-rated unavailable player(s)
    # in that position back to available.
    for position, min_count in MIN_AVAILABLE.items():
        mask = df["position"] == position
        available_count = int((mask & df["available"]).sum())
        shortfall = min_count - available_count
        if shortfall > 0:
            candidates = df[mask & ~df["available"]].sort_values("avg_rating", ascending=False)
            flip_ids = candidates.head(shortfall)["player_id"]
            df.loc[df["player_id"].isin(flip_ids), "available"] = True

    return df.reset_index(drop=True)


if __name__ == "__main__":
    df = generate_squad()
    print(df.shape, "players;", int(df["available"].sum()), "available")
    print(df.groupby("position")["available"].agg(["count", "sum"]))
    print(df.sort_values("avg_rating", ascending=False).head(10).to_string(index=False))

"""Per-player contribution score — Stage 1 of the design doc's two-stage approach.

The design doc explicitly calls for starting simple: "a per-position rating
score (weighted combination of player_season_stats features) — then iterate
toward a learned model once enough Gold data exists." This is that simple
scoring function, not a trained model — there's no label to train against
yet (would need many historical lineups + results, which the live pipeline
doesn't have either). `optimizer.py` treats this as the per-player value the
constraint solver maximizes.
"""

from __future__ import annotations

import pandas as pd

# Per-position feature weights. avg_rating dominates everywhere (it's
# already a holistic per-match grade); goals/assists are weighted by how
# much they matter for that position; minutes_played as a fraction of
# possible minutes is a fitness/reliability proxy common to all positions.
WEIGHTS = {
    "GK":  {"avg_rating": 6.0, "goals": 0.0, "assists": 0.5, "availability": 1.0},
    "DEF": {"avg_rating": 5.0, "goals": 0.5, "assists": 0.8, "availability": 1.0},
    "MID": {"avg_rating": 4.5, "goals": 0.8, "assists": 1.0, "availability": 1.0},
    "FWD": {"avg_rating": 4.0, "goals": 1.2, "assists": 0.8, "availability": 1.0},
}
MAX_REASONABLE_APPEARANCES = 38  # a full league season, for normalizing availability


def score_players(squad: pd.DataFrame) -> pd.DataFrame:
    """Adds a `contribution_score` column to a squad DataFrame.

    Expects columns: position, avg_rating, goals, assists, appearances.
    Per-90 rates (goals/assists per appearance) avoid rewarding players who
    just played more minutes; availability_factor rewards squad members
    who've actually been getting picked (proxy for fitness/form/trust).
    """
    df = squad.copy()
    weights = df["position"].map(WEIGHTS)
    goals_per_app = df["goals"] / df["appearances"].clip(lower=1)
    assists_per_app = df["assists"] / df["appearances"].clip(lower=1)
    availability_factor = (df["appearances"] / MAX_REASONABLE_APPEARANCES).clip(upper=1.0)

    df["contribution_score"] = (
        weights.map(lambda w: w["avg_rating"]) * df["avg_rating"]
        + weights.map(lambda w: w["goals"]) * goals_per_app * 10
        + weights.map(lambda w: w["assists"]) * assists_per_app * 10
        + weights.map(lambda w: w["availability"]) * availability_factor * 2
    ).round(2)
    return df

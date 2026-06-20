"""CLI: two teams in -> win/draw/loss probabilities out.

Usage:
    python -m ml.match_odds.src.predict --home "Manchester City" --away Arsenal
    python -m ml.match_odds.src.predict --home 50 --away 42

Team args accept either a silver.dim_team name (case-insensitive substring
match) or a numeric team_id. Falls back to cold-start defaults for teams with
no Gold history yet (see features.py DEFAULT_*).
"""

from __future__ import annotations

import argparse

import joblib
import pandas as pd

from ml.match_odds.src.features import FEATURE_COLUMNS, build_feature_row, latest_team_stats, resolve_team_id
from ml.match_odds.src.train import MODEL_PATH

CLASS_NAME = {"H": "Home win", "D": "Draw", "A": "Away win"}


def _explain(feature_row: dict[str, float]) -> str:
    """Lightweight, dependency-free explanation: sign + magnitude of the ELO
    gap and form differential. (SHAP would give a model-faithful per-feature
    attribution — left as a Phase 7 polish item to avoid pulling in a heavy
    extra dependency for an MVP CLI.)"""
    parts = []
    elo_diff = feature_row["elo_diff"]
    if abs(elo_diff) >= 10:
        side = "home" if elo_diff > 0 else "away"
        parts.append(f"ELO gap favors {side} ({elo_diff:+.0f})")
    ppg_diff = feature_row["home_ppg_last5"] - feature_row["away_ppg_last5"]
    if abs(ppg_diff) >= 0.3:
        side = "home" if ppg_diff > 0 else "away"
        parts.append(f"recent form favors {side} ({ppg_diff:+.2f} PPG)")
    gf_diff = feature_row["home_avg_gf_last5"] - feature_row["away_avg_gf_last5"]
    if abs(gf_diff) >= 0.3:
        side = "home" if gf_diff > 0 else "away"
        parts.append(f"attacking output favors {side} ({gf_diff:+.2f} GF/match)")
    parts.append("home advantage (+)")
    return ", ".join(parts) if parts else "no strong signal either way"


def load_model():
    if not MODEL_PATH.exists():
        raise SystemExit(
            f"No trained model found at {MODEL_PATH}. Run "
            f"`python -m ml.match_odds.src.train` first."
        )
    return joblib.load(MODEL_PATH)


def get_match_probabilities(
    spark,
    home_id: int,
    away_id: int,
    model=None,
    team_stats: dict[int, dict[str, float]] | None = None,
    home_elo_offset: float = 0.0,
    away_elo_offset: float = 0.0,
) -> tuple[dict[str, float], dict[str, float]]:
    """Reusable core of the predictor: feature row + calibrated probabilities
    for a home/away matchup, with optional ELO offsets for "what if this side
    fielded a stronger/weaker lineup" scenarios.

    Returns (prob_by_class, feature_row). Used directly by predict() below
    and by ml/squad_optimizer/src/recommend.py, which reuses this win
    probability function rather than re-implementing match prediction.
    """
    if model is None:
        model = load_model()
    if team_stats is None:
        team_stats = latest_team_stats(spark)

    feature_row = build_feature_row(home_id, away_id, team_stats, home_elo_offset, away_elo_offset)
    X = pd.DataFrame([feature_row])[FEATURE_COLUMNS]
    proba = model.predict_proba(X)[0]
    classes = list(model.classes_)
    prob_by_class = dict(zip(classes, proba))
    return prob_by_class, feature_row


def predict(spark, home_id: int, away_id: int, home_label: str, away_label: str) -> None:
    model = load_model()
    team_stats = latest_team_stats(spark)
    prob_by_class, feature_row = get_match_probabilities(
        spark, home_id, away_id, model=model, team_stats=team_stats
    )

    print(f"\n{home_label} (home) vs {away_label} (away)")
    for c in ["H", "D", "A"]:
        print(f"  {CLASS_NAME[c]:<9}: {prob_by_class.get(c, 0.0):.0%}")
    print(f"Top contributing factors: {_explain(feature_row)}")

    for tid, label in [(home_id, home_label), (away_id, away_label)]:
        if tid not in team_stats:
            print(f"  (note: no Gold history for {label} — used cold-start defaults)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict match odds for two teams.")
    parser.add_argument("--home", required=True, help="Home team name or team_id")
    parser.add_argument("--away", required=True, help="Away team name or team_id")
    args = parser.parse_args()

    from streaming.jobs.spark_session import get_spark
    spark = get_spark("match_odds_predict")

    home_id = resolve_team_id(spark, args.home)
    away_id = resolve_team_id(spark, args.away)
    if home_id is None:
        raise SystemExit(f"Could not resolve home team '{args.home}' in silver.dim_team")
    if away_id is None:
        raise SystemExit(f"Could not resolve away team '{args.away}' in silver.dim_team")

    predict(spark, home_id, away_id, args.home, args.away)


if __name__ == "__main__":
    main()

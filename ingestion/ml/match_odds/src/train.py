"""Train + calibrate the match odds model, with a walk-forward backtest report.

Usage:
    python -m ml.match_odds.src.train
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier

from ml.match_odds.src.evaluate import elo_only_baseline_factory, print_report, walk_forward_backtest
from ml.match_odds.src.features import FEATURE_COLUMNS, LABEL_COL
from ml.match_odds.src.synthetic_data import generate_synthetic_seasons

MODEL_DIR = Path(__file__).resolve().parents[1] / "models"
MODEL_PATH = MODEL_DIR / "match_odds_model.joblib"


def gbm_factory() -> GradientBoostingClassifier:
    return GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05, random_state=42,
    )


def calibrated_gbm_factory() -> CalibratedClassifierCV:
    return CalibratedClassifierCV(gbm_factory(), method="isotonic", cv=5)


def run() -> None:
    df = generate_synthetic_seasons()
    print(f"[train] Synthetic dataset: {len(df)} matches, "
          f"{df['season'].nunique()} seasons "
          f"({df['home_team_id'].nunique()} teams)")
    print("[train] NOTE: trained on synthetic data — real fixtures from "
          "silver.fact_match are too few (1 finished match) to train on yet. "
          "See ml/match_odds/src/synthetic_data.py for why and how this is "
          "swapped out once live data accumulates.")

    baseline_folds = walk_forward_backtest(df, elo_only_baseline_factory)
    print_report("ELO-only baseline (logistic regression)", baseline_folds)

    gbm_folds = walk_forward_backtest(df, calibrated_gbm_factory)
    print_report("Calibrated gradient boosting (full feature set)", gbm_folds)

    # Model selection: deploy whichever the backtest actually favors (lower
    # avg log-loss), not whichever is fancier. On this synthetic data the GBM
    # doesn't beat the ELO-only baseline — the extra form features are mostly
    # redundant with elo_diff once ELO has converged, so the higher-capacity
    # model just adds variance. Re-run this comparison once real fixtures
    # replace the synthetic data; it may well flip.
    baseline_avg_ll = sum(f.log_loss for f in baseline_folds) / len(baseline_folds)
    gbm_avg_ll = sum(f.log_loss for f in gbm_folds) / len(gbm_folds)

    if gbm_avg_ll < baseline_avg_ll:
        chosen_name = "calibrated_gbm"
        chosen_factory = calibrated_gbm_factory
        chosen_folds = gbm_folds
    else:
        chosen_name = "elo_only_baseline"
        chosen_factory = elo_only_baseline_factory
        chosen_folds = baseline_folds
    print(f"\n[train] Backtest avg log-loss: baseline={baseline_avg_ll:.4f} "
          f"gbm={gbm_avg_ll:.4f} -> deploying '{chosen_name}'")

    # Final production model: fit on every season (most data, deployed model
    # isn't held back from the most recent season the way backtest folds are).
    final_model = chosen_factory()
    final_model.fit(df[FEATURE_COLUMNS], df[LABEL_COL])

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, MODEL_PATH)

    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_columns": FEATURE_COLUMNS,
        "label_col": LABEL_COL,
        "training_rows": len(df),
        "training_seasons": int(df["season"].nunique()),
        "data_source": "synthetic (ml/match_odds/src/synthetic_data.py)",
        "model_selected": chosen_name,
        "backtest_avg_log_loss": {
            "elo_only_baseline": round(baseline_avg_ll, 4),
            "calibrated_gbm": round(gbm_avg_ll, 4),
        },
        "backtest_avg_accuracy": round(
            sum(f.accuracy for f in chosen_folds) / len(chosen_folds), 4
        ),
    }
    with open(MODEL_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n[train] Saved model -> {MODEL_PATH}")
    print(f"[train] Saved metadata -> {MODEL_DIR / 'metadata.json'}")


if __name__ == "__main__":
    run()

"""Walk-forward backtest harness for the match odds model.

Trains on seasons [0..k), evaluates on season k, for each k in turn — avoids
the leakage a random train/test split would introduce (form/ELO features are
only valid if computed strictly before the match they describe, and a random
split would let the model "see the future" via teams' later-season stats
appearing in training while early ones appear in test).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss

from ml.match_odds.src.features import CLASS_LABELS, FEATURE_COLUMNS, LABEL_COL


@dataclass
class FoldMetrics:
    season: int
    n_train: int
    n_test: int
    log_loss: float
    brier_score: float
    accuracy: float
    home_baseline_accuracy: float


def _multiclass_brier(y_true: np.ndarray, proba: np.ndarray, classes: list[str]) -> float:
    """Mean squared error between predicted probabilities and the one-hot
    actual outcome, averaged over classes (multi-class generalization of
    sklearn's binary-only brier_score_loss)."""
    one_hot = np.array([[1.0 if c == y else 0.0 for c in classes] for y in y_true])
    return float(np.mean(np.sum((proba - one_hot) ** 2, axis=1)))


def walk_forward_backtest(df: pd.DataFrame, model_factory) -> list[FoldMetrics]:
    """model_factory() -> an unfitted sklearn-compatible classifier."""
    seasons = sorted(df["season"].unique())
    folds = []

    for k in seasons[1:]:  # need at least one prior season to train on
        train = df[df["season"] < k]
        test = df[df["season"] == k]
        if train.empty or test.empty:
            continue

        model = model_factory()
        model.fit(train[FEATURE_COLUMNS], train[LABEL_COL])

        proba = model.predict_proba(test[FEATURE_COLUMNS])
        classes = list(model.classes_)
        preds = model.predict(test[FEATURE_COLUMNS])

        y_true = test[LABEL_COL].to_numpy()
        ll = log_loss(y_true, proba, labels=classes)
        brier = _multiclass_brier(y_true, proba, classes)
        acc = accuracy_score(y_true, preds)
        home_baseline_acc = float((y_true == "H").mean())

        folds.append(FoldMetrics(
            season=int(k), n_train=len(train), n_test=len(test),
            log_loss=ll, brier_score=brier, accuracy=acc,
            home_baseline_accuracy=home_baseline_acc,
        ))

    return folds


class EloOnlyModel:
    """Baseline model: logistic regression on elo_diff alone (drops the form
    columns). Cheap, explainable — the design doc's "ELO-based expected score
    -> win/draw/loss probabilities" approach, implemented via a fitted
    logistic model rather than a hand-tuned draw-margin heuristic.

    Defined at module level (not nested in the factory function) so it can be
    pickled by joblib when this is the model train.py selects to deploy.
    """

    def __init__(self):
        self._lr = LogisticRegression(max_iter=1000)

    def fit(self, X, y):
        self._lr.fit(X[["elo_diff"]], y)
        self.classes_ = self._lr.classes_
        return self

    def predict_proba(self, X):
        return self._lr.predict_proba(X[["elo_diff"]])

    def predict(self, X):
        return self._lr.predict(X[["elo_diff"]])


def elo_only_baseline_factory() -> EloOnlyModel:
    return EloOnlyModel()


def print_report(name: str, folds: list[FoldMetrics]) -> None:
    print(f"\n--- {name} ---")
    print(f"{'season':>6} {'n_train':>8} {'n_test':>7} {'log_loss':>9} "
          f"{'brier':>7} {'acc':>6} {'home_base':>10}")
    for f in folds:
        print(f"{f.season:>6} {f.n_train:>8} {f.n_test:>7} {f.log_loss:>9.4f} "
              f"{f.brier_score:>7.4f} {f.accuracy:>6.1%} {f.home_baseline_accuracy:>10.1%}")
    if folds:
        avg_ll = np.mean([f.log_loss for f in folds])
        avg_brier = np.mean([f.brier_score for f in folds])
        avg_acc = np.mean([f.accuracy for f in folds])
        avg_base = np.mean([f.home_baseline_accuracy for f in folds])
        print(f"{'avg':>6} {'':>8} {'':>7} {avg_ll:>9.4f} {avg_brier:>7.4f} "
              f"{avg_acc:>6.1%} {avg_base:>10.1%}")

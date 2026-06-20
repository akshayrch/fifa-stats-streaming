# App 2 — Match Odds Predictor

See [design doc](../../docs/apps/02_match_odds_predictor.md).

## Layout

```
match_odds/
├── src/
│   ├── synthetic_data.py   # generates historical training data (see below)
│   ├── features.py         # feature schema + Gold-layer lookups for predict.py
│   ├── evaluate.py         # walk-forward backtest harness + metrics
│   ├── train.py            # trains baseline + GBM, picks the backtest winner
│   └── predict.py          # CLI: two teams -> win/draw/loss probabilities
├── models/                 # match_odds_model.joblib + metadata.json (gitignored)
└── requirements.txt
```

## Why synthetic training data

The design doc calls for training on historical fixtures with walk-forward
backtesting across seasons. The real pipeline has only run in mock mode so
far, so `silver.fact_match` has exactly **1 finished match** — nowhere near
enough to fit or validate a classifier. `synthetic_data.py` simulates 6
seasons of a 20-team round-robin using the *same* ELO update rule as
`gold_aggregate.build_elo_ratings`, so the feature columns are computed
identically to the real Gold table and the result has genuine learnable
structure (stronger teams win more, ~45% home win rate matching the design
doc's real-world baseline) rather than being pure noise.

This is a bridge, not a permanent fixture: once the live API key is
producing real fixtures, swap `generate_synthetic_seasons()` for a loader
over real historical `gold.match_prediction_features` rows in `train.py` —
everything downstream (feature schema, backtest, model, CLI) is unchanged.

## What the backtest found

`train.py` runs a walk-forward backtest (train on seasons `[0..k)`, evaluate
on season `k`) for two models and **deploys whichever wins on log-loss** —
not whichever is fancier:

| Model | Avg log-loss | Avg Brier | Avg accuracy |
|---|---|---|---|
| ELO-only baseline (logistic regression on `elo_diff`) | **0.898** | 0.527 | 61.1% |
| Calibrated gradient boosting (full feature set, isotonic) | 0.958 | 0.552 | 58.8% |

On this synthetic data, **the simple ELO-only baseline wins**. That's not a
bug — once ELO has converged it's already a strong proxy for the hidden
team-strength that drives results, and the rolling-form features
(`*_ppg_last5`, `*_avg_gf_last5`) are mostly redundant with it, so the
higher-capacity GBM just adds variance without adding signal. Both models
comfortably beat the "always predict home win" baseline (~45% accuracy) on
log-loss/Brier, since they produce calibrated probabilities instead of a
single point prediction.

`train.py` saves whichever model the backtest favors to
`models/match_odds_model.joblib`, with the comparison recorded in
`models/metadata.json`. Re-run this once real fixtures replace the synthetic
data — with head-to-head and squad-strength features added (per the design
doc), the GBM may well overtake the baseline.

## Running it

```bash
export PYTHONPATH=$PWD
pip install -r ml/match_odds/requirements.txt

python -m ml.match_odds.src.train      # generates data, backtests, saves model
python -m ml.match_odds.src.predict --home "Manchester City" --away Arsenal
python -m ml.match_odds.src.predict --home 50 --away 42   # team_id also works
```

`predict.py` looks up each team's latest ELO + rolling form from the real
`gold.elo_ratings` / `gold.team_form_features` tables (falling back to
cold-start defaults for teams with no Gold history yet), assembles the same
feature row the model was trained on, and prints calibrated probabilities
plus a short, rule-based explanation of the main drivers (ELO gap, form
differential, home advantage). SHAP-based per-prediction attribution is
listed as a Phase 7 polish item rather than wired in now, to avoid pulling in
a heavy extra dependency for an MVP CLI.

## Status

✅ Phase 4 complete — baseline + GBM models, walk-forward backtest, model
selection, CLI serving. Verified end-to-end against the mock Gold/Silver
data from Phase 3.

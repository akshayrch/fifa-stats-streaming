# App 2 — Match Odds Predictor

See [design doc](../../docs/apps/02_match_odds_predictor.md).

## Planned layout

```
match_odds/
├── notebooks/
│   └── 01_eda_and_baseline.ipynb     # ELO baseline + feature exploration
├── src/
│   ├── features.py                    # reads gold.match_prediction_features
│   ├── train.py                       # trains + calibrates the GBM model
│   ├── evaluate.py                    # walk-forward backtest, log-loss/Brier
│   └── predict.py                     # CLI: two team IDs -> probabilities
└── models/                            # saved model artifacts (gitignored)
```

## Status

📋 Not started — Phase 4.

# App 2 — Match Odds Predictor

## Problem statement

Given two teams (and a venue/home-away assignment), predict the probability
distribution over `{home win, draw, away win}`.

## Inputs (from Gold layer — `match_prediction_features`)

Per matchup, joined from Gold tables:

- **Form features**: rolling last-5/10 results, goals for/against, points per
  game (overall, home, away splits) for both teams
- **ELO ratings**: incrementally updated after each result (`gold.elo_ratings`)
- **Head-to-head**: historical results between these two teams (last N
  meetings, venue-adjusted)
- **Squad strength proxy**: aggregate of `player_season_stats` for likely
  starters (falls back to season-average squad rating if lineup unknown)
- **Context**: home/away, days since last match (rest), competition stage

## Modeling approach

1. **Baseline**: ELO-based expected score formula -> converted to
   win/draw/loss probabilities via a simple draw-margin heuristic. Cheap,
   explainable, gives a sanity-check floor.
2. **Model v1**: gradient boosting classifier (XGBoost/LightGBM),
   multi-class (`home_win` / `draw` / `away_win`), trained on historical
   fixtures with the Gold features above as inputs and final result as label.
3. **Calibration**: probability calibration (Platt scaling / isotonic) since
   raw GBM probabilities tend to be overconfident — important since the output
   is literally "odds."

## Evaluation

- **Backtesting**: walk-forward validation on historical seasons (train on
  seasons 1..N, evaluate on season N+1) — avoids leakage from future form/ELO.
- **Metrics**: log-loss / Brier score (probability quality) and accuracy
  against a "always predict home win" baseline (home advantage is real —
  ~45% of matches are home wins historically, so the baseline isn't trivial).
- **Sanity check vs. market odds**: if/when `football.odds.raw` is available,
  compare model probabilities to implied bookmaker probabilities — not to
  "beat the market," but to validate the model is in a reasonable range.

## Output

```
Team A (home) vs Team B (away)
  Home win: 48%
  Draw:     27%
  Away win: 25%
Top contributing factors: ELO gap (+), home advantage (+), Team A's
last-5 form (+), head-to-head record (neutral)
```

## Serving (MVP)

CLI / notebook: input = two team IDs (+ optional venue) -> output =
probabilities + top feature contributions (e.g. via SHAP).

## Status

📋 Design only — implementation starts in Phase 4, right after Gold features
exist. This is the foundation for Apps 1 and 3.

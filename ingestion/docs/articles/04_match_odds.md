# Predicting match odds from a real-time feature store

This is the phase where the project's recurring honest constraint first
became impossible to design around: after Phase 3, `silver.fact_match` has
exactly one finished match. Not "not much data" — one row. You cannot fit
or evaluate a classifier on one row, and the design doc for this app
(`docs/apps/02_match_odds_predictor.md`) calls for a walk-forward backtest
across seasons. Phase 4 is where I built the synthetic-data bridge pattern
that ends up repeating in every subsequent app.

## Building a synthetic season generator, not faking a result

`ml/match_odds/src/synthetic_data.py` simulates six seasons of a 20-team
round-robin league. The important constraint I set for myself: it has to
use the *same* ELO update rule as `gold_aggregate.build_elo_ratings`
(K=32, base 1500, +60 ELO home advantage), with goals drawn from a Poisson
process driven by each team's hidden "true strength." That's the
difference between "synthetic data" and "fake data" for me — this isn't a
shortcut around the real Gold logic, it's the same logic, run forward in a
simulation, specifically so a model trained on it doesn't need to change
when real historical Gold data eventually exists. The resulting outcome
distribution — 44.7% home win, 21.8% draw, 33.5% away win — lines up with
the ~45% real-world home-win rate the design doc had already cited as a
target, which was a good sanity check that the simulation wasn't producing
something statistically nonsensical.

## Feature parity and the cold-start path

`ml/match_odds/src/features.py` defines `FEATURE_COLUMNS` — `elo_diff`,
`home_ppg_last5`, `away_ppg_last5`, `home_avg_gf_last5`, `away_avg_gf_last5`
— matching `gold.match_prediction_features` exactly. That parity is the
whole point of the bridge: a model trained on synthetic data can be pointed
at real Gold data with no retraining, because the feature columns are
identical by construction, not by coincidence.

`latest_team_stats()` pulls the latest ELO and rolling form for a real team
straight from `gold.elo_ratings`/`gold.team_form_features` via a
`row_number()` window, and `build_feature_row()` assembles a feature row
for any two team IDs — falling back to documented cold-start defaults (ELO
1500, PPG 1.0, GF 1.2) for teams with no Gold history yet. Given that most
teams in this mock pipeline have no Gold history yet, that fallback path
isn't an edge case, it's close to the default case, and it needed to behave
sensibly rather than just not crash.

## Letting the backtest pick the model, not my intuition

`ml/match_odds/src/evaluate.py` runs a walk-forward backtest: train on
seasons `[0..k)`, evaluate on season `k`, specifically to avoid the leakage
a random train/test split would introduce (a random split would let the
model see later-season team strength while "predicting" an earlier season
in test, which is backwards). It computes log-loss, a hand-rolled
multi-class Brier score — sklearn's `brier_score_loss` is binary-only, so I
wrote the multi-class mean-squared-error-against-one-hot version by hand —
accuracy, and the "always predict home win" baseline accuracy for
comparison.

`train.py` runs that backtest for two candidates: an `EloOnlyModel` (a
thin wrapper around `LogisticRegression(elo_diff)`, implementing the design
doc's ELO-based baseline as an actual fitted model rather than a hand-tuned
heuristic) and a calibrated gradient boosting model
(`GradientBoostingClassifier` wrapped in `CalibratedClassifierCV(method=
"isotonic", cv=5)`, using the full feature set). It then deploys whichever
wins on average log-loss.

Here's the part I didn't expect going in: the ELO-only baseline won.
0.898 average log-loss vs. 0.958 for the gradient boosting model, across
2,280 synthetic matches over 6 seasons. I assumed the fancier model with
more features would win, so I checked whether it was a tuning artifact —
swept `max_depth`, `n_estimators`, `learning_rate`, and `subsample`, then
re-ran on a 10-season dataset. The baseline stayed ahead every time. The
likely explanation: once ELO has converged, it's already a strong proxy
for the hidden team-strength variable the simulation is built around, so
the rolling-form features are largely redundant with it in this synthetic
world — and a higher-capacity model just adds variance without adding
signal. `train.py` reports both models' scores and picks the backtest
winner automatically. That was the actual point of building the harness:
not to assume the more sophisticated model wins, but to let the data say
so or not.

## What shipped, and what it actually predicts

`ml/match_odds/src/predict.py` is the CLI: `--home`/`--away` take either a
team name (substring match) or a numeric `team_id`, loads the deployed
model, pulls real Gold ELO/form where it exists, and prints calibrated
win/draw/loss probabilities plus a rule-based explanation (ELO gap, form
differential, attacking output, home advantage). Running it against
Manchester City (home) vs. Arsenal (away) — the one real finished match in
this pipeline, where City beat Arsenal 2-1 — correctly produced a
home-favored prediction (49% home win) driven by the ELO gap and form
differential that result fed into Gold. Running it against two teams with
no Gold history yet correctly fell back to neutral 50/26/30%-ish odds with
an explicit "no Gold history" note, rather than guessing. SHAP-based
attribution was on the design doc's wishlist but got explicitly deferred
to Phase 7 polish — not because it's not useful, but because adding a heavy
extra dependency to an MVP CLI for a feature the rule-based explanation
already covers adequately wasn't worth it yet.

## What's next

This phase produced the first trained, backtested model in the project,
and the first real instance of the synthetic-data bridge. It's also the
model the next two apps build directly on top of rather than
re-implementing: Phase 5's Squad Optimizer calls into this app's
`get_match_probabilities()` to score lineups, and Phase 6's Tournament
Predictor runs this same model thousands of times inside a Monte Carlo
simulation. Next up: the Squad Optimizer, and what it's like wiring a PuLP
constraint solver into a Spark-backed feature store.

## LinkedIn version

Phase 4: training the first real ML model in this project — and the
backtest immediately humbled my intuition.

The data problem: after 3 phases of pipeline work, the real mock data has
exactly 1 finished match. Not enough to train anything. So I built a
synthetic season generator using the *same* ELO formula as the real Gold
layer (K=32, base 1500), simulating 6 seasons / 20 teams — 44.7% home win
rate, matching the design doc's ~45% real-world target.

Then I ran a walk-forward backtest comparing an ELO-only logistic
regression baseline against a calibrated gradient boosting model on the
full feature set. I expected the GBM to win.

It didn't. ELO-only baseline: 0.898 log-loss. Gradient boosting: 0.958.
Confirmed it wasn't a tuning fluke (swept hyperparameters, reran on more
seasons) — once ELO converges, it's already capturing most of the signal,
and extra features just add variance.

`train.py` now deploys whichever model the backtest actually favors. Small
detail, but it's the difference between demoing a metric and demoing a
discipline.

Full write-up on the synthetic-data bridge and the backtest:
[link to Medium article]

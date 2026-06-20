# App 1 — Squad Optimizer

See [design doc](../../docs/apps/01_squad_optimizer.md).

## Layout

```
squad_optimizer/
├── src/
│   ├── synthetic_squad_data.py  # generates a 23-player squad (see below)
│   ├── contribution.py          # Stage 1: per-position weighted player score
│   ├── optimizer.py             # Stage 2: PuLP integer program -> best XI
│   └── recommend.py             # CLI: opponent + formation -> XI + win prob
└── requirements.txt
```

## Why a synthetic squad

The design doc needs a full squad — 20+ players spanning GK/DEF/MID/FWD,
each with season stats — to optimize a lineup from. The real mock pipeline's
`silver.dim_player` (4 players, from one lineup payload) and
`gold.player_season_stats` (2 players, from one player-stats payload) don't
even overlap, so there's no real team with enough positioned, stat-bearing
players yet. `synthetic_squad_data.py` generates a realistic 23-player squad
(3 GK / 8 DEF / 7 MID / 5 FWD, ~15% flagged unavailable to simulate
injuries/suspensions) for `team_id=50` — Manchester City in the real Phase 3
Gold/Silver data — so the *opponent* side of the win-probability calculation
still uses real ELO/form. Same bridge pattern as `ml/match_odds`: swap this
for a real loader over `gold.player_season_stats` + `silver.dim_player` once
a real squad's worth of data exists.

## Two-stage approach (per the design doc)

1. **`contribution.py`** — per-position weighted combination of
   `avg_rating`, goals/assists-per-appearance, and an availability/fitness
   proxy. This is a deliberately simple scoring function, not a trained
   model — the design doc calls for starting this way ("then iterate toward
   a learned model once enough Gold data exists"), and there's no
   historical lineup-vs-result dataset yet to train one against anyway.
2. **`optimizer.py`** — PuLP integer program: selects exactly 11 players
   matching a formation's GK/DEF/MID/FWD counts from the *available* pool,
   maximizing total contribution score, with an optional total-`cost`
   budget cap. Four formations supported: `4-4-2`, `4-3-3`, `3-5-2`, `5-3-2`.
   Raises a clear error if the squad/budget can't fill the formation.

## Connecting to the win-probability model

`recommend.py` reuses `ml.match_odds.src.predict.get_match_probabilities()`
directly — App 1 calls into App 2 rather than re-implementing match
prediction, per the roadmap's build order. The optimized XI's average
contribution score (vs. a "naive" baseline XI — fill each position with the
first available players in roster order, no optimization) is translated
into an ELO offset for the squad's team
(`ELO_POINTS_PER_CONTRIBUTION_POINT = 15.0`, a documented simplifying
assumption rather than a fitted coefficient — there's no historical
lineup-vs-result data to fit it against), then fed into the same calibrated
model from Phase 4 to get a win/draw/loss probability for both lineups.

## Running it

```bash
export PYTHONPATH=$PWD
pip install -r ml/squad_optimizer/requirements.txt
pip install -r ml/match_odds/requirements.txt   # recommend.py needs the trained odds model
python -m ml.match_odds.src.train               # if models/match_odds_model.joblib doesn't exist yet

python -m ml.squad_optimizer.src.recommend --opponent Arsenal
python -m ml.squad_optimizer.src.recommend --opponent 42 --formation 4-3-3
python -m ml.squad_optimizer.src.recommend --opponent Arsenal --budget 650
```

Example output:

```
Recommended XI (4-4-2) vs opponent team_id=42:
  GK   Kwame Lindgren       rating=6.56  contribution=40.2
  ...

Predicted win probability:
  Optimized XI : Home win 53% | Draw 25% | Away win 23%
  Naive XI     : Home win 49% | Draw 25% | Away win 26%
  Uplift from optimization: +3.7% win probability

Key swap explanations:
  - Pierre Akinola in for Marcus Reyes (MID): +5.2 contribution score
  - Mateo Adeyemi in for Lukas Costa (FWD): +6.5 contribution score
```

## Status

✅ Phase 5 complete — contribution scoring, PuLP lineup optimizer (4
formations, optional budget cap), CLI serving wired into Phase 4's win
probability model. Verified end-to-end against the mock Gold/Silver data
from Phase 3, including the budget-infeasible error path.

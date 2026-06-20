# Optimizing a starting XI with constraint programming + ML

Phase 5 is the Squad Optimizer — given a player pool and formation
constraints, recommend a starting XI that maximizes predicted win
probability. It's the first app in this project that isn't really a
machine learning problem at its core; it's an optimization problem with a
machine-learned objective function bolted on, and getting that combination
right meant leaning on two very different tools: a scoring heuristic and a
PuLP integer program.

## The data problem, again, from a different angle

By now the pattern should sound familiar: after Phase 3,
`silver.dim_player` has 4 players (all from one lineup payload — Onana,
Mazraoui, Becker, Alexander-Arnold) and `gold.player_season_stats` has 2
(from one player-stats payload — Palmer, Son). Worse than just being
sparse, these two sets don't even overlap — there's no single team with
enough positioned, stat-bearing players to build a feasible XI from in the
real data yet. So `ml/squad_optimizer/src/synthetic_squad_data.py`
generates a realistic 23-player squad — 3 GK, 8 DEF, 7 MID, 5 FWD, with
position-appropriate goal/assist/rating distributions and roughly 15%
flagged unavailable to simulate injuries or suspensions — for `team_id=50`,
which is Manchester City in the real Phase 3 Gold/Silver data. That choice
matters: by anchoring the synthetic squad to a real team ID, the
*opponent* side of the win-probability calculation still uses real
ELO/form data from Gold, not synthetic data on both sides. One detail
worth being honest about: availability gets corrected post-generation to
guarantee every formation stays feasible, which is a deliberate
simplification, not an emergent property of the random generation.

## Two stages, on purpose kept separate

The design doc calls for a two-stage approach, and I kept the stages
genuinely separate rather than collapsing them into one scoring function.
Stage 1, `ml/squad_optimizer/src/contribution.py`, computes a per-position
weighted score from average rating, goals-per-appearance,
assists-per-appearance, and an availability/fitness proxy derived from
appearance count. This is deliberately a hand-weighted formula, not a
trained model — the design doc is explicit about starting simple and
iterating toward a learned model only once enough Gold data exists, and
there's no historical lineup-vs-result dataset in this pipeline to train
one against regardless. Pretending otherwise would have meant either
skipping the model or training on data that doesn't exist.

Stage 2, `ml/squad_optimizer/src/optimizer.py`, is where the actual
optimization happens: `select_best_xi()` is a PuLP integer program (CBC
backend) that picks exactly 11 *available* players matching a formation's
GK/DEF/MID/FWD counts, maximizing total contribution score, with an
optional total-cost budget cap. Four formations are supported (`4-4-2`,
`4-3-3`, `3-5-2`, `5-3-2`), and infeasibility raises a clear `ValueError`
rather than silently returning a partial lineup — verified directly by
setting a budget of 50 against a squad that needs roughly 690 in
unconstrained cost, and confirming it correctly reports infeasible instead
of producing nonsense. `naive_xi()` provides the "no optimization"
baseline the design doc calls for — filling each position with the first
available players in roster order — which matters for the next step,
because "optimized XI beats nothing" isn't an interesting claim; "optimized
XI beats the lineup you'd pick without optimizing" is.

## Reusing App 2 instead of reinventing it

`ml/squad_optimizer/src/recommend.py` ties it together, and the most
interesting engineering decision in this phase is what it doesn't do: it
doesn't reimplement match prediction. It calls
`ml.match_odds.src.predict.get_match_probabilities()` directly — App 1
calling into App 2's win-probability function, per the build order the
roadmap specified (Match Odds Predictor before Squad Optimizer, precisely
so this reuse was possible). The optimized XI's average-contribution-score
edge over the naive baseline XI gets converted into an ELO offset via a
documented constant, `ELO_POINTS_PER_CONTRIBUTION_POINT = 15.0` — flagged
explicitly as a simplifying assumption, not a fitted coefficient, the same
way Phase 4's synthetic data was flagged as a bridge rather than ground
truth. That offset gets fed into App 2's model twice — once for the
optimized lineup, once for the naive one — to produce a concrete
win-probability uplift number, plus per-position swap explanations by
diffing the two XIs.

Making that reuse work cleanly required going back into Phase 4's code:
`predict.py`'s CLI logic was refactored to extract
`get_match_probabilities(spark, home_id, away_id, model=None,
team_stats=None, home_elo_offset=0.0, away_elo_offset=0.0)` as a standalone
function — the same calibrated-probability computation minus the printing,
with optional ELO offsets to support "what if this side fielded a
stronger or weaker lineup" scenarios. I re-ran the Phase 4 CLI after the
refactor and confirmed it produced byte-for-byte identical output, which
is the kind of unglamorous regression check that's easy to skip and
exactly the one you need after touching code three other things depend on.

## What it actually produces

Running `recommend.py --opponent Arsenal` for a 4-4-2 produces a full XI
with per-player rating and contribution score, then a probability
comparison: the optimized XI at 53% home win / 25% draw / 23% away win
against a naive XI at 49% / 25% / 26% — a +3.7% win-probability uplift —
plus swap explanations like "Erik Kovac in for Cole Tanaka (FWD): +4.4
contribution score." Other formations produce valid XIs with correct
positional counts, and the budget cap behaves the way you'd want: a
budget at 95% of the unconstrained cost dropped total contribution score
from 399.2 to 388.9 rather than failing outright, trading score for cost
gracefully until the budget gets tight enough to be genuinely infeasible.

## What's next

This is the second app built on Gold, and the first one that calls another
app's code directly rather than just reading the same tables — a pattern
that pays off again in the next phase, where the Tournament Predictor runs
App 2's model thousands of times inside a Monte Carlo simulation instead
of training anything of its own.

## LinkedIn version

Phase 5: the Squad Optimizer — pick the best starting XI under formation
and budget constraints, scored by predicted win-probability uplift.

This one isn't really an ML problem, it's an optimization problem with a
learned objective function: a hand-weighted contribution score (rating,
goals/assists-per-appearance, availability) feeds a PuLP integer program
(CBC backend) that picks exactly 11 available players matching a
formation's position counts, under an optional budget cap.

The interesting decision: it doesn't reimplement match prediction. It calls
directly into Phase 4's `get_match_probabilities()` to score the optimized
XI against a naive baseline lineup — App 1 reusing App 2, not duplicating
it. That required refactoring Phase 4's CLI logic into a reusable function
with optional ELO offsets, then re-verifying the original CLI still
produced identical output.

Result on a sample run: optimized XI lifts win probability from 49% to 53%
against a naive lineup, with per-position swap explanations showing exactly
which player and how much score each swap added.

Full write-up on the two-stage design and the budget-constraint behavior:
[link to Medium article]

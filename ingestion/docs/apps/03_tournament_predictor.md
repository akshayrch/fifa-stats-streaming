# App 3 — Live Tournament Predictor

## Problem statement

For an in-progress tournament (group stage + knockout, e.g. a World Cup /
continental championship), continuously estimate:

- Each team's probability of advancing from its group
- Each team's probability of reaching each knockout round / winning the
  tournament
- How those probabilities shift in (near) real time as live match events
  arrive

## Inputs

- **Tournament structure** (static reference data): groups, fixture schedule,
  knockout bracket rules — modeled as a small reference dataset
  (`gold.tournament_structure`)
- **Match outcome probabilities**: from App 2's odds model, applied to every
  *remaining* fixture in the tournament
- **Live state**: current group standings / bracket state, updated as
  `football.events.live` messages arrive (goal -> score change -> standings
  recompute)

## Approach: Monte Carlo simulation

1. For all remaining fixtures, get `{home_win, draw, away_win}` probabilities
   from App 2.
2. Run N simulations (e.g. 10,000) of the rest of the tournament:
   - Sample each remaining fixture's result from its probability distribution
   - Apply tournament rules (points, goal difference, head-to-head tiebreaks,
     bracket progression) to get a final outcome per simulation
3. Aggregate across simulations -> probability of each team reaching each
   stage / winning it all.

## Real-time triggering

- A lightweight consumer subscribes to `football.events.live`.
- On a **goal** event (score change) for a tracked tournament match:
  - Update the live match state (current score)
  - If the match has just **finished** (status -> `FT`), update group
    standings / bracket state permanently and re-run the full simulation
  - For an **in-progress** score change, optionally re-run a "conditional"
    simulation (current match's remaining-time outcome distribution shifts
    based on current score) — this is a stretch enhancement; MVP re-simulates
    on full-time results only.
- Re-simulation results are written to `gold.tournament_state` (or a small
  Postgres table for low-latency reads) and timestamped, so the "live"
  dashboard always reflects the latest completed-match state.

## Output

```
Group A standings + qualification probabilities (live, updated 14:32 UTC)
  Team 1: Pld 2, Pts 4  -> Qualify: 78%  | Win group: 52%
  Team 2: Pld 2, Pts 4  -> Qualify: 75%  | Win group: 41%
  Team 3: Pld 2, Pts 1  -> Qualify: 31%  | Win group: 5%
  Team 4: Pld 2, Pts 1  -> Qualify: 16%  | Win group: 2%

Tournament winner probabilities (top 5)
  Team X: 14.2%  Team Y: 11.8%  ...
```

## Evaluation

- Compare simulated qualification probabilities at various tournament stages
  against actual outcomes (calibration over many historical tournaments, if
  data available) — or, for a single live tournament, simply track how
  probabilities evolve and sanity-check against intuition/public odds.

## Serving (MVP)

A polling script/notebook that re-reads `gold.tournament_state` every time it
changes and prints the table above. Streamlit live dashboard is a stretch
goal.

## Status

📋 Design only — implementation starts in Phase 6, after Apps 1 and 2 exist
and the live-events stream (Phase 1-3) is reliable. Best demoed during an
actual live tournament window.

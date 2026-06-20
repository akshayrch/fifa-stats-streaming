# App 3 — Live Tournament Predictor

See [design doc](../../docs/apps/03_tournament_predictor.md).

## Planned layout

```
tournament_predictor/
├── notebooks/
│   └── 01_simulation_prototype.ipynb
├── src/
│   ├── structure.py        # tournament structure (groups, bracket rules) as data
│   ├── simulate.py          # Monte Carlo simulation using ml/match_odds
│   ├── live_consumer.py     # consumes football.events.live, triggers re-simulation
│   └── state.py             # reads/writes gold.tournament_state
└── data/
    └── tournament_structure.example.json
```

## Status

📋 Not started — Phase 6 (depends on `ml/match_odds` and a reliable
`football.events.live` stream).

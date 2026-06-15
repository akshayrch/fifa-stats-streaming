# App 1 — Squad Optimizer

See [design doc](../../docs/apps/01_squad_optimizer.md).

## Planned layout

```
squad_optimizer/
├── notebooks/
│   └── 01_player_contribution_eda.ipynb
├── src/
│   ├── features.py        # reads gold.player_season_stats, team_form_features
│   ├── contribution.py    # per-player win-probability contribution model
│   ├── optimizer.py        # PuLP/OR-Tools lineup selection given constraints
│   └── recommend.py        # CLI: fixture_id + squad -> recommended XI
└── models/                  # saved model artifacts (gitignored)
```

## Status

📋 Not started — Phase 5 (depends on `ml/match_odds`).

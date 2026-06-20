# ML

Feature engineering, models, and evaluation for the 3 apps. Each app has its
own subfolder with a `README.md` (linking to the design doc in
[`docs/apps/`](../docs/apps/)), `notebooks/` for exploration, and `src/` for
productionized code once a model is finalized.

| Folder | App | Design doc |
|---|---|---|
| [`squad_optimizer/`](squad_optimizer) | App 1 | [docs/apps/01_squad_optimizer.md](../docs/apps/01_squad_optimizer.md) |
| [`match_odds/`](match_odds) | App 2 | [docs/apps/02_match_odds_predictor.md](../docs/apps/02_match_odds_predictor.md) |
| [`tournament_predictor/`](tournament_predictor) | App 3 | [docs/apps/03_tournament_predictor.md](../docs/apps/03_tournament_predictor.md) |

## Build order

`match_odds` first (Phase 4) — it produces the win-probability function reused
by both `squad_optimizer` (Phase 5) and `tournament_predictor` (Phase 6).

## Status

📋 Design docs only. All Gold tables (`medallion/README.md`) need to exist
with real data before model work starts.

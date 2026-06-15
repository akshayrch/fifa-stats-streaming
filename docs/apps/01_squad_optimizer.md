# App 1 — Squad Optimizer

## Problem statement

Given an upcoming fixture, a pool of available/eligible players (your squad),
and constraints (formation, max players per position, fitness/availability),
recommend a starting XI that maximizes predicted win probability.

## Inputs (from Gold layer)

- `player_season_stats` — per-player rolling stats (goals, assists, rating,
  minutes played, pass accuracy, defensive actions, position)
- `team_form_features` — recent team form (last-N results, goals for/against,
  home/away splits)
- `head_to_head_features` — historical results vs. the specific opponent
- Player availability/fitness (from lineups history — has the player started
  recently? injury proxy = sudden drop in minutes)

## Approach

Two-stage:

1. **Player contribution model**: a regression/ranking model that estimates
   each player's marginal contribution to win probability in this specific
   matchup context (opponent strength, home/away, recent form). Start simple
   — a per-position rating score (weighted combination of `player_season_stats`
   features) — then iterate toward a learned model once enough Gold data
   exists.
2. **Lineup optimization**: a constraint solver (PuLP or OR-Tools, CP-SAT)
   that selects 11 players + formation to maximize summed contribution scores
   subject to:
   - Exactly 1 GK, formation-valid counts of DEF/MID/FWD
   - Player availability (no injured/suspended players)
   - Optional: budget cap if simulating fantasy-football-style constraints

## Output

- Recommended XI + formation
- Predicted win probability (vs. a baseline lineup, e.g. "last match's XI")
- Key swap explanations ("Player X in for Player Y: +2.3% win probability —
  better recent form vs. left-footed wingers")

## Evaluation

- Backtest: for historical fixtures, compare the optimizer's suggested XI's
  *predicted* win probability against the *actual* XI used and the *actual*
  result. Can't "prove" the optimizer picks better lineups without
  experimentation, but can show it's directionally consistent (stronger
  lineups -> higher predicted probability -> better historical results).

## Serving (MVP)

CLI / notebook: input = fixture_id + squad list -> output = recommended XI +
probability. A small Streamlit UI is a stretch goal (Phase 7).

## Status

📋 Design only — implementation starts in Phase 5 (after the odds model in
App 2 exists, since this app reuses its win-probability function).

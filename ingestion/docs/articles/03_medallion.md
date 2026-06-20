# Designing a Medallion architecture for football stats: Bronze, Silver, Gold

Phase 3 is where the Medallion architecture stopped being three boxes in a
mermaid diagram and became two real Spark jobs:
`streaming/jobs/silver_transform.py` and `streaming/jobs/gold_aggregate.py`.
This is the longest and, I think, most consequential phase so far — it's
the layer every one of the three apps eventually reads from, so getting the
table contracts and the transformation logic right here saves a lot of
rework later.

## Silver: parsing, conforming, and the foreachBatch pattern

Bronze gave me five tables of unparsed JSON strings. Silver's job is to
turn that into conformed dimension and fact tables, and the implementation
detail that mattered most was the streaming pattern: one streaming query
per Bronze source, each with its own checkpoint
(`_checkpoints/silver_<source>`), using `foreachBatch` plus a Delta MERGE
upsert rather than a plain append. That gets you two things at once —
idempotent, safe-to-re-run writes (the MERGE condition is each table's
natural key), and the ability to write *multiple* Silver tables from a
single Bronze source in one batch. The fixtures source, for example, writes
`fact_match`, `dim_team`, and `dim_league` all from the same
`foreachBatch` call, because a single fixture payload genuinely contains
all three.

The other piece of groundwork was writing an explicit `StructType` schema
for every payload shape, because the payloads aren't uniform: fixtures and
standings arrive as one item per message, while events, player-stats, and
lineups arrive as arrays — an array of event dicts for a fixture, an array
of team-player blocks, an array of lineup items. Getting that schema
mapping right meant going back to what each Phase 1 producer actually
publishes, not what I assumed it published.

| Bronze source | Silver table(s) |
|---|---|
| `fixtures_raw` | `fact_match`, `dim_team`, `dim_league` |
| `standings_raw` | `fact_standings_snapshot` |
| `events_raw` | `fact_match_event` |
| `player_stats_raw` | `fact_player_match_stat` |
| `lineups_raw` | `dim_player` |

One small but real design decision: `fact_match_event` doesn't have a
natural event ID from the API, so I synthesized one —
`event_id = md5(fixture_id|minute|player_id|type|detail)` — which gives the
MERGE upsert something stable to key on without inventing a sequence table.

## Gold: where ELO, form, and head-to-head actually get computed

`gold_aggregate.py` is a batch job (idempotent overwrite, not a streaming
query) that computes five feature tables directly off Silver:

- `team_form_features` — unpivots matches into home+away rows, then uses
  `rowsBetween(-4, 0)` and `rowsBetween(-9, 0)` windows to get rolling
  last-5 and last-10 points-per-game, goals-for, and goals-against.
- `elo_ratings` — chronological ELO updates computed driver-side, K=32,
  base rating 1500, storing `elo_before`/`elo_after` per team per match.
- `head_to_head_features` — canonicalizes the team pair with `least()`/
  `greatest()` so "Arsenal vs Man City" and "Man City vs Arsenal" aggregate
  into the same row, then rolls up wins/draws.
- `player_season_stats` — joins back to `fact_match` to attach a season,
  groups by `(player, season)`.
- `match_prediction_features` — the table the Match Odds Predictor will
  actually read: joins upcoming fixtures with each team's latest ELO and
  last-5 form.

The ELO formula computed here matters beyond this phase — it's the exact
same K=32/base-1500 update rule the synthetic data generators in Phases 4
and 6 are built to match, specifically so a model trained on synthetic data
doesn't have to be retrained when real Gold data eventually has enough rows.

## Closing the loop with data quality

The other piece of this phase was `streaming/jobs/data_quality.py` — 52
checks spanning table existence, row counts, null rates on key columns, and
freshness (ingest_ts age against a threshold) across Bronze, Silver, and
Gold. Running the whole chain end to end —

```
Kafka (5 topics) → bronze_ingest.py → Bronze (5 tables)
                 → silver_transform.py → Silver (7 tables)
                 → gold_aggregate.py  → Gold (5 tables)
                 → data_quality.py    → 52/52 PASS, 0 FAIL
```

— against the mock data gave Silver row counts like 2 in `fact_match`
(fixture 1001 not-yet-started, fixture 1002 Man City 2-1 Arsenal,
finished), 4 in `dim_team` (Man United, Liverpool, Man City, Arsenal), and
Gold row counts like `elo_ratings` showing Man City moving 1500 → 1516 and
Arsenal moving 1500 → 1484 off that single result. All 52 quality checks
passed on this data. These are small numbers, deliberately — they're proof
the mechanics work, not proof of statistical significance, and that
distinction is the whole reason Phase 4 needed a synthetic-data bridge
rather than trying to train anything on 2 rows of `fact_match`.

## What surprised me here

The honest friction in this phase was less about Spark and more about the
real-world messiness of the mock API-Football payloads — nested optional
fields, player-stats shapes that weren't perfectly consistent message to
message. None of that is glamorous to write about, but it's exactly the
kind of thing the Medallion split is supposed to absorb: every quirk gets
fixed once, in `silver_transform.py`, instead of every downstream
consumer having to know about it. That payoff didn't show up dramatically
in Phase 3 itself — it showed up later, when three very different apps
(a classifier, a constraint solver, a Monte Carlo simulator) all read from
the same Gold tables without any of them needing their own JSON-parsing
logic. Phase 3 is where you pay that cost; later phases are where you
collect on it.

## What's next

Gold now has a real `match_prediction_features` table with ELO diff and
form differential for at least one upcoming fixture. The next phase is the
first ML app — Match Odds Predictor — and the first time this project has
to confront, head-on, that the real mock pipeline doesn't produce nearly
enough rows to train anything.

## LinkedIn version

Phase 3: turning raw Kafka JSON into a real Medallion lakehouse — Bronze,
Silver, Gold.

Silver's job: parse, conform, dedupe. One streaming query per Bronze
source, Delta MERGE upserts via `foreachBatch`, explicit schemas per
payload shape (fixtures, events, lineups, standings, player-stats all look
different on the wire).

Gold's job: turn conformed facts into features. Rolling last-5/10 form via
window functions, chronological ELO ratings (K=32, base 1500), canonical
head-to-head pairing, and the `match_prediction_features` table the first
ML app will read from.

Closed the loop with `data_quality.py` — 52 checks across all three layers
(row counts, null rates, freshness). 52/52 passed end to end on mock data.

The real payoff of the Bronze/Silver/Gold split wasn't visible yet in this
phase — it showed up later, when three very different apps all read from
the same Gold tables without re-implementing any JSON parsing.

Full write-up on the table contracts and the ELO/form computation:
[link to Medium article]

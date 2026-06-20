# Progress Log

A detailed record of what's been built so far, how it was verified, and how
to run it. Pairs with [`roadmap.md`](roadmap.md) (phase checklist) and
[`architecture.md`](architecture.md) (design rationale).

---

## Phase 0 — Repo scaffold ✅

Established the project skeleton and design docs before writing any code:

- **README + architecture docs** — overall system diagram (mermaid),
  layer-by-layer explanation of Kafka -> Spark -> Delta (Bronze/Silver/Gold)
  -> 3 ML apps, and the rationale for choosing **Spark Structured Streaming
  over Flink** (see [`architecture.md`](architecture.md)).
- **Medallion table contracts** ([`medallion/README.md`](../medallion/README.md))
  — target schemas for `silver.dim_team`, `silver.dim_player`,
  `silver.fact_match`, `silver.fact_player_match_stat`, and the Gold feature
  tables (`gold.team_form_features`, `gold.elo_ratings`,
  `gold.head_to_head_features`, `gold.player_season_stats`,
  `gold.match_prediction_features`) — design only, not yet implemented.
- **Kafka topic design** ([`ingestion/kafka/topics.md`](../ingestion/kafka/topics.md),
  [`topics.yaml`](../ingestion/kafka/topics.yaml)) — 5 topics, partitioned and
  retained per the access pattern (see Phase 1 below).
- **App specs** for the 3 ML apps under [`docs/apps/`](apps/).

---

## Phase 1 — Ingestion: Kafka producer/consumer ✅

### What was built

- **`ingestion/producers/api_football_client.py`** — REST client for
  API-Football (RapidAPI) with:
  - Retry/backoff via `tenacity`
  - Rate-limit-aware request pacing
  - A **`mock: true` mode** that reads canned JSON responses from
    `ingestion/sample_data/` instead of calling the live API — lets the whole
    pipeline be built and tested without spending the free tier's 100
    requests/day before going live.
- **`ingestion/producers/kafka_producer.py`** — shared producer config
  (idempotent: `enable.idempotence=true`, `acks=all`, `compression.type=zstd`)
  and a `publish()` helper that wraps every payload in a common envelope:
  ```json
  {
    "source": "api-football",
    "endpoint": "...",
    "request_id": "...",
    "ingest_ts": "...",
    "payload": { ... raw API-Football JSON ... }
  }
  ```
- **Four producers**, each polling on a configurable interval and publishing
  to its topic:

  | Producer | Topic | Partition key | Notes |
  |---|---|---|---|
  | `fixtures_producer.py` | `football.fixtures.raw` | `league_id` | Fixtures for tracked leagues |
  | `standings_producer.py` | `football.standings.raw` | `league_id` | League standings snapshots |
  | `live_events_producer.py` | `football.events.live` + `football.player_stats.raw` | `fixture_id` | Live match events + per-player stats for in-play fixtures |
  | `lineups_producer.py` | `football.lineups.raw` | `fixture_id` | Only publishes for fixtures kicking off within `lineups_lookahead_minutes` (lookahead-window logic) |

- **`ingestion/consumers/sanity_check_consumer.py`** — dev tool that
  subscribes to all 5 topics and prints
  `[topic] key=... ingest_ts=... endpoint=... payload_size=...` per message,
  to verify partitioning keys and envelope shape end to end.

### Topic design

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `football.fixtures.raw` | 3 | 7 days | `league_id` |
| `football.events.live` | 6 | 7 days | `fixture_id` |
| `football.lineups.raw` | 3 | 7 days | `fixture_id` |
| `football.standings.raw` | 3 | 30 days | `league_id` |
| `football.player_stats.raw` | 6 | 7 days | `fixture_id` |

Higher partition counts on the two high-volume, fixture-keyed topics
(`events.live`, `player_stats.raw`) so concurrent live matches spread across
partitions; `standings.raw` gets a longer retention since it's a low-volume
daily snapshot that's useful to retain for history/replay.

### Local dev environment (no Docker required)

Docker Hub's anonymous pull rate limit blocked `docker compose up` for the
Kafka image during development, so an alternative path was built:

- **`infra/run_local_kafka.sh`** — downloads Apache Kafka 3.8.1 directly from
  `archive.apache.org` to `/opt/kafka` and runs a single broker in **KRaft
  mode** (no Zookeeper) on `localhost:9092`.
- **`ingestion/kafka/create_topics.sh --no-docker`** — creates the 5 topics
  from `topics.yaml` using `kafka-topics.sh` in `/opt/kafka/bin` directly,
  instead of `docker exec`.

The Docker Compose path (`infra/docker-compose.yml`: Kafka + Kafka UI +
MinIO) remains available for environments where Docker Hub pulls aren't
rate-limited.

### Verification (Phase 1)

Ran end-to-end in **mock mode** against the local KRaft broker:

```bash
python3 -m ingestion.producers.fixtures_producer --once
python3 -m ingestion.producers.standings_producer --once
python3 -m ingestion.producers.live_events_producer --once
python3 -m ingestion.producers.lineups_producer --once
python3 -m ingestion.consumers.sanity_check_consumer --max-messages 10 --timeout 15
```

Confirmed:
- Each producer publishes to its correct topic with the correct envelope.
- Partition keys (`league_id` for fixtures/standings, `fixture_id` for
  events/lineups/player-stats) are set correctly — verified via
  `kafka-get-offsets.sh` and the sanity consumer's printed keys.
- `lineups_producer` correctly **skips** publishing when no tracked fixture
  is within the lookahead window (verified the date-filter logic by
  temporarily widening `lineups_lookahead_minutes`, which then produced 1
  message as expected).

**Outstanding for Phase 1**: running the producers against the **live**
API-Football endpoint (needs a RapidAPI key — currently mock-only).

---

## Phase 2 — Stream processing + Medallion (Bronze) ✅

### What was built

- **`streaming/jobs/spark_session.py`** — shared `SparkSession` builder,
  configured via environment variables:
  - `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
  - `LAKEHOUSE_BASE_PATH` — **new in this phase**:
    - Defaults to `file:///tmp/fifa-lakehouse` (local filesystem — no
      Docker/MinIO needed for local dev)
    - If set to an `s3a://...` URI, the builder additionally pulls in
      `hadoop-aws` and configures the S3A connector against MinIO
      (`MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`)
  - Always configures the Delta Lake catalog/extensions
    (`io.delta.sql.DeltaSparkSessionExtension`, `DeltaCatalog`) and pulls
    `spark-sql-kafka-0-10` + `delta-spark` via `spark.jars.packages`.
- **`streaming/jobs/bronze_ingest.py`** — Spark Structured Streaming job that:
  - Subscribes to all 5 `football.*` Kafka topics
    (`TOPIC_TABLE_MAP`)
  - For each topic, reads `key`, `value` (both cast to string), `topic`,
    `partition`, `offset`, `timestamp` (as `kafka_ts`), and stamps
    `ingest_ts = current_timestamp()`
  - Appends, unparsed, to the corresponding `bronze.<table>` Delta table —
    Bronze is the **replayable raw source of truth**, no parsing/dedup yet
  - Supports two run modes:
    - **One-shot** (default): `trigger(availableNow=True)` — processes
      everything currently on the topics, then exits (good for
      cron/Airflow-scheduled batches)
    - **`--continuous`**: runs forever, picking up new messages as they
      arrive
  - Each topic gets its own checkpoint directory
    (`_checkpoints/bronze_<table>`) so each stream tracks its own Kafka
    offsets independently.

### Bronze table map

| Kafka topic | Bronze Delta table |
|---|---|
| `football.fixtures.raw` | `bronze/fixtures_raw` |
| `football.events.live` | `bronze/events_raw` |
| `football.lineups.raw` | `bronze/lineups_raw` |
| `football.standings.raw` | `bronze/standings_raw` |
| `football.player_stats.raw` | `bronze/player_stats_raw` |

Every Bronze row has the shape:

```text
key, value, topic, partition, offset, kafka_ts, ingest_ts
```

where `value` is the raw envelope JSON string (`source`, `endpoint`,
`request_id`, `ingest_ts`, `payload`) — unparsed, exactly as published by the
Phase 1 producers.

### Local dev setup used for verification

- Spark/Delta deps installed into a dedicated venv (`/opt/spark-venv`):
  `pyspark==3.5.3`, `delta-spark==3.2.1` (required pinning
  `setuptools<70` to avoid a pyspark 3.5.3 packaging issue with newer
  setuptools).
- `LAKEHOUSE_BASE_PATH=file:///tmp/fifa-lakehouse` (local filesystem — the
  default, no env vars needed).
- Kafka topics re-populated via the Phase 1 producers in mock mode.

### Verification (Phase 2)

```bash
export PYTHONPATH=$PWD
python streaming/jobs/bronze_ingest.py   # one-shot, trigger(availableNow=True)
```

This downloaded and cached the Kafka/Delta connector jars (~63 MB, via Maven
coordinates in `spark.jars.packages`) and wrote all 5 Bronze Delta tables
under `/tmp/fifa-lakehouse/bronze/`.

Read back each table with `spark.read.format("delta").load(...)` and
confirmed:

| Bronze table | Rows | Verified |
|---|---|---|
| `fixtures_raw` | 2 | key = `league_id`, correct topic/partition/offset, envelope has `source`/`endpoint`/`request_id`/`ingest_ts`/`payload` |
| `events_raw` | 1 | key = `fixture_id`, same envelope shape |
| `lineups_raw` | 1 | key = `fixture_id` |
| `standings_raw` | 1 | key = `league_id` |
| `player_stats_raw` | 1 | key = `fixture_id` |

All tables have correctly populated `_delta_log` transaction logs and
per-table checkpoint directories under `_checkpoints/bronze_*`, confirming
Structured Streaming's exactly-once write semantics are wired up correctly
for replay/resume.

**Outstanding for Phase 2** (deferred, will be picked up alongside Phase 3):
- Explicit schema-on-read validation of the `value` JSON (currently stored
  raw/unparsed by design — Bronze stays schema-less)
- A kill-and-resume replay test on a long-running `--continuous` stream

---

## Phase 3 — Silver + Gold ✅

### What was built

- **`streaming/jobs/silver_transform.py`** — Spark Structured Streaming job
  that processes all 5 Bronze Delta tables into conformed Silver tables via
  `foreachBatch` + Delta MERGE (upsert):

  **Key design decisions**:
  - One streaming query per Bronze source, each with its own checkpoint
    (`_checkpoints/silver_<source>`) — independent progress tracking
  - Multiple Silver tables written per batch (e.g., fixtures source writes
    `fact_match`, `dim_team`, and `dim_league` in a single foreachBatch call)
  - Explicit `StructType` schemas for every payload shape (reflecting what each
    Phase 1 producer actually publishes — single fixture items, arrays of events,
    arrays of team-player stats, etc.)
  - MERGE condition per table natural key — idempotent, safe to re-run

  **Payload structures** (one Kafka message per item, not per full API response):

  | Bronze source | What `payload` contains |
  |---|---|
  | `fixtures_raw` | One fixture item `{fixture, league, teams, goals}` per message |
  | `standings_raw` | One response item `{league: {id, standings: [[...]]}}` per league |
  | `events_raw` | Array of all event dicts for a fixture |
  | `player_stats_raw` | Array of team-player blocks for a fixture |
  | `lineups_raw` | Array of lineup items for a fixture |

  **Bronze → Silver mapping**:

  | Bronze | Silver table(s) |
  |---|---|
  | `fixtures_raw` | `fact_match`, `dim_team`, `dim_league` |
  | `standings_raw` | `fact_standings_snapshot` |
  | `events_raw` | `fact_match_event` (synthetic `event_id = md5(fixture_id|minute|player_id|type|detail)`) |
  | `player_stats_raw` | `fact_player_match_stat` |
  | `lineups_raw` | `dim_player` |

- **`streaming/jobs/gold_aggregate.py`** — batch job (idempotent overwrite)
  computing 5 Gold feature tables:

  | Gold table | Logic |
  |---|---|
  | `team_form_features` | Unpivot to home+away rows, window `rowsBetween(-4,0)` / `(-9,0)` for PPG/GF/GA |
  | `elo_ratings` | K=32, base=1500, chronological on driver; stores `elo_before`/`elo_after` per team per match |
  | `head_to_head_features` | Canonical pair ordering via `least()`/`greatest()`, aggregate wins/draws |
  | `player_season_stats` | Join with `fact_match` for season, group by (player, season) |
  | `match_prediction_features` | Upcoming fixtures + latest ELO + last-5 form joined in |

- **`streaming/jobs/data_quality.py`** — 52 quality checks across all layers.

### Verification (Phase 3)

```
Kafka (5 topics) → bronze_ingest.py → Bronze (5 tables)
                 → silver_transform.py → Silver (7 tables)
                 → gold_aggregate.py  → Gold (5 tables)
                 → data_quality.py    → 52/52 PASS, 0 FAIL
```

Silver results after parsing mock Bronze data:

| Silver table | Rows | Notes |
|---|---|---|
| `fact_match` | 2 | fixtures 1001 (NS) + 1002 (FT, Man City 2-1 Arsenal) |
| `fact_match_event` | 2 | Palmer goal + Son penalty for fixture 1003 |
| `fact_player_match_stat` | 2 | Palmer + Son stats (rating, goals, shots, passes) |
| `fact_standings_snapshot` | 4 | Top 4 PL teams |
| `dim_team` | 4 | Man United, Liverpool, Man City, Arsenal |
| `dim_league` | 1 | Premier League 2025 |
| `dim_player` | 4 | Players from lineup (Onana, Mazraoui, Becker, Alexander-Arnold) |

Gold results:

| Gold table | Rows | Notes |
|---|---|---|
| `team_form_features` | 2 | Man City + Arsenal rolling form |
| `elo_ratings` | 2 | Man City 1500→1516, Arsenal 1500→1484 |
| `head_to_head_features` | 1 | Man City vs Arsenal H2H |
| `player_season_stats` | 2 | Palmer + Son season totals |
| `match_prediction_features` | 1 | Fixture 1001 (Man Utd vs Liverpool) with ELO diff + form |

---

## Phase 4 — App 2: Match Odds Predictor ✅

### The data problem

The design doc ([`docs/apps/02_match_odds_predictor.md`](apps/02_match_odds_predictor.md))
calls for training a classifier on historical fixtures with walk-forward
backtesting across seasons. After Phase 3, `silver.fact_match` has exactly
**1 finished match** (mock data) — nowhere near enough to fit or evaluate a
model. Rather than block Phase 4 on a live API key, built
**`ml/match_odds/src/synthetic_data.py`**: simulates 6 seasons of a 20-team
round-robin league using the *same* ELO update rule as
`gold_aggregate.build_elo_ratings` (K=32, base=1500, +60 ELO home advantage),
with goals drawn from a Poisson process driven by each team's hidden "true
strength." This produces a dataset with genuine learnable structure (better
teams win more) and a realistic outcome distribution — **44.7% home win,
21.8% draw, 33.5% away win** — matching the design doc's cited ~45%
real-world home-win rate. This is a deliberate bridge: swapping in real
historical Gold data later is a one-line change in `train.py`.

### What was built

- **`ml/match_odds/src/features.py`** — `FEATURE_COLUMNS` (`elo_diff`,
  `home_ppg_last5`, `away_ppg_last5`, `home_avg_gf_last5`,
  `away_avg_gf_last5`) matching `gold.match_prediction_features` exactly, so
  a model trained on synthetic data drops in against real Gold data with no
  retraining. Also: `latest_team_stats()` (pulls latest ELO + rolling form
  per team from the real `gold.elo_ratings` / `gold.team_form_features`
  tables via a `row_number()` window), `build_feature_row()` (assembles a
  feature row for any two team IDs, falling back to cold-start defaults —
  ELO 1500, PPG 1.0, GF 1.2 — for teams with no Gold history), and
  `resolve_team_id()` (case-insensitive name lookup against `silver.dim_team`,
  or accepts a raw numeric ID).
- **`ml/match_odds/src/evaluate.py`** — walk-forward backtest harness:
  for each season boundary `k`, trains on seasons `[0..k)` and evaluates on
  season `k` (avoids leakage — a random split would let the model see
  later-season team strength in training while predicting earlier seasons in
  test). Computes log-loss, a hand-rolled multi-class Brier score (mean
  squared error between predicted probabilities and one-hot actual outcome,
  averaged over classes — sklearn's `brier_score_loss` is binary-only),
  accuracy, and the "always predict home win" baseline accuracy per fold.
  Also defines `EloOnlyModel` — a thin wrapper around
  `LogisticRegression(elo_diff)` implementing the design doc's "ELO-based
  expected score -> win/draw/loss probabilities" baseline as a fitted model
  rather than a hand-tuned draw-margin heuristic.
- **`ml/match_odds/src/train.py`** — generates the synthetic dataset, runs
  the backtest for both the ELO-only baseline and a calibrated gradient
  boosting model (`GradientBoostingClassifier` wrapped in
  `CalibratedClassifierCV(method="isotonic", cv=5)`), and **deploys whichever
  wins on average log-loss** — not whichever is fancier. Saves the chosen
  model to `models/match_odds_model.joblib` (joblib) and a `metadata.json`
  with training provenance (synthetic data source, row/season counts,
  both models' backtest scores, which one was selected).
- **`ml/match_odds/src/predict.py`** — CLI: `--home`/`--away` accept either a
  team name (substring match) or a numeric `team_id`. Loads the saved model,
  looks up real Gold ELO/form for both teams, assembles the feature row,
  calls `predict_proba`, and prints calibrated win/draw/loss probabilities
  plus a short rule-based explanation (ELO gap, form differential, attacking
  output, home advantage) — SHAP-based attribution noted as a Phase 7 polish
  item rather than added now, to avoid a heavy extra dependency on an MVP CLI.

### What the backtest found

| Model | Avg log-loss | Avg Brier | Avg accuracy | vs. home-baseline accuracy |
|---|---|---|---|---|
| ELO-only baseline (logistic regression) | **0.898** | 0.527 | 61.1% | 44.8% |
| Calibrated gradient boosting (full feature set) | 0.958 | 0.552 | 58.8% | 44.8% |

The simpler ELO-only baseline won on this synthetic data — confirmed it
wasn't a tuning artifact by sweeping `max_depth`/`n_estimators`/`learning_rate`/
`subsample` and re-running on a 10-season dataset, with the baseline still
ahead each time. The likely reason: once ELO has converged it's already a
strong proxy for hidden team strength, and the rolling-form features are
mostly redundant with it here, so the higher-capacity GBM adds variance
without adding signal. `train.py` reports this comparison and picks the
backtest winner automatically — the point of building the harness was to let
the data decide, not to assume the fancier model wins.

### Verification (Phase 4)

```bash
export PYTHONPATH=$PWD
pip install -r ml/match_odds/requirements.txt
python -m ml.match_odds.src.train
```

Confirmed:
- Synthetic dataset: 2,280 matches across 6 seasons, 20 teams.
- Backtest report printed for both models across 5 folds (seasons 1-5,
  season 0 used only as the first training window).
- Model selection logged (`deploying 'elo_only_baseline'`) and
  `models/match_odds_model.joblib` + `models/metadata.json` written.

```bash
python -m ml.match_odds.src.predict --home "Manchester City" --away Arsenal
```

```
Manchester City (home) vs Arsenal (away)
  Home win : 49%
  Draw     : 25%
  Away win : 26%
Top contributing factors: ELO gap favors home (+32), recent form favors home
(+3.00 PPG), attacking output favors home (+1.00 GF/match), home advantage (+)
```

This correctly reflects the real mock-data result (Man City beat Arsenal 2-1
in fixture 1002 — see Phase 3) flowing through `gold.elo_ratings` and
`gold.team_form_features` into the prediction. Also verified team-ID input
(`--home 50 --away 42`, identical output) and cold-start fallback for teams
with no Gold history yet (`--home 33 --away 40`, fixture 1001's teams —
prints the "no Gold history" note and falls back to neutral
50/26/30% odds).

---

## Phase 5 — App 1: Squad Optimizer ✅

### The data problem (same shape as Phase 4)

The design doc ([`docs/apps/01_squad_optimizer.md`](apps/01_squad_optimizer.md))
needs a full squad — 20+ players across GK/DEF/MID/FWD, each with season
stats — to select a lineup from. After Phase 3, `silver.dim_player` has 4
players (from one lineup payload: Onana, Mazraoui, Becker, Alexander-Arnold)
and `gold.player_season_stats` has 2 (from one player-stats payload: Palmer,
Son) — and **these two sets don't even overlap**. No real team has enough
positioned, stat-bearing players to optimize a lineup from yet. Built
**`ml/squad_optimizer/src/synthetic_squad_data.py`**: generates a realistic
23-player squad (3 GK / 8 DEF / 7 MID / 5 FWD, position-appropriate
goal/assist/rating distributions, ~15% flagged unavailable to simulate
injuries/suspensions, with availability corrected post-generation to
guarantee every formation stays feasible) for **`team_id=50`** — Manchester
City in the real Phase 3 Gold/Silver data — so the *opponent* side of the
win-probability calculation still uses real ELO/form. Same bridge pattern as
Phase 4: swap this for a real loader once a real squad's worth of data exists.

### What was built

- **`ml/squad_optimizer/src/contribution.py`** — Stage 1 of the design doc's
  two-stage approach: a per-position weighted score
  (`avg_rating`, goals-per-appearance, assists-per-appearance, an
  availability/fitness proxy from appearance count) — deliberately a
  hand-weighted formula, not a trained model, exactly as the design doc
  specifies for this phase ("start simple... then iterate toward a learned
  model once enough Gold data exists" — there's no historical
  lineup-vs-result dataset to train one against regardless).
- **`ml/squad_optimizer/src/optimizer.py`** — Stage 2: a PuLP integer
  program (`select_best_xi()`) that picks exactly 11 *available* players
  matching a formation's GK/DEF/MID/FWD counts, maximizing total
  contribution score, with an optional total-`cost` budget cap. Four
  formations (`4-4-2`, `4-3-3`, `3-5-2`, `5-3-2`). Raises a clear
  `ValueError` when the squad/budget can't fill the formation (verified:
  budget=50 on a squad needing ~690 in unconstrained cost correctly reports
  infeasible). Also provides `naive_xi()` — fills each position with the
  first available players in roster order, as the "no optimization"
  baseline the design doc calls for ("vs. a baseline lineup, e.g. last
  match's XI").
- **`ml/squad_optimizer/src/recommend.py`** — CLI tying it together. Reuses
  **`ml.match_odds.src.predict.get_match_probabilities()` directly** — App 1
  calling into App 2's win-probability function rather than re-implementing
  match prediction, per the roadmap's stated build order. Converts the
  optimized XI's average-contribution-score edge over the naive baseline
  into an ELO offset (`ELO_POINTS_PER_CONTRIBUTION_POINT = 15.0` — a
  documented simplifying assumption, not a fitted coefficient, flagged the
  same way as Phase 4's synthetic-data bridge) and calls the Phase 4 model
  twice (optimized lineup vs. naive lineup) to get a concrete win-probability
  uplift number. Also prints per-position swap explanations by diffing the
  two XIs.
  - To support reuse, refactored `ml/match_odds/src/predict.py`: extracted
    `get_match_probabilities(spark, home_id, away_id, model=None,
    team_stats=None, home_elo_offset=0.0, away_elo_offset=0.0)` out of the
    CLI's `predict()` function — same calibrated-probability computation,
    minus the printing, with optional ELO offsets for "what if this side
    fielded a stronger/weaker lineup" scenarios. Re-verified the Phase 4 CLI
    still produces identical output after the refactor.

### Verification (Phase 5)

```bash
export PYTHONPATH=$PWD
pip install -r ml/squad_optimizer/requirements.txt
python -m ml.squad_optimizer.src.recommend --opponent Arsenal
```

```
Recommended XI (4-4-2) vs opponent team_id=42:
  GK   Kwame Lindgren       rating=6.56  contribution=40.2
  DEF  Sebastian Larsson    rating=6.74  contribution=35.4
  ...
  FWD  Erik Kovac           rating=7.36  contribution=36.4
  FWD  Mateo Adeyemi        rating=6.82  contribution=35.6

Predicted win probability:
  Optimized XI : Home win 53% | Draw 25% | Away win 23%
  Naive XI     : Home win 49% | Draw 25% | Away win 26%
  Uplift from optimization: +3.7% win probability

Key swap explanations:
  - Kwame Lindgren in for Sven Moreno (GK): +1.1 contribution score
  - Rafa Bergstrom in for Aaron Sousa (DEF): +2.1 contribution score
  - Pierre Akinola in for Marcus Reyes (MID): +5.2 contribution score
  - Erik Kovac in for Cole Tanaka (FWD): +4.4 contribution score
  - Mateo Adeyemi in for Lukas Costa (FWD): +6.5 contribution score
```

Also verified:
- `--formation 4-3-3` and other formations select valid 11-player XIs with
  correct positional counts.
- `--budget` cap correctly trades off contribution score for cost (e.g.
  95%-of-unconstrained-cost budget drops total score from 399.2 to 388.9),
  and reports infeasibility cleanly when the budget is too tight to fill
  the formation at all.
- Team-name (`Arsenal`, `"Manchester United"`) and numeric team-ID (`42`)
  opponent resolution both work via the same `resolve_team_id()` used in
  Phase 4.

---

## Phase 6 — App 3: Live Tournament Predictor ✅

### The data problem (same shape as Phases 4 and 5, different angle)

The design doc ([`docs/apps/03_tournament_predictor.md`](apps/03_tournament_predictor.md))
assumes a World Cup–shaped competition: neutral venue, groups + knockout.
That's a different competition *shape* from the club leagues already in the
mock pipeline, not just sparser data — `silver.dim_team` has 4 real club
teams, which is half a group and the wrong format regardless of row count.
Built **`ml/tournament_predictor/src/structure.py`**: 8 fictional national
teams (Norrland, Castellan, Meridia, Boreas, Tarawak, Valdoria, Solaria,
Kestria) with deliberately spread base ELOs (1470–1620) across 2 groups of
4, single round-robin group stage, then a cross-bracket-seeded 4-team
knockout (semifinals + final). Same bridge pattern as Phases 4 and 5: these
teams don't exist in the real lakehouse, so this app tracks their ELO/form
purely in memory — no Spark dependency anywhere in App 3, only the trained
joblib model file from Phase 4.

### What was built

- **`structure.py`** — `TEAMS`, `GROUPS`, `GROUP_FIXTURES` (round-robin via
  `itertools.combinations`), `KNOCKOUT_SEEDING` (group winner vs. the
  *other* group's runner-up, so the same group's teams can't meet again
  before the final), and `compute_group_standings()` — points → goal
  difference → goals for → head-to-head tiebreak chain.
- **`simulate.py`** — Monte Carlo engine (default 10,000 trials, ~3.5 min).
  Per trial: simulate every remaining group fixture by sampling an outcome
  from App 2's trained model, updating each team's in-trial ELO/form after
  every match so later fixtures — including the knockout stage — reflect
  results earlier in *that* trial; compute standings; build and simulate the
  knockout bracket; record who qualified / won the group / won it all.
  Aggregating across trials gives each team's stage-reaching probabilities.
  Two correctness issues solved here:
  - **Neutral-venue bias**: the match_odds model was trained on club
    fixtures with a real home side, so it bakes in a learned home-advantage
    effect. `_match_outcome_probs()` averages the team-A-as-home and
    team-B-as-home framings to cancel that bias out for a neutral-venue
    tournament match, rather than arbitrarily picking a "home" team.
  - **No scorelines, no shootouts**: the model only predicts a W/D/L
    category, but group standings need goal difference to break ties —
    `_sample_scoreline()` layers a simple weighted heuristic on top, purely
    for tiebreak purposes. Knockout matches can't end in a draw and the
    model has no signal to predict a penalty shootout, so a drawn knockout
    match resolves via a 50/50 coin flip — a documented simplification, not
    a modeled outcome.
- **`state.py`** — the design doc calls for results in `gold.tournament_state`
  (or "a small Postgres table"); since this app already has no Spark
  dependency, this substitutes a single JSON file under the lakehouse root
  (`$LAKEHOUSE_BASE_PATH/gold/tournament_state.json`) — `load_state()` /
  `save_state()` / `record_result()`, plain file I/O.
- **`live_consumer.py`** — Kafka consumer that re-triggers the simulation
  on a live result. Subscribes to `football.fixtures.raw` (the only topic
  carrying match status — confirmed `football.events.live` has no status
  field at all) and watches each tracked fixture (both teams in `TEAMS`)
  for its first NS/1H/2H → FT transition: records the final score, then
  re-simulates with a smaller trial count (2,000 — fast enough to react to
  a live event, vs. the CLI's 10,000-trial on-demand default) and prints
  the updated report. Guards against double-counting a result if a message
  is redelivered or the consumer restarts before its last offset commit.
  Also subscribes to `football.events.live` for goal/card/sub events,
  logged for visibility only — resimulating mid-match on every goal, rather
  than only at full-time, is the design doc's stated stretch goal, not the
  MVP. Real producers only ever publish real club fixtures, so nothing in
  the live pipeline naturally matches a fictional team id — `live_consumer.py`
  doubles as its own test harness via `--publish-test-result HOME AWAY
  HOME_GOALS AWAY_GOALS`, which publishes one synthetic, already-finished
  fixture message in the same envelope a real producer would use.

### Verification (Phase 6)

```bash
export PYTHONPATH=$PWD
python -m ml.tournament_predictor.src.simulate --trials 10000
```

```
Group A standings + qualification probabilities (10,000 simulations)
  Norrland   Pld 0, Pts 0   -> Qualify: 70%  | Win group: 43%
  Meridia    Pld 0, Pts 0   -> Qualify: 53%  | Win group: 26%
  Boreas     Pld 0, Pts 0   -> Qualify: 40%  | Win group: 17%
  Castellan  Pld 0, Pts 0   -> Qualify: 36%  | Win group: 14%

Group B standings + qualification probabilities (10,000 simulations)
  Tarawak    Pld 0, Pts 0   -> Qualify: 68%  | Win group: 41%
  Solaria    Pld 0, Pts 0   -> Qualify: 50%  | Win group: 24%
  Kestria    Pld 0, Pts 0   -> Qualify: 45%  | Win group: 20%
  Valdoria   Pld 0, Pts 0   -> Qualify: 36%  | Win group: 15%

Tournament winner probabilities (top 5)
  Norrland: 25.1%  Tarawak: 21.3%  Meridia: 13.6%  Solaria: 11.0%  Kestria: 9.3%
```

Confirmed favorites rank correctly by base ELO at every stage (Norrland
1620 and Tarawak 1600 lead qualify/win-group/win-tournament; Castellan 1480
and Valdoria 1470 trail). Verified `compute_group_standings()` correctly
ignores cross-group matches when fed a shared `completed_results` list
(Group A's table is unaffected by a Group B result and vice versa) — the
implicit thing `_run_one_trial()` relies on.

Verified `--from-state` against a real recorded result (Norrland beat
Castellan 3-0): Norrland's qualify probability jumped to 93%, reflecting
the played match plus a partial points/goal-difference table for the
unplayed teams.

End-to-end live trigger, against a real local Kafka broker (KRaft mode):
started `live_consumer.py` in one process, ran
`live_consumer.py --publish-test-result 9001 9002 3 0` in another. The
consumer detected the FT transition, recorded the result, re-simulated
(2,000 trials), and printed the updated report — matching the
`--from-state` numbers above. Re-publishing the identical result was
correctly skipped by the dedupe guard (`_already_recorded()`); publishing a
second, different tracked pair (Tarawak 1-1 Valdoria) was correctly
recorded alongside the first, leaving exactly 2 entries in
`tournament_state.json`. Also verified `football.events.live` messages for
tracked teams are logged (`[fixture 99001] 23' Norrland: Goal (Normal
Goal)`) while events for untracked (real club) teams are correctly
filtered out.

---

## Phase 7 — Polish & wrap-up ✅

### The data problem

Not a data problem this time — all 3 apps and the medallion pipeline were
already built and verified (Phases 1-6). What was missing was the
operational layer around them: a way to *run* the pipeline on a schedule
instead of by hand, a way to *see* whether it's healthy without reading
Spark logs, and a way to *use* the 3 apps without a terminal.

### What was built

- **Orchestration (`orchestration/dags/`)** — two Airflow DAGs, each task a
  `BashOperator` shelling out to the exact module/script a developer would
  run by hand (no pipeline logic reimplemented in Airflow):
  - `medallion_pipeline_dag.py` (`@daily`): 4 parallel ingestion producers
    (`--once`) -> `bronze_ingest` -> `silver_transform` -> `gold_aggregate`
    -> `data_quality`. The last task doubles as a quality gate — it already
    exits non-zero on any failed check, which fails the DAG run with no
    extra DAG-side logic needed.
  - `model_retrain_dag.py` (`@weekly`): re-trains + re-backtests App 2's
    match odds model via `ml.match_odds.src.train`, which already deploys
    whichever model wins the walk-forward backtest.
  - Both parameterized by `FIFA_REPO_HOME`/`FIFA_PYTHON_BIN` env vars
    (defaulting to the inferred repo root and `python3`), following the
    `LAKEHOUSE_BASE_PATH`/`KAFKA_BOOTSTRAP_SERVERS` convention already used
    everywhere else in the pipeline.
  - Airflow lives in its **own venv** (`/opt/airflow-venv`, installed via
    the official constraints-file method), kept separate from the
    Spark/ML venv — Airflow's own process never imports `pyspark`, it just
    invokes the other venv's Python via `FIFA_PYTHON_BIN`. Avoids Airflow's
    large, conflict-prone dependency tree ever touching the
    already-verified Spark/ML stack.
- **Observability (`streaming/jobs/data_quality.py`, refactored)** — split
  the existing 52-check suite's core into `run_checks()` (reusable, no
  printing/exiting) so it's shared by the CLI, the Airflow DAG, and report
  writing. Added:
  - `write_report()` — JSON snapshot (`gold/data_quality_report.json`) of
    every run: total/passed/failed counts + each check's table/check/detail,
    so a dashboard can read the latest results without its own Spark
    session.
  - `notify_on_failure()` — logs a warning per failed check always; also
    POSTs a summary to `SLACK_WEBHOOK_URL` if that env var is set, so the
    alert path is demoable without depending on a real Slack workspace.
- **Streamlit app (`app/`)** — a web UI over all 3 apps + pipeline health,
  sharing one Spark session and one loaded model across pages
  (`st.cache_resource`, `app/shared.py`):
  - `streamlit_app.py` — landing page / nav overview.
  - `pages/1_Match_Odds_Predictor.py` — team selectors (from `silver.dim_team`
    when available) -> win/draw/loss bar chart + the same
    `explain_prediction()` text the CLI prints (promoted from a private
    helper in `predict.py` so both the CLI and this page call the same code).
  - `pages/2_Squad_Optimizer.py` — opponent/formation/budget controls ->
    recommended XI table, win-probability uplift vs. a naive baseline, and
    swap explanations (`describe_swaps()`, similarly promoted from
    `recommend.py`).
  - `pages/3_Tournament_Predictor.py` — adjustable-trial-count simulation,
    standings + qualification odds, and a "record a live result" form that
    calls the exact same `state.record_result()` + `simulate.run_simulation()`
    path `live_consumer.py` calls on a real Kafka FT event — lets the
    live-trigger effect be demoed in the UI without a running broker. This
    page has no Spark dependency, matching App 3's existing design.
  - `pages/4_Pipeline_Health.py` — renders the latest
    `data_quality_report.json` directly (no Spark session); a button
    re-runs `data_quality.py` as a subprocess and refreshes.
- **Instruction manual (`docs/RUNBOOK.md`)** — the single from-scratch guide
  to environment setup (the two-venv split, Kafka, env vars), running every
  phase end to end, the 3 apps' CLIs, the Streamlit app, the Airflow DAGs,
  an end-to-end smoke test script, and a troubleshooting section.
- **`orchestration/README.md`** — Airflow-specific setup/verification
  instructions, separated from the general runbook the same way each
  component already has its own README.
- **Architecture + README refresh** — `docs/architecture.md`'s diagram and
  "Serving / Apps" section updated to reflect what was actually built
  (correcting one inaccuracy from the Phase 0 design: App 3's live trigger
  watches `football.fixtures.raw`, the only topic carrying match status,
  not `football.events.live`) plus new Orchestration + observability
  section; top-level `README.md` rewritten from its Phase-0 draft to
  reflect the finished system.

### Verification

- **Airflow DAGs**: `airflow dags list-import-errors` reported zero errors
  for both DAGs after `airflow db migrate`; `airflow dags list` showed both
  `medallion_pipeline` and `match_odds_model_retrain` registered.
  `airflow tasks test medallion_pipeline ingest_fixtures 2025-01-01` ran the
  real `BashOperator` command end-to-end (producer connected to the local
  Kafka broker and published 2 messages); `airflow tasks test
  match_odds_model_retrain train_match_odds_model 2025-01-01` ran the real
  training script end-to-end (synthetic backtest, model saved to
  `ml/match_odds/models/match_odds_model.joblib`).
- **`data_quality.py` refactor**: ran against the real `/tmp/fifa-lakehouse`
  data — 52 checks, 48 passed, 4 failed (all 4 were `freshness` checks on
  data older than the 25h threshold — correct behavior, not a bug).
  Confirmed `gold/data_quality_report.json` was written with the right
  structure, and `notify_on_failure()` logged 4 warnings with no Slack
  attempt (silent no-op, since `SLACK_WEBHOOK_URL` was unset).
- **Streamlit app**: started a real server (`streamlit run
  app/streamlit_app.py`) and confirmed it serves all 5 pages over HTTP. No
  browser is available in this dev environment, so page-script correctness
  was verified with `streamlit.testing.v1.AppTest` instead — ran every page
  headlessly against the real Spark session/lakehouse/model and asserted no
  exception was raised; all 5 passed (Match Odds Predictor and Squad
  Optimizer each exercised a full predict/recommend round trip against real
  Gold data, not just a page load). This confirms the page logic runs
  correctly end-to-end but isn't a substitute for an actual browser check of
  layout/UX.

### Public demo (Streamlit Community Cloud)

Added so the Streamlit app is reachable by anyone with a browser, not just
someone with the full local stack running:

- **The data problem**: a free hosted container has no Java, no Kafka, and
  no live lakehouse — and shouldn't need one just to demo 3 ML apps that
  already have a documented synthetic-data bridge each.
- **What was built**: `app/shared.py`'s `get_cached_spark()` now returns
  `None` whenever a real Spark session can't be started (missing Java/pyspark,
  or `FIFA_FORCE_DEMO_MODE=true` for local testing) instead of raising —
  every lookup (`get_team_stats`, `get_team_list`, `resolve_team_id`) checks
  `spark is None` and falls back to a static snapshot
  (`app/demo_data/teams.csv` + `team_stats.json`, 20 fictional clubs) built
  by `app/demo_data/build_snapshot.py`, which replays
  `synthetic_data.py`'s exact ELO-update loop and seed so the demo's numbers
  are produced by the same math as the real Gold layer. `get_cached_model()`
  trains the match-odds model in-process from synthetic data on first load
  when no model file exists yet (no binary committed to git) — same
  `train.run()` the CLI uses, just triggered lazily. Net result:
  `app/requirements.txt` only needs `streamlit`/`pandas`/`numpy`/
  `scikit-learn`/`joblib`/`pulp` — no pyspark/delta-spark/Java on the hosted
  side at all, and the *same* `streamlit_app.py` entrypoint runs unmodified
  locally or on Streamlit Cloud.
- **Verification**: `streamlit.testing.v1.AppTest` run twice — once against
  the real Spark/lakehouse (all 5 pages + Predict/Recommend/Run-simulation
  button clicks passed), once with `FIFA_FORCE_DEMO_MODE=true` and no
  pre-trained model and no lakehouse at all (all 5 pages passed; Pipeline
  Health correctly short-circuits with an explanatory message instead of
  attempting a Spark subprocess; Predict/Recommend/Run-simulation all
  produced real probabilities against the synthetic snapshot).
- **Docs**: [`docs/DEPLOY_STREAMLIT_CLOUD.md`](DEPLOY_STREAMLIT_CLOUD.md) —
  step-by-step Streamlit Community Cloud deployment, plus how to preview the
  exact same demo path locally before deploying.

### What's next

All 7 roadmap phases are now functionally complete. Remaining Phase 7 items
are presentation polish that don't change behavior: a refreshed
architecture diagram image (the mermaid source is updated; a rendered
image/GIF is optional) and a demo video/GIF walkthrough of the Streamlit
app. See [`retrospective.md`](retrospective.md) for the project-level
lessons learned.

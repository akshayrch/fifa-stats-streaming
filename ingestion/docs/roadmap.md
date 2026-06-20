# Roadmap

Phased so each phase ships something demo-able and write-up-able (Medium +
LinkedIn). Pace is intentionally not date-bound — adapt to your weekly
schedule (this pairs well with the weekend "project day" pattern in the main
`data_engineering_architecture` repo).

## Phase 0 — Repo scaffold (this commit)
- [x] Repo structure, architecture docs, roadmap
- [x] Kafka topic design
- [x] Medallion table contracts (design only)
- [x] App specs for the 3 apps

**Write-up**: "I'm building a real-time football data platform — here's the
architecture" (Medium #1 / LinkedIn #1)

## Phase 1 — Ingestion: Kafka producer/consumer
- [x] Local Kafka cluster (KRaft mode) — via docker-compose, or
      `infra/run_local_kafka.sh` if Docker Hub anonymous pulls are rate-limited
- [x] API-Football client with auth, rate-limit handling, retries, and a
      `mock` mode (canned responses in `ingestion/sample_data/`) for testing
      without spending the free-tier daily budget
- [x] `fixtures_producer` — fixtures for tracked leagues -> `football.fixtures.raw`
- [x] `live_events_producer` — live match events + player stats ->
      `football.events.live` / `football.player_stats.raw`
- [x] `lineups_producer` (lookahead-window logic), `standings_producer`
- [x] `sanity_check_consumer` — verified end-to-end (producers -> Kafka ->
      consumer) with correct `league_id`/`fixture_id` partition keying
- [x] Topic/partition design documented and justified
- [ ] Run against the **live** API-Football endpoint (needs a RapidAPI key)

**Write-up**: "Building a Kafka producer for live football data — topics,
partitions, and the rate-limit dance with a free API" (Medium #2 / LinkedIn #2)

## Phase 2 — Stream processing + Medallion (Bronze)
- [x] `bronze_ingest.py` — Spark Structured Streaming, Kafka -> Delta (Bronze)
- [x] Local-dev lakehouse on the filesystem (`LAKEHOUSE_BASE_PATH=file://...`)
      as a no-Docker/no-MinIO alternative to S3A, selected automatically by
      `spark_session.py`
- [x] Checkpointing per topic/table (`_checkpoints/bronze_<table>`);
      `--continuous` flag for long-running mode vs. one-shot
      `trigger(availableNow=True)` for scheduled/replay runs
- [x] Bronze tables queryable via Spark SQL — verified end-to-end
      (Kafka -> Spark Structured Streaming -> Delta) for all 5 topics:
      `fixtures_raw`, `events_raw`, `lineups_raw`, `standings_raw`,
      `player_stats_raw`, with Kafka metadata (`key`, `topic`, `partition`,
      `offset`, `kafka_ts`) and `ingest_ts` preserved
- [ ] Schema-on-read validation (deferred to Phase 3 alongside Silver parsing)
- [ ] Replay test (kill + resume mid-stream) on a long-running `--continuous` run

**Write-up**: "From Kafka to Lakehouse: Spark Structured Streaming into a
Bronze layer" (Medium #3 / LinkedIn #3)

## Phase 3 — Silver + Gold
- [x] `silver_transform.py` — explicit StructType schemas per payload, Delta
      MERGE upsert via `foreachBatch`, one streaming query per Bronze source,
      multiple Silver tables written per batch (fixtures → `fact_match` +
      `dim_team` + `dim_league`; standings → `fact_standings_snapshot`;
      events → `fact_match_event`; player_stats → `fact_player_match_stat`;
      lineups → `dim_player`)
- [x] `gold_aggregate.py` — rolling last-5/10 form features, ELO ratings
      (chronological, driver-side K=32 formula), H2H records, player season
      stats, upcoming-match prediction feature join
- [x] `data_quality.py` — 52 checks across Bronze/Silver/Gold: table
      existence, row counts, null rates on key columns, freshness (ingest_ts
      age vs threshold); all 52 checks pass on mock data end-to-end
- [ ] Replay test (kill + resume) on `--continuous` Silver streaming run

**Write-up**: "Designing a Medallion architecture for football stats: Bronze,
Silver, Gold" (Medium #4 / LinkedIn #4)

## Phase 4 — App 2: Match Odds Predictor ✅
(Built before App 1 — it's the simplest model and validates the Gold features)
- [x] Feature set from `match_prediction_features` (`elo_diff`, last-5 PPG,
      last-5 GF for both sides) — `ml/match_odds/src/features.py`
- [x] Synthetic historical training data (`synthetic_data.py`) — the real
      mock pipeline only has 1 finished match so far; simulates 6 seasons /
      20 teams with the same ELO update rule as `gold_aggregate.py`, ~45%
      home-win rate matching the design doc's real-world baseline
- [x] Baseline model (logistic regression on `elo_diff`) -> calibrated
      gradient boosting (full feature set, isotonic calibration)
- [x] Walk-forward backtest (`evaluate.py`) — train on seasons `[0..k)`,
      evaluate on season `k`; `train.py` deploys whichever model wins on
      log-loss rather than always picking the fancier one (on this synthetic
      data, the ELO-only baseline wins: 0.898 vs 0.958 avg log-loss)
- [x] Minimal serving — CLI (`predict.py`): team name or ID -> win/draw/loss
      probabilities + rule-based explanation, using real Gold ELO/form where
      available and cold-start defaults otherwise

**Write-up**: "Predicting match odds from a real-time feature store" (Medium #5
/ LinkedIn #5)

## Phase 5 — App 1: Squad Optimizer ✅
- [x] Player-level features — synthetic 23-player squad
      (`synthetic_squad_data.py`) for `team_id=50`, since real
      `player_season_stats`/`dim_player` don't yet overlap on a full squad;
      `contribution.py` scores each player via a per-position weighted
      combination of rating, goals/assists-per-appearance, and an
      availability/fitness proxy (deliberately simple, per the design doc —
      not a trained model yet)
- [x] Win-probability uplift — `recommend.py` reuses
      `ml.match_odds.src.predict.get_match_probabilities()` directly
      (App 1 calling into App 2, per the build order), translating the
      optimized XI's contribution-score edge over a naive baseline XI into
      an ELO offset
- [x] Constraint solver (PuLP, CBC backend) — exact GK/DEF/MID/FWD counts
      for 4 formations (`4-4-2`, `4-3-3`, `3-5-2`, `5-3-2`), available-only,
      optional total-cost budget cap; raises a clear error when infeasible
- [x] Output: recommended XI + win probability vs. a naive baseline lineup +
      per-position swap explanations (CLI: `recommend.py`)

**Write-up**: "Optimizing a starting XI with constraint programming + ML"
(Medium #6 / LinkedIn #6)

## Phase 6 — App 3: Live Tournament Predictor
- [ ] Tournament structure model (groups/knockout) as data
- [ ] Monte Carlo simulation using App 2's odds model
- [ ] Re-trigger simulation on `football.events.live` result changes
- [ ] Live-updating output (qualification probabilities, bracket odds)

**Write-up**: "Real-time tournament simulation triggered by live match events"
(Medium #7 / LinkedIn #7)

## Phase 7 — Polish & wrap-up
- [ ] Orchestration (Airflow) for batch gold/model-retraining jobs
- [ ] Observability: pipeline dashboards, data quality alerts
- [ ] README polish, architecture diagram refresh, demo video/GIFs
- [ ] Retrospective write-up: what worked, what I'd change, lessons learned

**Write-up**: "What I learned building a real-time football AI platform end to
end" (Medium #8 / LinkedIn #8)

---

## Stretch ideas (post-MVP)
- Cloud deployment (MSK/Kinesis + EMR/Glue or Databricks)
- Multi-source ingestion (add a second free API for cross-validation)
- A small web UI (Streamlit) for the 3 apps
- Model monitoring / drift detection on the odds predictor

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

## What's next

See [`roadmap.md`](roadmap.md) for the full phase list. Immediate next step
is **Phase 3 — Silver + Gold**:

- `silver_transform.py`: define explicit schemas for each payload type, parse
  `bronze.*` -> conformed `silver.dim_team`, `silver.dim_player`,
  `silver.fact_match`, `silver.fact_player_match_stat` (dedup + MERGE/upsert)
- `gold_aggregate.py`: rolling team form, ELO ratings, head-to-head records,
  player season stats
- Data quality checks (row counts, null rates, freshness) across all layers

Then Phase 4 (Match Odds Predictor — built first since it's the simplest
model and validates the Gold features), Phase 5 (Squad Optimizer), Phase 6
(Live Tournament Predictor), and Phase 7 (polish/orchestration/wrap-up).

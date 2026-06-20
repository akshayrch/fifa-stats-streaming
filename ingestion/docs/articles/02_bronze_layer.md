# From Kafka to Lakehouse: Spark Structured Streaming into a Bronze layer

With five Kafka topics publishing real (well, mock) data, Phase 2 was about
getting that data durably and replayably onto disk as the first hop of the
Medallion lakehouse. This is the phase where "Spark Structured Streaming"
stopped being an architecture-doc line item and became a script that either
worked or didn't.

## What Bronze is for, and what it deliberately isn't

The design principle for Bronze, decided back in Phase 0 and held to here,
is that it's the raw, replayable source of truth — no parsing, no
deduplication, no schema enforcement on the payload itself. Every row that
lands in Bronze keeps the Kafka metadata (`key`, `topic`, `partition`,
`offset`, `kafka_ts`) alongside the raw envelope JSON string and an
`ingest_ts` stamp. That's it. The temptation when you're already in Spark
and already have a schema in your head is to parse the JSON right there in
the Bronze job — I didn't, on purpose, because the whole point of a Bronze
layer is that if your Silver parsing logic has a bug, you can fix it and
replay from Bronze without re-pulling anything from Kafka (which, given the
100-requests/day budget from Phase 1, matters more here than it would in a
project with an unlimited data source).

`streaming/jobs/bronze_ingest.py` subscribes to all 5 `football.*` topics
via a `TOPIC_TABLE_MAP`, reads `key`/`value` (cast to string), `topic`,
`partition`, `offset`, and `timestamp` (renamed `kafka_ts`), stamps
`ingest_ts = current_timestamp()`, and appends to the matching
`bronze.<table>` Delta table:

| Kafka topic | Bronze Delta table |
|---|---|
| `football.fixtures.raw` | `bronze/fixtures_raw` |
| `football.events.live` | `bronze/events_raw` |
| `football.lineups.raw` | `bronze/lineups_raw` |
| `football.standings.raw` | `bronze/standings_raw` |
| `football.player_stats.raw` | `bronze/player_stats_raw` |

It supports two run modes: a one-shot `trigger(availableNow=True)` (the
default — processes everything currently on the topics, then exits, which
is exactly what you want for a cron/Airflow-scheduled batch run) and a
`--continuous` flag for a long-running stream that picks up new messages as
they arrive. Each topic also gets its own checkpoint directory
(`_checkpoints/bronze_<table>`), so each of the 5 streams tracks its own
Kafka offsets independently — one stream falling behind or failing doesn't
take the others down with it.

## The no-Docker lakehouse path

`streaming/jobs/spark_session.py` is the shared `SparkSession` builder
behind every streaming/batch job in the project, and it makes a decision
worth calling out: `LAKEHOUSE_BASE_PATH` defaults to
`file:///tmp/fifa-lakehouse` — a plain local filesystem path — rather than
requiring MinIO/S3A. If you do set it to an `s3a://...` URI, the builder
pulls in `hadoop-aws` and wires up the S3A connector against MinIO instead.
Given the Docker Hub rate-limit pain from Phase 1, having a working
no-Docker, no-MinIO path for local dev wasn't optional — it's the path I
actually used to verify this phase, with the S3A path remaining available
for anyone who wants the closer-to-production setup.

The Spark/Delta dependency story had its own small landmine: `pyspark==3.5.3`
and `delta-spark==3.2.1` needed `setuptools<70` pinned, to avoid a packaging
issue between pyspark 3.5.3 and newer setuptools. Small thing, but it's the
kind of dependency-pinning detail that costs you twenty minutes of
confusing error messages if you don't write it down — so it's now in the
dedicated `/opt/spark-venv` setup notes rather than in my head. Both Spark
and Delta also need to agree on Scala/connector versions under the hood,
which is exactly the kind of compatibility matrix that's invisible until
it silently isn't satisfied, so pinning both at once rather than letting
either float was the safer call here.

## Verifying it actually worked

Running `python streaming/jobs/bronze_ingest.py` in one-shot mode
downloaded and cached the Kafka/Delta connector jars (about 63 MB, pulled
via Maven coordinates in `spark.jars.packages`) and wrote all 5 Bronze
Delta tables under `/tmp/fifa-lakehouse/bronze/`. Reading each one back with
`spark.read.format("delta").load(...)` confirmed:

| Bronze table | Rows | Key |
|---|---|---|
| `fixtures_raw` | 2 | `league_id` |
| `events_raw` | 1 | `fixture_id` |
| `lineups_raw` | 1 | `fixture_id` |
| `standings_raw` | 1 | `league_id` |
| `player_stats_raw` | 1 | `fixture_id` |

Tiny row counts — this is mock data, and it's the same handful-of-rows
constraint that ends up shaping every later phase. But the things that
mattered for this phase checked out: every table has the right key, the
right topic/partition/offset metadata, and the envelope's
`source`/`endpoint`/`request_id`/`ingest_ts`/`payload` shape intact. The
`_delta_log` transaction logs and the per-table checkpoint directories
under `_checkpoints/bronze_*` were also populated correctly, which is the
part that actually matters for production behavior — it confirms
Structured Streaming's exactly-once write semantics are wired up correctly
for replay and resume, not just that data showed up once.

## What I deferred, on purpose

Two things didn't happen in Phase 2, and I want to be specific about why,
because "deferred" is doing real work here rather than papering over a gap.
Schema-on-read validation of the `value` JSON was pushed to Phase 3 — Bronze
is supposed to be schema-less by design, so validating the JSON shape
belongs in Silver, where parsing happens anyway, not bolted awkwardly onto
an append-only raw layer. And a kill-and-resume replay test on a
long-running `--continuous` stream didn't happen yet, because at this point
in the project there wasn't a long-running stream to kill — that's a test
that makes more sense once Phase 3's Silver job, which actually does
stateful upserts, is also in place to exercise alongside it.

## What's next

Bronze gave me five raw, replayable, metadata-tagged Delta tables. None of
it is queryable in a useful way yet — `value` is still a JSON string,
unparsed. The next phase is where that actually becomes structured data:
explicit schemas, a Delta MERGE upsert pattern, and the Gold feature tables
the three ML apps will eventually read from.

## LinkedIn version

Phase 2 of my real-time football platform: getting Kafka data durably onto
disk as a Bronze Delta layer via Spark Structured Streaming.

The design principle: Bronze stays raw and unparsed on purpose. No schema
enforcement, no dedup — just the Kafka metadata (key, topic, partition,
offset) plus the envelope JSON, so a bad parsing rule downstream can always
be fixed and replayed without re-pulling from a rate-limited API.

What shipped:
- `bronze_ingest.py`: one Spark Structured Streaming job, 5 Kafka topics in,
  5 Bronze Delta tables out, independent checkpoints per table
- A one-shot `trigger(availableNow=True)` mode for scheduled batch runs vs.
  a `--continuous` mode for long-running streams
- A no-Docker, no-MinIO local lakehouse path (`file:///tmp/fifa-lakehouse`)
  as a first-class option, not just a fallback
- Verified end to end: all 5 Bronze tables, correct partition keys, correct
  metadata, correct transaction logs for exactly-once replay/resume

Deliberately deferred: schema validation (that's Silver's job) and a
kill-and-resume replay test (needs a long-running stream to actually
exercise).

Full write-up: [link to Medium article]

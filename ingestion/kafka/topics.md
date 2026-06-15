# Kafka Topic Design

## Topics

| Topic | Key | Partitions | Retention | Description |
|---|---|---|---|---|
| `football.fixtures.raw` | `league_id` | 3 | 7 days | Fixture lists (scheduled/finished) per league/season |
| `football.events.live` | `fixture_id` | 6 | 7 days | Live match events: goals, cards, subs, VAR |
| `football.lineups.raw` | `fixture_id` | 3 | 7 days | Starting XI + formation per fixture |
| `football.standings.raw` | `league_id` | 3 | 30 days | League table snapshots |
| `football.player_stats.raw` | `fixture_id` | 6 | 7 days | Per-player per-match statistics |

Defined machine-readably in [`topics.yaml`](topics.yaml) and created by
[`create_topics.sh`](create_topics.sh).

## Partitioning rationale

- **Key by `fixture_id` for match-scoped data** (events, lineups, player
  stats): guarantees all messages for a given match land in the same
  partition, preserving event ordering — critical for `football.events.live`,
  where event order (goal before/after a card, etc.) matters for downstream
  state (live score, tournament simulation triggers).
- **Key by `league_id` for league-scoped data** (fixtures, standings): much
  lower volume, but keying by league still gives useful parallelism if
  tracking multiple leagues, and keeps a league's data ordered.
- **Partition counts** are deliberately small (3-6) for a single-broker local
  dev cluster and the data volumes of 1-2 tracked leagues. This is a "explain
  the trade-off" point for write-ups: partition count should match expected
  consumer parallelism and throughput, not be maxed out by default. Document
  in the Kafka deep-dive write-up how/when you'd repartition for multiple
  leagues or a full tournament (e.g., `fixture_id`-keyed topics scale
  naturally — just increase partitions; `league_id`-keyed topics would need a
  different key if tracking 50+ leagues).

## Producer config (all producers)

```python
{
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all",
    "retries": 5,
    "linger.ms": 50,          # small batching window
    "compression.type": "zstd",
}
```

## Message envelope (all topics)

```json
{
  "source": "api-football",
  "endpoint": "/fixtures",
  "request_id": "uuid",
  "ingest_ts": "2026-06-11T14:32:00Z",
  "payload": { "...raw API-Football response item..." }
}
```

The `payload` is kept as the raw API response shape — schema parsing happens
in the Silver Spark job, not at ingestion time. This keeps producers simple
and Bronze tables faithful to the source.

## Consumer groups (planned)

| Consumer group | Topics consumed | Purpose |
|---|---|---|
| `bronze-writer` | all 5 topics | Spark Structured Streaming -> Bronze Delta tables |
| `tournament-live` | `football.events.live` | App 3's real-time trigger (Phase 6) |
| `sanity-check` | all 5 topics | Phase 1 — simple consumer to validate producers during development |

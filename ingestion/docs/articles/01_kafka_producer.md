# Building a Kafka producer for live football data — topics, partitions, and the rate-limit dance with a free API

Phase 1 of this project was supposed to be the "easy" phase: write some
Python that polls an API and publishes to Kafka. It turned out to have two
real constraints worth writing about — a free-tier API budget that shapes
the whole ingestion design, and a local Kafka cluster that refused to start
because of a rate limit that had nothing to do with football at all.

## The API budget problem

API-Football's free tier (via RapidAPI) gives you 100 requests/day,
rate-limited to roughly 10/minute. That's not a "be a little careful"
constraint, it's a hard design input. You cannot poll continuously and you
cannot brute-force your way past it on a free plan. So before writing the
producers, I wrote down a budget split in `docs/data_sources.md`: roughly
10 requests for the day's fixtures, 10 for standings, 10 for lineups near
kickoff, and the remaining ~70 reserved for live-event polling during an
actual match window. On non-match days the pipeline just idles, which is
correct behavior, not a bug — and worth saying explicitly in a write-up,
because "cost-aware ingestion" is a real production concern, not just a
free-tier inconvenience.

That budget is also why `ingestion/producers/api_football_client.py` has a
`mock: true` mode baked in from the start — it reads canned JSON responses
from `ingestion/sample_data/` instead of hitting the live API. Every
producer, every Spark job, every app in this project has so far been built
and verified entirely in mock mode, specifically so that building and
testing the pipeline doesn't eat into the 100-requests/day budget before
the pipeline is even working. The client also wraps `tenacity` for
retry/backoff and paces requests against the rate limit, so the one time
this does point at the live API, it doesn't immediately get throttled.

## Four producers, one envelope, five topics

The actual producer code ended up being the straightforward part once the
topic design was settled:

| Producer | Topic | Partition key |
|---|---|---|
| `fixtures_producer.py` | `football.fixtures.raw` | `league_id` |
| `standings_producer.py` | `football.standings.raw` | `league_id` |
| `live_events_producer.py` | `football.events.live` + `football.player_stats.raw` | `fixture_id` |
| `lineups_producer.py` | `football.lineups.raw` | `fixture_id` |

`lineups_producer.py` has a bit of logic worth calling out: it only
publishes for fixtures kicking off within a configurable
`lineups_lookahead_minutes` window, instead of publishing every lineup it
can see. I verified that filter actually works by temporarily widening the
lookahead window and confirming it then produced a message — the kind of
"prove the negative case" check that's easy to skip and exactly the case
that bites you later if the filter logic is inverted.

Every producer publishes through a shared `kafka_producer.py` with an
idempotent config (`enable.idempotence=true`, `acks=all`,
`compression.type=zstd`) and a `publish()` helper that wraps every payload
in the same envelope — `source`, `endpoint`, `request_id`, `ingest_ts`, and
the raw API payload itself. That consistency matters later: Bronze doesn't
need topic-specific parsing logic, because every message on every topic
looks the same at the envelope level regardless of what's inside `payload`.

Partition keys follow the access pattern, not just "spread load evenly":
fixtures and standings key on `league_id`, events and lineups and
player-stats key on `fixture_id`. The two fixture-keyed topics
(`events.live`, `player_stats.raw`) also get more partitions (6 vs. 3) than
the league-keyed ones, since concurrent live matches need to spread across
partitions, while `standings.raw` gets a longer retention window (30 days
vs. 7) because it's a low-volume daily snapshot that's actually useful to
keep around for history and replay.

To verify all of this end to end, I built
`ingestion/consumers/sanity_check_consumer.py` — a dev tool that subscribes
to all 5 topics and prints `[topic] key=... ingest_ts=... endpoint=...
payload_size=...` per message. Running the four producers once in mock
mode, then the sanity consumer, confirmed every producer hits its correct
topic with the correct envelope and the correct partition key — checked
against `kafka-get-offsets.sh`, not just eyeballed in the consumer output.

## The Docker Hub surprise

The constraint I didn't plan for: Docker Hub's anonymous pull rate limit
blocked `docker compose up` for the Kafka image entirely during local
development. Not a football problem, not even really a Kafka problem — a
"free container registry tier has limits too" problem that showed up at
the worst possible time, right as I was trying to bring up the simplest
piece of local infrastructure in the whole project.

The fix was to stop depending on the Docker image at all for the dev loop:
`infra/run_local_kafka.sh` downloads Apache Kafka 3.8.1 directly from
`archive.apache.org` and runs a single broker in KRaft mode (no Zookeeper)
on `localhost:9092`, and `ingestion/kafka/create_topics.sh --no-docker`
creates the 5 topics straight from `topics.yaml` using `kafka-topics.sh`
instead of `docker exec`. The Docker Compose path (Kafka + Kafka UI + MinIO)
is still there and still works in environments where the registry pull
limit isn't an issue — this was an "and" not an "or." But it meant Phase 1
ended up shipping two ways to stand up the same broker, which wasn't in the
original plan and is a good reminder that "local dev environment" is
rarely as solved a problem as it looks on paper.

## What's outstanding

Everything above was verified in mock mode against the local KRaft broker.
What's explicitly *not* done yet: running the producers against the live
API-Football endpoint, since that needs an actual RapidAPI key and starts
spending the daily budget for real. That's fine — the whole point of
building mock mode first was to get the topic design, partitioning, and
envelope shape right before the budget clock starts.

Next up: what happens to these five raw topics once Spark Structured
Streaming starts consuming them — the Bronze layer, checkpointing, and the
first real Kafka-to-Delta pipeline.

## LinkedIn version

Phase 1 of my real-time football data platform: Kafka producers, and two
constraints that shaped the design more than I expected.

The football-specific one: API-Football's free tier is 100 requests/day,
~10/min. That's a hard input, not a suggestion — so every producer ships
with a documented `mock` mode reading canned JSON, and the whole pipeline
so far has been built and tested without spending a single real API call.

The not-football-specific one: Docker Hub's anonymous pull rate limit
blocked my local Kafka broker before I'd even started. Fixed it by
downloading Kafka 3.8.1 directly and running KRaft mode (no Zookeeper)
instead of depending on the Docker image.

What shipped:
- 5 Kafka topics, partition keys matched to access pattern
  (`league_id` for fixtures/standings, `fixture_id` for events/lineups/stats)
- 4 producers + a shared idempotent producer config + a common message
  envelope
- A lookahead-window filter on the lineups producer, verified by proving
  both the "skip" and "publish" cases
- A sanity-check consumer that verified partitioning end to end against
  real Kafka offsets

Full write-up on the topic design and the rate-limit budget split:
[link to Medium article]

# Ingestion

Python producers that poll API-Football and publish raw JSON payloads to
Kafka. See [Kafka topic design](kafka/topics.md) and
[data source notes](../docs/data_sources.md) for the rate-limit-aware polling
strategy.

## Layout

```
ingestion/
├── config/
│   └── settings.example.yaml   # copy to settings.yaml (gitignored) and fill in
├── kafka/
│   ├── topics.yaml              # topic definitions
│   ├── topics.md                # design rationale
│   └── create_topics.sh         # creates topics on the local broker
├── sample_data/                 # canned API-Football responses for mock mode
├── producers/
│   ├── api_football_client.py   # REST client w/ rate limiting, retries, mock mode
│   ├── kafka_producer.py         # shared producer config + envelope publish helper
│   ├── config.py                 # settings.yaml loader
│   ├── fixtures_producer.py      # -> football.fixtures.raw
│   ├── standings_producer.py     # -> football.standings.raw
│   ├── lineups_producer.py       # -> football.lineups.raw
│   └── live_events_producer.py   # -> football.events.live, football.player_stats.raw
└── consumers/
    └── sanity_check_consumer.py  # prints messages from all 5 topics (dev tool)
```

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r ingestion/requirements.txt
cp ingestion/config/settings.example.yaml ingestion/config/settings.yaml
# settings.yaml defaults to mock: true (no API key needed yet)
```

## 1. Start Kafka

```bash
cd infra && docker compose up -d        # if Docker Hub pulls aren't rate-limited
# or, if they are:
./run_local_kafka.sh                    # runs Kafka 3.8.1 directly via /opt/kafka

cd ../ingestion/kafka && ./create_topics.sh --no-docker   # (drop --no-docker for Compose)
```

## 2. Run producers (mock mode)

With `mock: true` in `settings.yaml`, the client reads canned responses from
`sample_data/` instead of calling API-Football — good for developing/testing
the Kafka pipeline without spending the free-tier daily budget.

```bash
export PYTHONPATH=$PWD/..   # repo root, so `ingestion.*` imports resolve

python3 -m ingestion.producers.fixtures_producer --once
python3 -m ingestion.producers.standings_producer --once
python3 -m ingestion.producers.live_events_producer --once
python3 -m ingestion.producers.lineups_producer --once   # only publishes if a
                                                            # tracked fixture kicks
                                                            # off within lineups_lookahead_minutes
```

Drop `--once` to run continuously on the configured poll interval.

## 3. Verify with the sanity-check consumer

```bash
python3 -m ingestion.consumers.sanity_check_consumer --max-messages 10 --timeout 15
```

Each line shows `[topic] key=... ingest_ts=... endpoint=... payload_size=...`
— confirms producers -> Kafka -> consumer end to end, and that partitioning
keys (`league_id` / `fixture_id`) are set correctly.

## Going live

Once you have a RapidAPI key for API-Football:
1. Set `api_football.rapidapi_key` in `settings.yaml` and `mock: false`
2. Set `tracked_leagues` to the league(s)/season you want
3. Run the producers as long-running processes (e.g. one terminal/process per
   producer, or wire them into `docker compose` as additional services)

## Status

✅ Phase 1 — producers, client, and sanity consumer implemented and verified
end-to-end against a local Kafka broker in mock mode. Live API-Football
testing pending a RapidAPI key.

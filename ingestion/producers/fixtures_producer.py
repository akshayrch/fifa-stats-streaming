"""Polls fixtures for each tracked league and publishes to
football.fixtures.raw, keyed by league_id.

Usage:
    python -m ingestion.producers.fixtures_producer [--once]

    --once  run a single poll cycle and exit (useful for testing/mock mode)
"""

from __future__ import annotations

import argparse
import logging
import time

from ingestion.producers.api_football_client import APIFootballClient
from ingestion.producers.config import load_settings
from ingestion.producers.kafka_producer import build_producer, publish

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "football.fixtures.raw"


def poll_once(client: APIFootballClient, producer, settings: dict) -> int:
    """Fetch fixtures for each tracked league and publish one Kafka message
    per fixture. Returns the number of messages published."""
    published = 0
    for league in settings["tracked_leagues"]:
        league_id, season = league["league_id"], league["season"]
        logger.info("Fetching fixtures for league=%s season=%s", league_id, season)

        response = client.get_fixtures(league_id, season)
        fixtures = response.get("response", [])
        logger.info("Got %d fixtures for league=%s", len(fixtures), league_id)

        for fixture in fixtures:
            publish(
                producer, TOPIC, key=league_id, payload=fixture,
                endpoint="/fixtures",
            )
            published += 1

    producer.flush()
    return published


def run(once: bool = False) -> None:
    settings = load_settings()
    client = APIFootballClient(**settings["api_football"])
    producer = build_producer(settings["kafka"]["bootstrap_servers"])
    interval = settings["producers"]["fixtures_poll_interval_seconds"]

    while True:
        count = poll_once(client, producer, settings)
        logger.info("Published %d messages to %s", count, TOPIC)
        if once:
            return
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    run(once=args.once)

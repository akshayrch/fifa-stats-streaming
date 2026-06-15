"""Polls league standings and publishes to football.standings.raw, keyed by
league_id.

Usage:
    python -m ingestion.producers.standings_producer [--once]
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

TOPIC = "football.standings.raw"


def poll_once(client: APIFootballClient, producer, settings: dict) -> int:
    """Fetch standings for each tracked league and publish one Kafka message
    per league (the full table). Returns the number of messages published."""
    published = 0
    for league in settings["tracked_leagues"]:
        league_id, season = league["league_id"], league["season"]
        logger.info("Fetching standings for league=%s season=%s", league_id, season)

        response = client.get_standings(league_id, season)
        standings = response.get("response", [])
        if not standings:
            logger.warning("No standings returned for league=%s season=%s", league_id, season)
            continue

        publish(
            producer, TOPIC, key=league_id, payload=standings[0],
            endpoint="/standings",
        )
        published += 1

    producer.flush()
    return published


def run(once: bool = False) -> None:
    settings = load_settings()
    client = APIFootballClient(**settings["api_football"])
    producer = build_producer(settings["kafka"]["bootstrap_servers"])
    interval = settings["producers"]["standings_poll_interval_seconds"]

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

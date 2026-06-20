"""Polls lineups for fixtures kicking off soon and publishes to
football.lineups.raw, keyed by fixture_id.

Usage:
    python -m ingestion.producers.lineups_producer [--once]
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone

from ingestion.producers.api_football_client import APIFootballClient
from ingestion.producers.config import load_settings
from ingestion.producers.kafka_producer import build_producer, publish

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "football.lineups.raw"


def _is_within_lookahead(fixture: dict, now: datetime, lookahead_minutes: int) -> bool:
    status = fixture["fixture"]["status"]["short"]
    if status != "NS":  # only fixtures Not yet Started have pre-match lineups
        return False
    kickoff = datetime.fromisoformat(fixture["fixture"]["date"])
    minutes_to_kickoff = (kickoff - now).total_seconds() / 60
    return 0 <= minutes_to_kickoff <= lookahead_minutes


def poll_once(client: APIFootballClient, producer, settings: dict) -> int:
    """For each tracked league, find upcoming fixtures within the configured
    lookahead window and publish their lineups (once announced). Returns the
    number of messages published."""
    published = 0
    lookahead_minutes = settings["producers"].get("lineups_lookahead_minutes", 60)
    now = datetime.now(timezone.utc)

    for league in settings["tracked_leagues"]:
        league_id, season = league["league_id"], league["season"]
        response = client.get_fixtures(league_id, season, next=10)

        for fixture in response.get("response", []):
            if not _is_within_lookahead(fixture, now, lookahead_minutes):
                continue

            fixture_id = fixture["fixture"]["id"]
            logger.info("Fetching lineups for fixture=%s", fixture_id)

            lineup_response = client.get_lineups(fixture_id)
            lineups = lineup_response.get("response", [])
            if not lineups:
                logger.info("Lineups not yet announced for fixture=%s", fixture_id)
                continue

            publish(
                producer, TOPIC, key=fixture_id, payload=lineups,
                endpoint="/fixtures/lineups",
            )
            published += 1

    producer.flush()
    return published


def run(once: bool = False) -> None:
    settings = load_settings()
    client = APIFootballClient(**settings["api_football"])
    producer = build_producer(settings["kafka"]["bootstrap_servers"])
    interval = settings["producers"]["lineups_poll_interval_seconds"]

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

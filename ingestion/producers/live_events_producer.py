"""Polls live match events + per-player stats for in-progress fixtures and
publishes to football.events.live and football.player_stats.raw, both keyed
by fixture_id.

This is the highest-frequency producer and the primary consumer of the daily
request budget on match days. See docs/data_sources.md for budgeting.

Usage:
    python -m ingestion.producers.live_events_producer [--once]
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

EVENTS_TOPIC = "football.events.live"
PLAYER_STATS_TOPIC = "football.player_stats.raw"


def poll_once(client: APIFootballClient, producer, settings: dict) -> int:
    """For each tracked league, find live fixtures and publish their current
    events + player stats. Returns the number of messages published."""
    published = 0

    for league in settings["tracked_leagues"]:
        league_id = league["league_id"]
        live = client.get_live_fixtures(league_id)
        live_fixtures = live.get("response", [])
        logger.info("league=%s has %d live fixture(s)", league_id, len(live_fixtures))

        for fixture in live_fixtures:
            fixture_id = fixture["fixture"]["id"]

            events = client.get_events(fixture_id)
            event_list = events.get("response", [])
            if event_list:
                publish(
                    producer, EVENTS_TOPIC, key=fixture_id, payload=event_list,
                    endpoint="/fixtures/events",
                )
                published += 1

            player_stats = client.get_player_stats(fixture_id)
            stats_list = player_stats.get("response", [])
            if stats_list:
                publish(
                    producer, PLAYER_STATS_TOPIC, key=fixture_id, payload=stats_list,
                    endpoint="/fixtures/players",
                )
                published += 1

    producer.flush()
    return published


def run(once: bool = False) -> None:
    settings = load_settings()
    client = APIFootballClient(**settings["api_football"])
    producer = build_producer(settings["kafka"]["bootstrap_servers"])
    interval = settings["producers"]["live_events_poll_interval_seconds"]

    while True:
        count = poll_once(client, producer, settings)
        logger.info("Published %d messages across %s / %s", count, EVENTS_TOPIC, PLAYER_STATS_TOPIC)
        if once:
            return
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    run(once=args.once)

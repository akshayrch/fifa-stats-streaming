"""Simple consumer that subscribes to all football.* topics and prints each
message's envelope metadata. Used during Phase 1 development to verify
producers are publishing correctly before wiring up Spark.

Usage:
    python -m ingestion.consumers.sanity_check_consumer [--max-messages N] [--timeout SECONDS]

    --max-messages  exit after consuming this many messages (default: run forever)
    --timeout       exit after this many seconds with no new messages (default: run forever)
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from confluent_kafka import Consumer

from ingestion.producers.config import load_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPICS = [
    "football.fixtures.raw",
    "football.events.live",
    "football.lineups.raw",
    "football.standings.raw",
    "football.player_stats.raw",
]


def run(max_messages: int | None = None, timeout: float | None = None) -> int:
    settings = load_settings()
    consumer = Consumer({
        "bootstrap.servers": settings["kafka"]["bootstrap_servers"],
        "group.id": "sanity-check",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(TOPICS)

    logger.info("Subscribed to %s. Waiting for messages...", TOPICS)
    consumed = 0
    last_message_at = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if timeout and (time.monotonic() - last_message_at) > timeout:
                    logger.info("No messages for %.0fs, exiting.", timeout)
                    return consumed
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())
            logger.info(
                "[%s] key=%s ingest_ts=%s endpoint=%s payload_size=%d",
                msg.topic(), msg.key().decode() if msg.key() else None,
                envelope.get("ingest_ts"), envelope.get("endpoint"),
                len(json.dumps(envelope.get("payload"))),
            )
            consumed += 1
            last_message_at = time.monotonic()
            if max_messages and consumed >= max_messages:
                logger.info("Reached max_messages=%d, exiting.", max_messages)
                return consumed
    except KeyboardInterrupt:
        return consumed
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=None)
    args = parser.parse_args()
    run(max_messages=args.max_messages, timeout=args.timeout)

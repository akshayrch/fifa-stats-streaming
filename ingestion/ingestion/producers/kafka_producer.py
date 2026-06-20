"""Shared Kafka producer setup and publish helper.

All messages are wrapped in a common envelope (see
ingestion/kafka/topics.md#message-envelope) before being published as JSON.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

PRODUCER_CONFIG_DEFAULTS = {
    "enable.idempotence": True,
    "acks": "all",
    "retries": 5,
    "linger.ms": 50,
    "compression.type": "zstd",
}


def build_producer(bootstrap_servers: str) -> Producer:
    config = {"bootstrap.servers": bootstrap_servers, **PRODUCER_CONFIG_DEFAULTS}
    return Producer(config)


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [%s] @ offset %s", msg.topic(), msg.partition(), msg.offset()
        )


def publish(
    producer: Producer,
    topic: str,
    key: str,
    payload: dict[str, Any],
    source: str = "api-football",
    endpoint: str = "",
) -> None:
    envelope = {
        "source": source,
        "endpoint": endpoint,
        "request_id": str(uuid.uuid4()),
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    producer.produce(
        topic=topic,
        key=str(key),
        value=json.dumps(envelope),
        callback=_delivery_report,
    )
    producer.poll(0)

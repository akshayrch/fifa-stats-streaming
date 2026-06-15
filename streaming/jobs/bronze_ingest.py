"""Phase 2 — Kafka -> Bronze Delta tables.

Reads each football.* topic as a Structured Streaming source and appends raw
records (plus Kafka metadata) to a corresponding bronze.* Delta table.
No parsing/transformation — Bronze is the replayable source of truth.

Usage:
    python streaming/jobs/bronze_ingest.py               # one-shot: process all
                                                            # currently-available
                                                            # messages, then exit
    python streaming/jobs/bronze_ingest.py --continuous   # run forever, picking
                                                            # up new messages as
                                                            # they arrive
"""

from __future__ import annotations

import argparse

from pyspark.sql.functions import col, current_timestamp

from streaming.jobs.spark_session import (
    checkpoint_path,
    get_spark,
    kafka_bootstrap_servers,
    lakehouse_path,
)

# topic -> bronze table name
TOPIC_TABLE_MAP = {
    "football.fixtures.raw": "fixtures_raw",
    "football.events.live": "events_raw",
    "football.lineups.raw": "lineups_raw",
    "football.standings.raw": "standings_raw",
    "football.player_stats.raw": "player_stats_raw",
}


def run(continuous: bool = False) -> None:
    spark = get_spark("bronze_ingest")
    queries = []

    for topic, table in TOPIC_TABLE_MAP.items():
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        bronze = raw.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_ts"),
            current_timestamp().alias("ingest_ts"),
        )

        writer = (
            bronze.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path(f"bronze_{table}"))
        )
        if not continuous:
            writer = writer.trigger(availableNow=True)

        queries.append(writer.start(lakehouse_path("bronze", table)))

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    run(continuous=args.continuous)

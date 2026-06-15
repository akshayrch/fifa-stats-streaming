"""Phase 3 — Bronze -> Silver.

Parses the raw JSON `value` column from each bronze.* table against an
explicit schema, deduplicates, and writes/merges into conformed Silver
dimension and fact tables (see medallion/README.md for the target schemas).

This job runs as a micro-batch (`trigger(availableNow=True)`) so it can be
scheduled (cron/Airflow) rather than running continuously like bronze_ingest.

Usage:
    python streaming/jobs/silver_transform.py
"""

from __future__ import annotations

from pyspark.sql.functions import col, from_json

from streaming.jobs.spark_session import checkpoint_path, get_spark, lakehouse_path

# TODO (Phase 3): define explicit StructType schemas for each payload shape
# returned by API-Football, e.g.:
#
# FIXTURE_SCHEMA = StructType([...])
# EVENT_SCHEMA = StructType([...])
# LINEUP_SCHEMA = StructType([...])
# STANDINGS_SCHEMA = StructType([...])
# PLAYER_STATS_SCHEMA = StructType([...])


def transform_fixtures(spark) -> None:
    bronze = spark.readStream.format("delta").load(lakehouse_path("bronze", "fixtures_raw"))

    # TODO (Phase 3):
    #   1. parse `value` (JSON envelope -> payload) with FIXTURE_SCHEMA
    #   2. select/rename columns to match silver.fact_match + silver.dim_team
    #      (see medallion/README.md)
    #   3. deduplicate on fixture_id, keeping latest ingest_ts
    #   4. MERGE into silver.fact_match (upsert) using foreachBatch

    parsed = bronze.select(
        from_json(col("value"), "ingest_ts STRING, payload STRING").alias("envelope")
    )

    (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("silver_fact_match"))
        .trigger(availableNow=True)
        .start(lakehouse_path("silver", "_fact_match_staging"))
    )


# TODO (Phase 3): transform_events, transform_lineups, transform_standings,
# transform_player_stats -- same pattern, target tables per medallion/README.md


def run() -> None:
    spark = get_spark("silver_transform")
    transform_fixtures(spark)
    # transform_events(spark)
    # transform_lineups(spark)
    # transform_standings(spark)
    # transform_player_stats(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()

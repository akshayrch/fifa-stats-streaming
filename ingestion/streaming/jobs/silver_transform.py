"""Phase 3 — Bronze -> Silver.

Parses raw JSON from each bronze.* table, deduplicates on natural keys, and
upserts (Delta MERGE) into conformed Silver dimension and fact tables.

Payload shapes (set by the Phase 1 producers):
  fixtures_raw    : one Kafka message per fixture — payload IS a fixture item
                    {"fixture":{...},"league":{...},"teams":{...},"goals":{...}}
  standings_raw   : one message per league — payload IS one standings response item
                    {"league":{"id":...,"standings":[[...]]}}
  events_raw      : one message per fixture — payload IS a list of event dicts
                    [{time, team, player, type, ...}, ...]
  player_stats_raw: one message per fixture — payload IS a list of team-player blocks
                    [{"team":{...},"players":[...]}, ...]
  lineups_raw     : one message per fixture — payload IS a list of lineup items
                    [{"team":{...},"formation":"...","startXI":[...]}, ...]

Usage:
    python streaming/jobs/silver_transform.py               # one-shot
    python streaming/jobs/silver_transform.py --continuous  # run forever
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, concat_ws, current_date, explode, from_json, md5, to_timestamp,
)
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, LongType,
    StringType, StructField, StructType,
)

from streaming.jobs.spark_session import checkpoint_path, get_spark, lakehouse_path


# ---------------------------------------------------------------------------
# Common envelope schema
# ---------------------------------------------------------------------------

ENVELOPE_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("endpoint", StringType()),
    StructField("request_id", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("payload", StringType()),   # inner JSON as opaque string
])

# ---------------------------------------------------------------------------
# Sub-schemas shared across multiple payloads
# ---------------------------------------------------------------------------

_STATUS = StructType([
    StructField("long", StringType()),
    StructField("short", StringType()),
    StructField("elapsed", IntegerType()),
])
_LEAGUE_INFO = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("country", StringType()),
    StructField("season", IntegerType()),
    StructField("round", StringType()),
])
_TEAM_REF = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("winner", BooleanType()),
])
_GOALS = StructType([StructField("home", IntegerType()), StructField("away", IntegerType())])
_PREF = StructType([StructField("id", IntegerType()), StructField("name", StringType())])

# ---------------------------------------------------------------------------
# Payload schemas — each matches what the producer actually puts in "payload"
# ---------------------------------------------------------------------------

# fixtures_raw: payload = one fixture item (not an array)
FIXTURE_ITEM_SCHEMA = StructType([
    StructField("fixture", StructType([
        StructField("id", LongType()),
        StructField("referee", StringType()),
        StructField("date", StringType()),
        StructField("timestamp", LongType()),
        StructField("status", _STATUS),
    ])),
    StructField("league", _LEAGUE_INFO),
    StructField("teams", StructType([
        StructField("home", _TEAM_REF),
        StructField("away", _TEAM_REF),
    ])),
    StructField("goals", _GOALS),
])

# standings_raw: payload = one standings response item {"league": {...}}
_STD_GOALS = StructType([StructField("for", IntegerType()), StructField("against", IntegerType())])
_STD_ALL = StructType([
    StructField("played", IntegerType()),
    StructField("win", IntegerType()),
    StructField("draw", IntegerType()),
    StructField("lose", IntegerType()),
    StructField("goals", _STD_GOALS),
])
_STD_ENTRY = StructType([
    StructField("rank", IntegerType()),
    StructField("team", StructType([StructField("id", IntegerType()), StructField("name", StringType())])),
    StructField("points", IntegerType()),
    StructField("goalsDiff", IntegerType()),
    StructField("all", _STD_ALL),
])
_STD_LEAGUE = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("country", StringType()),
    StructField("season", IntegerType()),
    StructField("standings", ArrayType(ArrayType(_STD_ENTRY))),
])
STANDINGS_ITEM_SCHEMA = StructType([StructField("league", _STD_LEAGUE)])

# events_raw: payload = list of event dicts
_EVENT_ITEM = StructType([
    StructField("time", StructType([StructField("elapsed", IntegerType()), StructField("extra", IntegerType())])),
    StructField("team", _PREF),
    StructField("player", _PREF),
    StructField("assist", _PREF),
    StructField("type", StringType()),
    StructField("detail", StringType()),
    StructField("comments", StringType()),
])
EVENTS_ARRAY_SCHEMA = ArrayType(_EVENT_ITEM)

# player_stats_raw: payload = list of {team, players} blocks
_STAT = StructType([
    StructField("games", StructType([
        StructField("minutes", IntegerType()),
        StructField("position", StringType()),
        StructField("rating", StringType()),
    ])),
    StructField("goals", StructType([StructField("total", IntegerType()), StructField("assists", IntegerType())])),
    StructField("shots", StructType([StructField("total", IntegerType()), StructField("on", IntegerType())])),
    StructField("passes", StructType([StructField("total", IntegerType()), StructField("accuracy", IntegerType())])),
])
_PLAYER_ENTRY = StructType([
    StructField("player", _PREF),
    StructField("statistics", ArrayType(_STAT)),
])
PLAYER_STATS_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("team", _PREF),
    StructField("players", ArrayType(_PLAYER_ENTRY)),
]))

# lineups_raw: payload = list of {team, formation, startXI, coach} items
_LINEUP_PLAYER = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("number", IntegerType()),
    StructField("pos", StringType()),
    StructField("grid", StringType()),
])
LINEUPS_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("team", _PREF),
    StructField("formation", StringType()),
    StructField("startXI", ArrayType(StructType([StructField("player", _LINEUP_PLAYER)]))),
    StructField("coach", _PREF),
]))


# ---------------------------------------------------------------------------
# Delta merge helper
# ---------------------------------------------------------------------------

def _merge(spark, df: DataFrame, table_path: str, merge_cond: str) -> None:
    """Upsert df into a Delta table; create on first run if it doesn't exist yet."""
    if DeltaTable.isDeltaTable(spark, table_path):
        (
            DeltaTable.forPath(spark, table_path).alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(table_path)


# ---------------------------------------------------------------------------
# foreachBatch processors
# ---------------------------------------------------------------------------

def _fixtures_batch(batch_df: DataFrame, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    parsed = (
        batch_df
        .withColumn("env", from_json(col("value"), ENVELOPE_SCHEMA))
        .withColumn("item", from_json(col("env.payload"), FIXTURE_ITEM_SCHEMA))
        .cache()
    )

    fact_match = parsed.select(
        col("item.fixture.id").cast("long").alias("fixture_id"),
        col("item.league.id").alias("league_id"),
        col("item.league.season").alias("season"),
        col("item.teams.home.id").alias("home_team_id"),
        col("item.teams.away.id").alias("away_team_id"),
        to_timestamp(col("item.fixture.date")).alias("kickoff_ts"),
        col("item.fixture.status.short").alias("status"),
        col("item.fixture.status.elapsed").alias("elapsed_minutes"),
        col("item.goals.home").alias("home_goals"),
        col("item.goals.away").alias("away_goals"),
        col("item.league.round").alias("round"),
        col("ingest_ts"),
    ).dropDuplicates(["fixture_id"])
    _merge(spark, fact_match, lakehouse_path("silver", "fact_match"),
           "t.fixture_id = s.fixture_id")

    home_teams = parsed.select(col("item.teams.home.id").alias("team_id"),
                               col("item.teams.home.name").alias("name"),
                               col("ingest_ts").alias("updated_ts"))
    away_teams = parsed.select(col("item.teams.away.id").alias("team_id"),
                               col("item.teams.away.name").alias("name"),
                               col("ingest_ts").alias("updated_ts"))
    dim_team = home_teams.union(away_teams).dropDuplicates(["team_id"])
    _merge(spark, dim_team, lakehouse_path("silver", "dim_team"),
           "t.team_id = s.team_id")

    dim_league = parsed.select(
        col("item.league.id").alias("league_id"),
        col("item.league.season").alias("season"),
        col("item.league.name").alias("name"),
        col("item.league.country").alias("country"),
        col("ingest_ts").alias("updated_ts"),
    ).dropDuplicates(["league_id", "season"])
    _merge(spark, dim_league, lakehouse_path("silver", "dim_league"),
           "t.league_id = s.league_id AND t.season = s.season")

    parsed.unpersist()


def _standings_batch(batch_df: DataFrame, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    # payload = one standings response item: {"league": {..., "standings": [[...]]}}
    parsed = (
        batch_df
        .withColumn("env", from_json(col("value"), ENVELOPE_SCHEMA))
        .withColumn("resp", from_json(col("env.payload"), STANDINGS_ITEM_SCHEMA))
        .withColumn("grp", explode(col("resp.league.standings")))
        .withColumn("entry", explode(col("grp")))
        .cache()
    )
    snapshot = parsed.select(
        col("resp.league.id").alias("league_id"),
        col("resp.league.season").alias("season"),
        col("entry.team.id").alias("team_id"),
        col("entry.team.name").alias("team_name"),
        current_date().alias("snapshot_date"),
        col("entry.rank").alias("rank"),
        col("entry.points").alias("points"),
        col("entry.all.played").alias("played"),
        col("entry.all.win").alias("won"),
        col("entry.all.draw").alias("draw"),
        col("entry.all.lose").alias("lost"),
        col("entry.all.goals.for").alias("gf"),
        col("entry.all.goals.against").alias("ga"),
        col("entry.goalsDiff").alias("gd"),
        col("ingest_ts"),
    ).dropDuplicates(["league_id", "season", "team_id", "snapshot_date"])
    _merge(spark, snapshot, lakehouse_path("silver", "fact_standings_snapshot"),
           "t.league_id = s.league_id AND t.season = s.season "
           "AND t.team_id = s.team_id AND t.snapshot_date = s.snapshot_date")
    parsed.unpersist()


def _events_batch(batch_df: DataFrame, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    # payload = list of event dicts; explode the parsed array
    parsed = (
        batch_df
        .withColumn("env", from_json(col("value"), ENVELOPE_SCHEMA))
        .withColumn("event", explode(from_json(col("env.payload"), EVENTS_ARRAY_SCHEMA)))
        .cache()
    )
    events = parsed.select(
        col("key").cast("long").alias("fixture_id"),
        col("event.time.elapsed").alias("minute"),
        col("event.time.extra").alias("extra_minute"),
        col("event.team.id").alias("team_id"),
        col("event.player.id").alias("player_id"),
        col("event.player.name").alias("player_name"),
        col("event.assist.id").alias("assist_player_id"),
        col("event.type").alias("type"),
        col("event.detail").alias("detail"),
        col("ingest_ts"),
    ).withColumn(
        "event_id",
        md5(concat_ws("|", col("fixture_id"), col("minute"),
                      col("player_id"), col("type"), col("detail")))
    ).dropDuplicates(["event_id"])
    _merge(spark, events, lakehouse_path("silver", "fact_match_event"),
           "t.event_id = s.event_id")
    parsed.unpersist()


def _player_stats_batch(batch_df: DataFrame, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    # payload = list of {team, players} blocks; explode twice
    parsed = (
        batch_df
        .withColumn("env", from_json(col("value"), ENVELOPE_SCHEMA))
        .withColumn("team_block", explode(from_json(col("env.payload"), PLAYER_STATS_ARRAY_SCHEMA)))
        .withColumn("player_entry", explode(col("team_block.players")))
        .withColumn("stat", col("player_entry.statistics")[0])
        .cache()
    )
    stats = parsed.select(
        col("key").cast("long").alias("fixture_id"),
        col("player_entry.player.id").alias("player_id"),
        col("player_entry.player.name").alias("player_name"),
        col("team_block.team.id").alias("team_id"),
        col("stat.games.position").alias("position"),
        col("stat.games.minutes").alias("minutes"),
        col("stat.games.rating").cast("double").alias("rating"),
        col("stat.goals.total").alias("goals"),
        col("stat.goals.assists").alias("assists"),
        col("stat.shots.total").alias("shots_total"),
        col("stat.shots.on").alias("shots_on"),
        col("stat.passes.total").alias("passes_total"),
        col("stat.passes.accuracy").alias("passes_accuracy"),
        col("ingest_ts"),
    ).dropDuplicates(["fixture_id", "player_id"])
    _merge(spark, stats, lakehouse_path("silver", "fact_player_match_stat"),
           "t.fixture_id = s.fixture_id AND t.player_id = s.player_id")
    parsed.unpersist()


def _lineups_batch(batch_df: DataFrame, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    # payload = list of lineup items; explode lineup list, then startXI
    parsed = (
        batch_df
        .withColumn("env", from_json(col("value"), ENVELOPE_SCHEMA))
        .withColumn("lineup", explode(from_json(col("env.payload"), LINEUPS_ARRAY_SCHEMA)))
        .withColumn("pw", explode(col("lineup.startXI")))
        .cache()
    )
    dim_player = parsed.select(
        col("pw.player.id").alias("player_id"),
        col("pw.player.name").alias("name"),
        col("pw.player.pos").alias("position"),
        col("pw.player.number").alias("shirt_number"),
        col("lineup.team.id").alias("team_id"),
        col("ingest_ts").alias("updated_ts"),
    ).dropDuplicates(["player_id"])
    _merge(spark, dim_player, lakehouse_path("silver", "dim_player"),
           "t.player_id = s.player_id")
    parsed.unpersist()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(continuous: bool = False) -> None:
    spark = get_spark("silver_transform")

    # Delete stale Silver checkpoints so the job re-reads from Bronze offset 0.
    # Safe because Silver tables use MERGE (idempotent on natural keys).
    import shutil, os
    ckpt_base = lakehouse_path("", "_checkpoints").replace("file://", "")
    for ckpt in ["silver_fixtures", "silver_standings", "silver_events",
                 "silver_player_stats", "silver_lineups"]:
        path = os.path.join(ckpt_base, ckpt)
        if os.path.exists(path):
            shutil.rmtree(path)

    queries = []
    sources = [
        ("bronze", "fixtures_raw",     _fixtures_batch,     "silver_fixtures"),
        ("bronze", "standings_raw",    _standings_batch,    "silver_standings"),
        ("bronze", "events_raw",       _events_batch,       "silver_events"),
        ("bronze", "player_stats_raw", _player_stats_batch, "silver_player_stats"),
        ("bronze", "lineups_raw",      _lineups_batch,      "silver_lineups"),
    ]

    for layer, table, fn, ckpt in sources:
        writer = (
            spark.readStream.format("delta")
            .load(lakehouse_path(layer, table))
            .writeStream.foreachBatch(fn)
            .option("checkpointLocation", checkpoint_path(ckpt))
        )
        if not continuous:
            writer = writer.trigger(availableNow=True)
        queries.append(writer.start())

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    run(continuous=args.continuous)

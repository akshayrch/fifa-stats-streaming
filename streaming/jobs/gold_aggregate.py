"""Phase 3 — Silver -> Gold.

Batch job (run on a schedule, e.g. via Airflow) that computes feature and
aggregate tables from Silver Delta tables. All functions are idempotent —
overwrite the Gold table on each run so partial failures can be safely retried.

Usage:
    python streaming/jobs/gold_aggregate.py
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, greatest, least, lit, round as spark_round,
    row_number, sum as spark_sum, when,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

from streaming.jobs.spark_session import get_spark, lakehouse_path

ELO_K = 32
ELO_BASE = 1500.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(spark: SparkSession, layer: str, table: str):
    return spark.read.format("delta").load(lakehouse_path(layer, table))


def _write_gold(df, table: str) -> None:
    df.write.format("delta").mode("overwrite").save(lakehouse_path("gold", table))


# ---------------------------------------------------------------------------
# Gold builders
# ---------------------------------------------------------------------------

def build_team_form_features(spark: SparkSession) -> None:
    """Rolling last-5 / last-10 form (PPG, GF/GA) per team, as of each match."""
    fact_match = _read(spark, "silver", "fact_match")
    finished = fact_match.filter(col("status") == "FT")

    if finished.count() == 0:
        print("[gold] No finished matches — skipping team_form_features")
        return

    # Unpivot: one row per team per match (home perspective + away perspective)
    home = finished.select(
        col("fixture_id"),
        col("home_team_id").alias("team_id"),
        col("kickoff_ts"),
        lit("H").alias("venue"),
        col("home_goals").alias("gf"),
        col("away_goals").alias("ga"),
        when(col("home_goals") > col("away_goals"), 3)
        .when(col("home_goals") == col("away_goals"), 1)
        .otherwise(0).alias("points"),
    )
    away = finished.select(
        col("fixture_id"),
        col("away_team_id").alias("team_id"),
        col("kickoff_ts"),
        lit("A").alias("venue"),
        col("away_goals").alias("gf"),
        col("home_goals").alias("ga"),
        when(col("away_goals") > col("home_goals"), 3)
        .when(col("away_goals") == col("home_goals"), 1)
        .otherwise(0).alias("points"),
    )
    team_matches = home.union(away)

    w5 = Window.partitionBy("team_id").orderBy("kickoff_ts").rowsBetween(-4, 0)
    w10 = Window.partitionBy("team_id").orderBy("kickoff_ts").rowsBetween(-9, 0)

    form = team_matches.select(
        col("team_id"),
        col("fixture_id").alias("as_of_fixture_id"),
        col("kickoff_ts").alias("as_of_ts"),
        spark_round(avg("points").over(w5), 2).alias("ppg_last5"),
        spark_round(avg("gf").over(w5), 2).alias("avg_gf_last5"),
        spark_round(avg("ga").over(w5), 2).alias("avg_ga_last5"),
        spark_sum("points").over(w5).alias("pts_last5"),
        spark_round(avg("points").over(w10), 2).alias("ppg_last10"),
        spark_round(avg("gf").over(w10), 2).alias("avg_gf_last10"),
        spark_round(avg("ga").over(w10), 2).alias("avg_ga_last10"),
        spark_sum("points").over(w10).alias("pts_last10"),
    )
    _write_gold(form, "team_form_features")
    print(f"[gold] team_form_features: {form.count()} rows")


def build_elo_ratings(spark: SparkSession) -> None:
    """ELO rating per team after each finished match (chronological, driver-side).

    ELO formula:
        expected_A = 1 / (1 + 10^((rating_B - rating_A) / 400))
        new_rating_A = rating_A + K * (actual_A - expected_A)   K=32
    """
    fact_match = _read(spark, "silver", "fact_match")
    finished = (
        fact_match.filter(col("status") == "FT")
        .orderBy("kickoff_ts")
        .select("fixture_id", "home_team_id", "away_team_id",
                "home_goals", "away_goals", "kickoff_ts")
        .collect()
    )

    if not finished:
        print("[gold] No finished matches — skipping elo_ratings")
        return

    ratings: dict[int, float] = {}
    elo_rows: list[tuple] = []

    for row in finished:
        h, a = row["home_team_id"], row["away_team_id"]
        r_h = ratings.get(h, ELO_BASE)
        r_a = ratings.get(a, ELO_BASE)

        exp_h = 1.0 / (1.0 + 10.0 ** ((r_a - r_h) / 400.0))
        exp_a = 1.0 - exp_h

        hg, ag = row["home_goals"] or 0, row["away_goals"] or 0
        if hg > ag:
            act_h, act_a = 1.0, 0.0
        elif hg == ag:
            act_h = act_a = 0.5
        else:
            act_h, act_a = 0.0, 1.0

        new_h = r_h + ELO_K * (act_h - exp_h)
        new_a = r_a + ELO_K * (act_a - exp_a)
        ratings[h], ratings[a] = new_h, new_a

        ts = row["kickoff_ts"]
        elo_rows.append((row["fixture_id"], h, round(r_h, 2), round(new_h, 2), ts))
        elo_rows.append((row["fixture_id"], a, round(r_a, 2), round(new_a, 2), ts))

    schema = StructType([
        StructField("fixture_id", LongType()),
        StructField("team_id", IntegerType()),
        StructField("elo_before", DoubleType()),
        StructField("elo_after", DoubleType()),
        StructField("as_of_ts", TimestampType()),
    ])
    elo_df = spark.createDataFrame(elo_rows, schema=schema)
    _write_gold(elo_df, "elo_ratings")
    print(f"[gold] elo_ratings: {len(elo_rows)} rows, {len(ratings)} teams rated")


def build_head_to_head_features(spark: SparkSession) -> None:
    """Historical H2H record per ordered team pair (lower team_id = team_a)."""
    fact_match = _read(spark, "silver", "fact_match")
    finished = fact_match.filter(col("status") == "FT")

    if finished.count() == 0:
        print("[gold] No finished matches — skipping head_to_head_features")
        return

    h2h_base = finished.select(
        least(col("home_team_id"), col("away_team_id")).alias("team_a_id"),
        greatest(col("home_team_id"), col("away_team_id")).alias("team_b_id"),
        when(col("home_goals") > col("away_goals"), col("home_team_id"))
        .when(col("home_goals") < col("away_goals"), col("away_team_id"))
        .otherwise(lit(None).cast(IntegerType())).alias("winner_id"),
    )
    h2h = h2h_base.groupBy("team_a_id", "team_b_id").agg(
        count("*").alias("matches_played"),
        spark_sum(when(col("winner_id") == col("team_a_id"), 1).otherwise(0)).alias("team_a_wins"),
        spark_sum(when(col("winner_id") == col("team_b_id"), 1).otherwise(0)).alias("team_b_wins"),
        spark_sum(when(col("winner_id").isNull(), 1).otherwise(0)).alias("draws"),
    )
    _write_gold(h2h, "head_to_head_features")
    print(f"[gold] head_to_head_features: {h2h.count()} pairs")


def build_player_season_stats(spark: SparkSession) -> None:
    """Aggregated per-player stats by season, joined with fact_match for season."""
    try:
        pms = _read(spark, "silver", "fact_player_match_stat")
        fm = _read(spark, "silver", "fact_match")
    except Exception as e:
        print(f"[gold] Skipping player_season_stats — {e}")
        return

    player_with_season = pms.join(
        fm.select("fixture_id", "season", "league_id"), on="fixture_id", how="left"
    )
    season_stats = player_with_season.groupBy(
        "player_id", "player_name", "team_id", "season"
    ).agg(
        count("fixture_id").alias("appearances"),
        spark_sum("goals").alias("goals"),
        spark_sum("assists").alias("assists"),
        spark_sum("minutes").alias("minutes_played"),
        spark_round(avg("rating"), 2).alias("avg_rating"),
        spark_sum("shots_total").alias("shots_total"),
        spark_sum("shots_on").alias("shots_on_target"),
        spark_sum("passes_total").alias("passes_total"),
    )
    _write_gold(season_stats, "player_season_stats")
    print(f"[gold] player_season_stats: {season_stats.count()} rows")


def build_match_prediction_features(spark: SparkSession) -> None:
    """Feature row per upcoming fixture: ELO diff + last-5 form for each side."""
    try:
        fact_match = _read(spark, "silver", "fact_match")
        elo = _read(spark, "gold", "elo_ratings")
        form = _read(spark, "gold", "team_form_features")
    except Exception as e:
        print(f"[gold] Skipping match_prediction_features — {e}")
        return

    upcoming = fact_match.filter(col("status") != "FT")
    if upcoming.count() == 0:
        print("[gold] No upcoming matches — skipping match_prediction_features")
        return

    # Latest ELO and form per team (most recent fixture)
    elo_w = Window.partitionBy("team_id").orderBy(col("as_of_ts").desc())
    latest_elo = elo.withColumn("rn", row_number().over(elo_w)).filter(col("rn") == 1)

    form_w = Window.partitionBy("team_id").orderBy(col("as_of_ts").desc())
    latest_form = form.withColumn("rn", row_number().over(form_w)).filter(col("rn") == 1)

    # Rename before joining to avoid column ambiguity
    home_elo = latest_elo.select(
        col("team_id").alias("_h_elo_tid"), col("elo_after").alias("home_elo"))
    away_elo = latest_elo.select(
        col("team_id").alias("_a_elo_tid"), col("elo_after").alias("away_elo"))
    home_form = latest_form.select(
        col("team_id").alias("_h_frm_tid"),
        col("ppg_last5").alias("home_ppg_last5"),
        col("avg_gf_last5").alias("home_avg_gf_last5"),
    )
    away_form = latest_form.select(
        col("team_id").alias("_a_frm_tid"),
        col("ppg_last5").alias("away_ppg_last5"),
        col("avg_gf_last5").alias("away_avg_gf_last5"),
    )

    features = (
        upcoming
        .join(home_elo, upcoming["home_team_id"] == col("_h_elo_tid"), "left")
        .join(away_elo, upcoming["away_team_id"] == col("_a_elo_tid"), "left")
        .join(home_form, upcoming["home_team_id"] == col("_h_frm_tid"), "left")
        .join(away_form, upcoming["away_team_id"] == col("_a_frm_tid"), "left")
        .select(
            upcoming["fixture_id"],
            upcoming["league_id"],
            upcoming["season"],
            upcoming["home_team_id"],
            upcoming["away_team_id"],
            upcoming["kickoff_ts"],
            col("home_elo"),
            col("away_elo"),
            spark_round(col("home_elo") - col("away_elo"), 2).alias("elo_diff"),
            col("home_ppg_last5"),
            col("away_ppg_last5"),
            col("home_avg_gf_last5"),
            col("away_avg_gf_last5"),
        )
    )
    _write_gold(features, "match_prediction_features")
    print(f"[gold] match_prediction_features: {features.count()} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    spark = get_spark("gold_aggregate")
    build_team_form_features(spark)
    build_elo_ratings(spark)
    build_head_to_head_features(spark)
    build_player_season_stats(spark)
    build_match_prediction_features(spark)
    print("[gold] All done.")


if __name__ == "__main__":
    run()

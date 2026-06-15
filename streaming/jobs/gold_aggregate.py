"""Phase 3/4 — Silver -> Gold.

Batch job (intended to run on a schedule, e.g. via Airflow) that computes the
feature/aggregate tables consumed by the 3 ML apps. See medallion/README.md
for table definitions.

Usage:
    python streaming/jobs/gold_aggregate.py
"""

from __future__ import annotations

from streaming.jobs.spark_session import get_spark, lakehouse_path


def build_team_form_features(spark) -> None:
    """gold.team_form_features: rolling last-5/10 form (PPG, GF/GA, home/away
    splits) per team, as of each match date.

    TODO (Phase 3):
      - read silver.fact_match
      - compute, per team per date, rolling aggregates over the last 5/10
        matches (use a window function ordered by kickoff_ts, partitioned by
        team_id)
      - write to gold.team_form_features
    """
    spark.read.format("delta").load(lakehouse_path("silver", "fact_match"))


def build_elo_ratings(spark) -> None:
    """gold.elo_ratings: incrementally updated ELO rating per team after each
    result.

    TODO (Phase 3):
      - read silver.fact_match ordered by kickoff_ts
      - iteratively (or via a UDF/foreachPartition) apply the ELO update
        formula after each match
      - write to gold.elo_ratings
    """


def build_head_to_head_features(spark) -> None:
    """gold.head_to_head_features: historical H2H record per team pair,
    venue-adjusted.

    TODO (Phase 3): self-join silver.fact_match on team pairs, aggregate
    historical results.
    """


def build_player_season_stats(spark) -> None:
    """gold.player_season_stats: aggregated per-player stats + recent-form
    trend, feeds App 1 (Squad Optimizer).

    TODO (Phase 5): aggregate silver.fact_player_match_stat by player+season,
    plus a rolling recent-form window.
    """


def build_match_prediction_features(spark) -> None:
    """gold.match_prediction_features: joined feature row per upcoming
    fixture, feeds App 2 (Match Odds Predictor).

    TODO (Phase 4): join team_form_features, elo_ratings,
    head_to_head_features for each upcoming fixture in silver.fact_match.
    """


def run() -> None:
    spark = get_spark("gold_aggregate")
    build_team_form_features(spark)
    build_elo_ratings(spark)
    build_head_to_head_features(spark)
    build_player_season_stats(spark)
    build_match_prediction_features(spark)


if __name__ == "__main__":
    run()

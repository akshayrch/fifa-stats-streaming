"""Feature schema + Gold-layer lookups shared by train.py and predict.py.

FEATURE_COLUMNS matches the columns gold_aggregate.build_match_prediction_features
produces (streaming/jobs/gold_aggregate.py), minus the join keys — so a model
trained on synthetic_data.py's output drops in against real Gold data with no
retraining once enough live fixtures accumulate.
"""

from __future__ import annotations

FEATURE_COLUMNS = [
    "elo_diff",
    "home_ppg_last5",
    "away_ppg_last5",
    "home_avg_gf_last5",
    "away_avg_gf_last5",
]
LABEL_COL = "result"
CLASS_LABELS = ["H", "D", "A"]  # home win / draw / away win

# Defaults used when a team has no Gold history yet (new/unseen team) —
# matches synthetic_data.py's cold-start values so the feature distribution
# the model sees at inference time matches what it was trained on.
DEFAULT_ELO = 1500.0
DEFAULT_PPG_LAST5 = 1.0
DEFAULT_AVG_GF_LAST5 = 1.2


def latest_team_stats(spark) -> dict[int, dict[str, float]]:
    """Latest ELO + rolling form per team_id from the real Gold tables.

    Returns {team_id: {"elo": ..., "ppg_last5": ..., "avg_gf_last5": ...}}.
    Used by predict.py to assemble a feature row for any two team IDs found
    in the (currently small) real lakehouse.
    """
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    from streaming.jobs.spark_session import lakehouse_path

    stats: dict[int, dict[str, float]] = {}

    try:
        elo = spark.read.format("delta").load(lakehouse_path("gold", "elo_ratings"))
        w = Window.partitionBy("team_id").orderBy(col("as_of_ts").desc())
        latest_elo = (
            elo.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("team_id", "elo_after")
            .collect()
        )
        for r in latest_elo:
            stats.setdefault(r["team_id"], {})["elo"] = float(r["elo_after"])
    except Exception:
        pass

    try:
        form = spark.read.format("delta").load(lakehouse_path("gold", "team_form_features"))
        w = Window.partitionBy("team_id").orderBy(col("as_of_ts").desc())
        latest_form = (
            form.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("team_id", "ppg_last5", "avg_gf_last5")
            .collect()
        )
        for r in latest_form:
            entry = stats.setdefault(r["team_id"], {})
            entry["ppg_last5"] = float(r["ppg_last5"])
            entry["avg_gf_last5"] = float(r["avg_gf_last5"])
    except Exception:
        pass

    return stats


def build_feature_row(
    home_id: int,
    away_id: int,
    team_stats: dict[int, dict[str, float]],
    home_elo_offset: float = 0.0,
    away_elo_offset: float = 0.0,
) -> dict[str, float]:
    """Assemble one feature row for a home/away matchup from looked-up stats,
    falling back to cold-start defaults for teams with no Gold history.

    The elo_offset args let callers (e.g. the squad optimizer) nudge a team's
    effective ELO up/down to reflect a specific lineup being stronger/weaker
    than its historical average, without touching the stored Gold rating.
    """
    home = team_stats.get(home_id, {})
    away = team_stats.get(away_id, {})

    home_elo = home.get("elo", DEFAULT_ELO) + home_elo_offset
    away_elo = away.get("elo", DEFAULT_ELO) + away_elo_offset

    return {
        "elo_diff": round(home_elo - away_elo, 2),
        "home_ppg_last5": home.get("ppg_last5", DEFAULT_PPG_LAST5),
        "away_ppg_last5": away.get("ppg_last5", DEFAULT_PPG_LAST5),
        "home_avg_gf_last5": home.get("avg_gf_last5", DEFAULT_AVG_GF_LAST5),
        "away_avg_gf_last5": away.get("avg_gf_last5", DEFAULT_AVG_GF_LAST5),
    }


def resolve_team_id(spark, name_or_id: str) -> int | None:
    """Resolve a team name (case-insensitive substring) or numeric ID against
    silver.dim_team. Returns None if nothing matches."""
    from pyspark.sql.functions import col, lower

    from streaming.jobs.spark_session import lakehouse_path

    if name_or_id.strip().lstrip("-").isdigit():
        return int(name_or_id)

    dim_team = spark.read.format("delta").load(lakehouse_path("silver", "dim_team"))
    match = (
        dim_team.filter(lower(col("name")).contains(name_or_id.strip().lower()))
        .collect()
    )
    if not match:
        return None
    return int(match[0]["team_id"])

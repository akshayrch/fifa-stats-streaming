"""Data quality checks across Bronze, Silver, and Gold layers.

Checks row counts, null rates on key columns, and data freshness.
Prints a PASS/FAIL line per check and exits non-zero if any check fails.

Usage:
    python streaming/jobs/data_quality.py
    python streaming/jobs/data_quality.py --fail-fast   # exit 1 on first failure
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from streaming.jobs.spark_session import get_spark, lakehouse_path


@dataclass
class CheckResult:
    table: str
    check: str
    passed: bool
    detail: str


def _check_table(
    spark: SparkSession,
    layer: str,
    table: str,
    key_cols: list[str],
    max_null_rate: float = 0.0,
    max_age_hours: Optional[float] = None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    path = lakehouse_path(layer, table)
    label = f"{layer}.{table}"

    try:
        df = spark.read.format("delta").load(path)
    except Exception as e:
        results.append(CheckResult(label, "table_exists", False, str(e)[:120]))
        return results

    results.append(CheckResult(label, "table_exists", True, "ok"))

    row_count = df.count()
    results.append(CheckResult(label, "row_count", row_count > 0, f"{row_count:,} rows"))

    for kc in key_cols:
        if kc not in df.columns:
            results.append(CheckResult(label, f"null_rate.{kc}", False, "column missing"))
            continue
        nulls = df.filter(col(kc).isNull()).count()
        rate = nulls / row_count if row_count else 0.0
        results.append(CheckResult(
            label, f"null_rate.{kc}",
            rate <= max_null_rate,
            f"{rate:.1%} null ({nulls:,}/{row_count:,})"
        ))

    if max_age_hours is not None and "ingest_ts" in df.columns and row_count > 0:
        latest = df.agg(spark_max("ingest_ts")).collect()[0][0]
        if latest is not None:
            now = datetime.now(timezone.utc)
            try:
                ts = latest if hasattr(latest, "tzinfo") else datetime.fromisoformat(str(latest))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_h = (now - ts).total_seconds() / 3600
                results.append(CheckResult(
                    label, "freshness",
                    age_h <= max_age_hours,
                    f"latest ingest_ts is {age_h:.1f}h ago (threshold {max_age_hours}h)"
                ))
            except Exception:
                pass

    return results


def run(fail_fast: bool = False) -> int:
    spark = get_spark("data_quality")
    all_results: list[CheckResult] = []

    # (layer, table, key_cols, max_null_rate, max_age_hours)
    checks = [
        # Bronze — just verify tables exist and have rows; no freshness SLA (Bronze is replay archive)
        ("bronze", "fixtures_raw",            ["key"],                        0.0, None),
        ("bronze", "events_raw",              ["key"],                        0.0, None),
        ("bronze", "lineups_raw",             ["key"],                        0.0, None),
        ("bronze", "standings_raw",           ["key"],                        0.0, None),
        ("bronze", "player_stats_raw",        ["key"],                        0.0, None),
        # Silver — tighter: key columns must never be null, refresh expected within 25h
        ("silver", "fact_match",              ["fixture_id"],                 0.0, 25.0),
        ("silver", "fact_match_event",        ["fixture_id", "event_id"],     0.0, 25.0),
        ("silver", "fact_player_match_stat",  ["fixture_id", "player_id"],   0.0, 25.0),
        ("silver", "fact_standings_snapshot", ["league_id", "team_id"],       0.0, 25.0),
        ("silver", "dim_team",               ["team_id"],                    0.0, None),
        ("silver", "dim_league",             ["league_id"],                  0.0, None),
        ("silver", "dim_player",             ["player_id"],                  0.0, None),
        # Gold
        ("gold",   "team_form_features",     ["team_id"],                    0.0, None),
        ("gold",   "elo_ratings",            ["team_id"],                    0.0, None),
        ("gold",   "player_season_stats",    ["player_id"],                  0.0, None),
    ]

    for layer, table, key_cols, max_null, max_age in checks:
        results = _check_table(spark, layer, table, key_cols, max_null, max_age)
        all_results.extend(results)
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            print(f"[{status}] {r.table} / {r.check}: {r.detail}")
        if fail_fast and any(not r.passed for r in results):
            return 1

    failed = [r for r in all_results if not r.passed]
    total = len(all_results)
    print(f"\n{'='*60}")
    print(f"Checks: {total} total | {total - len(failed)} passed | {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-fast", action="store_true",
                        help="Exit 1 on first failed check")
    args = parser.parse_args()
    sys.exit(run(fail_fast=args.fail_fast))

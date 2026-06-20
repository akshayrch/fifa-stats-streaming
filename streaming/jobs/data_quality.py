"""Data quality checks across Bronze, Silver, and Gold layers.

Checks row counts, null rates on key columns, and data freshness.
Prints a PASS/FAIL line per check and exits non-zero if any check fails.
Also writes a JSON report (`gold/data_quality_report.json`) so the
Streamlit "Pipeline Health" page can show the latest results without
needing a Spark session of its own, and fires a lightweight alert hook on
failure (see `notify_on_failure`).

Usage:
    python streaming/jobs/data_quality.py
    python streaming/jobs/data_quality.py --fail-fast   # exit 1 on first failure
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from streaming.jobs.spark_session import get_spark, lakehouse_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_LAKEHOUSE_BASE_PATH = "file:///tmp/fifa-lakehouse"

# (layer, table, key_cols, max_null_rate, max_age_hours)
CHECKS = [
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


def run_checks(spark: SparkSession, fail_fast: bool = False) -> list[CheckResult]:
    """Runs every check in CHECKS and returns the flat results list, without
    printing or exiting — the reusable core, shared by the CLI (`run()`),
    the Airflow DAG, and report writing."""
    all_results: list[CheckResult] = []
    for layer, table, key_cols, max_null, max_age in CHECKS:
        results = _check_table(spark, layer, table, key_cols, max_null, max_age)
        all_results.extend(results)
        if fail_fast and any(not r.passed for r in results):
            break
    return all_results


def _report_path() -> Path:
    base = os.environ.get("LAKEHOUSE_BASE_PATH", DEFAULT_LAKEHOUSE_BASE_PATH)
    if base.startswith("file://"):
        base = base[len("file://"):]
    return Path(base) / "gold" / "data_quality_report.json"


def write_report(results: list[CheckResult]) -> Path:
    """Writes a JSON snapshot of the latest run so the Streamlit "Pipeline
    Health" page can render it without needing its own Spark session."""
    failed = [r for r in results if not r.passed]
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "total": len(results),
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "results": [asdict(r) for r in results],
    }
    path = _report_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    return path


def notify_on_failure(results: list[CheckResult]) -> None:
    """Lightweight alert hook. Always logs a warning per failed check; also
    POSTs a summary to SLACK_WEBHOOK_URL if that env var is set, so the
    pipeline is demoable without depending on a real Slack workspace."""
    failed = [r for r in results if not r.passed]
    if not failed:
        return

    for r in failed:
        logger.warning("Data quality check failed: %s / %s — %s", r.table, r.check, r.detail)

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    import requests
    summary = "\n".join(f"- {r.table} / {r.check}: {r.detail}" for r in failed)
    try:
        requests.post(webhook_url, json={
            "text": f":rotating_light: Data quality: {len(failed)} check(s) failed\n{summary}"
        }, timeout=10)
    except requests.RequestException as e:
        logger.error("Failed to send Slack alert: %s", e)


def run(fail_fast: bool = False) -> int:
    spark = get_spark("data_quality")
    all_results = run_checks(spark, fail_fast=fail_fast)

    for r in all_results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.table} / {r.check}: {r.detail}")

    failed = [r for r in all_results if not r.passed]
    total = len(all_results)
    print(f"\n{'='*60}")
    print(f"Checks: {total} total | {total - len(failed)} passed | {len(failed)} failed")

    report_path = write_report(all_results)
    print(f"Report written to {report_path}")
    notify_on_failure(all_results)

    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-fast", action="store_true",
                        help="Exit 1 on first failed check")
    args = parser.parse_args()
    sys.exit(run(fail_fast=args.fail_fast))

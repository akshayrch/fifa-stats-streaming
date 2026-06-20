"""Pipeline Health -- reads the latest data_quality_report.json directly, no
Spark session needed. Optionally re-runs streaming/jobs/data_quality.py as a
subprocess (the same checks the medallion_pipeline Airflow DAG's last task
runs) and refreshes from the new report.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
for _p in (str(REPO_ROOT), str(APP_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
import streamlit as st

from shared import get_cached_spark, is_demo_mode

st.set_page_config(page_title="Pipeline Health", page_icon="🩺")
st.title("Pipeline Health")
st.caption("Latest data quality report across Bronze/Silver/Gold (streaming/jobs/data_quality.py).")

if is_demo_mode(get_cached_spark()):
    st.info(
        "Pipeline Health needs a real running medallion pipeline (Kafka + "
        "Spark), which this hosted demo doesn't have -- the other pages run "
        "against a static synthetic snapshot instead. Run the full stack "
        "locally (`docs/RUNBOOK.md`) to see real data quality checks here."
    )
    st.stop()


def _report_path() -> Path:
    base = os.environ.get("LAKEHOUSE_BASE_PATH", "file:///tmp/fifa-lakehouse")
    if base.startswith("file://"):
        base = base[len("file://"):]
    return Path(base) / "gold" / "data_quality_report.json"


def _load_report() -> dict | None:
    path = _report_path()
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


if st.button("Run data quality checks now", type="primary"):
    python_bin = os.environ.get("FIFA_PYTHON_BIN", sys.executable)
    with st.spinner("Running checks (starts a Spark session, ~30-60s)..."):
        proc = subprocess.run(
            [python_bin, "-m", "streaming.jobs.data_quality"],
            cwd=str(REPO_ROOT), capture_output=True, text=True,
        )
    if proc.returncode not in (0, 1):  # 1 == checks ran, some failed -- still a valid report
        st.error("data_quality.py crashed -- see output below.")
        st.code(proc.stdout + proc.stderr)
    else:
        st.success("Checks complete.")

report = _load_report()
if report is None:
    st.warning(
        "No report found yet. Run `python streaming/jobs/data_quality.py` once, "
        "or click the button above."
    )
else:
    st.caption(f"Last run: {report['run_at']}")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total checks", report["total"])
    c2.metric("Passed", report["passed"])
    c3.metric("Failed", report["failed"])

    df = pd.DataFrame(report["results"])
    df["status"] = df["passed"].map({True: "PASS", False: "FAIL"})

    if report["failed"] > 0:
        st.subheader("Failed checks")
        st.dataframe(
            df[~df["passed"]][["table", "check", "detail"]],
            width='stretch', hide_index=True,
        )

    st.subheader("All checks")
    st.dataframe(
        df[["table", "check", "status", "detail"]],
        width='stretch', hide_index=True,
    )

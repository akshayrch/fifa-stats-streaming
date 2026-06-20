"""Persisted tournament state — the running list of completed fixture results
for the fictional tournament (`structure.py`), so a live re-simulation can
pick up where the last one left off instead of re-simulating from scratch.

The design doc suggests writing live state to `gold.tournament_state` or a
small Postgres table. `simulate.py` has no Spark/DB dependency by design
(see structure.py's docstring), so this substitutes a single JSON file under
the same lakehouse root Spark jobs use (`LAKEHOUSE_BASE_PATH`, default
`file:///tmp/fifa-lakehouse`) — plain file I/O, no Spark session needed to
read or write it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

DEFAULT_LAKEHOUSE_BASE_PATH = "file:///tmp/fifa-lakehouse"


def _lakehouse_base_path() -> Path:
    base = os.environ.get("LAKEHOUSE_BASE_PATH", DEFAULT_LAKEHOUSE_BASE_PATH)
    if base.startswith("file://"):
        base = base[len("file://"):]
    return Path(base)


STATE_PATH = _lakehouse_base_path() / "gold" / "tournament_state.json"


def load_state() -> dict:
    """Returns {"completed_results": [...]}. Empty if no state file yet
    (start-of-tournament: nothing has been played)."""
    if not STATE_PATH.exists():
        return {"completed_results": []}
    with open(STATE_PATH) as f:
        return json.load(f)


def save_state(completed_results: list[dict]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump({"completed_results": completed_results}, f, indent=2)


def record_result(home_id: int, away_id: int, home_goals: int, away_goals: int) -> dict:
    """Appends a finished match to the persisted state and returns the
    updated state dict. Used by live_consumer.py when a tracked fixture
    transitions to full-time."""
    state = load_state()
    state["completed_results"].append({
        "home_id": home_id, "away_id": away_id,
        "home_goals": home_goals, "away_goals": away_goals,
    })
    save_state(state["completed_results"])
    return state

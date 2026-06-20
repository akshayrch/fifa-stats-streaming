"""Lineup optimization — Stage 2 of the design doc's two-stage approach.

Integer program (PuLP, CBC backend): pick exactly 11 available players
matching a formation's positional counts, maximizing total contribution
score, optionally subject to a budget cap.
"""

from __future__ import annotations

import pulp
import pandas as pd

# (DEF, MID, FWD) outfield counts; GK is always exactly 1.
FORMATIONS = {
    "4-4-2": {"GK": 1, "DEF": 4, "MID": 4, "FWD": 2},
    "4-3-3": {"GK": 1, "DEF": 4, "MID": 3, "FWD": 3},
    "3-5-2": {"GK": 1, "DEF": 3, "MID": 5, "FWD": 2},
    "5-3-2": {"GK": 1, "DEF": 5, "MID": 3, "FWD": 2},
}


def select_best_xi(
    squad: pd.DataFrame, formation: str = "4-4-2", budget: float | None = None
) -> pd.DataFrame:
    """Returns the 11-row subset of `squad` (must have contribution_score,
    position, available, cost columns) that maximizes total contribution
    score under the formation's positional counts, available-only, and an
    optional total-cost budget cap.

    Raises ValueError if the squad can't fill the formation (not enough
    available players in some position, or budget too tight).
    """
    if formation not in FORMATIONS:
        raise ValueError(f"Unknown formation '{formation}'. Choose from {list(FORMATIONS)}")
    counts = FORMATIONS[formation]

    pool = squad[squad["available"]].reset_index(drop=True)
    prob = pulp.LpProblem("squad_optimizer", pulp.LpMaximize)
    x = {i: pulp.LpVariable(f"x_{i}", cat="Binary") for i in pool.index}

    prob += pulp.lpSum(x[i] * pool.loc[i, "contribution_score"] for i in pool.index)

    for position, count in counts.items():
        position_idx = pool.index[pool["position"] == position]
        prob += pulp.lpSum(x[i] for i in position_idx) == count, f"count_{position}"

    if budget is not None:
        prob += pulp.lpSum(x[i] * pool.loc[i, "cost"] for i in pool.index) <= budget, "budget"

    status = prob.solve(pulp.PULP_CBC_CMD(msg=False))
    if pulp.LpStatus[status] != "Optimal":
        raise ValueError(
            f"No feasible XI for formation={formation} budget={budget} "
            f"(solver status: {pulp.LpStatus[status]}). Squad may lack enough "
            f"available players in one position, or the budget is too tight."
        )

    selected_idx = [i for i in pool.index if x[i].value() == 1]
    return pool.loc[selected_idx].sort_values(
        "position", key=lambda s: s.map({"GK": 0, "DEF": 1, "MID": 2, "FWD": 3})
    )


def naive_xi(squad: pd.DataFrame, formation: str = "4-4-2") -> pd.DataFrame:
    """Baseline lineup for comparison: fill each position with its first
    `count` available players in roster order — no optimization. Represents
    "pick whoever's available in roster order" rather than the smarter
    "actually weigh contribution" choice the optimizer makes.
    """
    counts = FORMATIONS[formation]
    pool = squad[squad["available"]]
    picks = [
        pool[pool["position"] == position].head(count)
        for position, count in counts.items()
    ]
    return pd.concat(picks).sort_values(
        "position", key=lambda s: s.map({"GK": 0, "DEF": 1, "MID": 2, "FWD": 3})
    )

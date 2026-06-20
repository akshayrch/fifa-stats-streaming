"""Tournament structure as data — 8 fictional national teams, 2 groups of 4,
single round-robin group stage, then a 4-team knockout (semifinals + final).

Why fictional teams: the design doc's example is a World Cup / continental
championship — a different competition shape (neutral-venue, groups +
knockout) from the club leagues already in the mock pipeline. Reusing the 4
real club teams (`silver.dim_team`) would only cover half a group and
implies a club competition, not an international one. These teams exist
only here — `simulate.py` tracks their ELO/form purely in-memory, no Gold
lookup needed, so this module has no Spark dependency.

Base ELOs are deliberately spread (1470-1620) so the simulation has
favorites and underdogs to produce an interesting probability spread, the
same way real tournament seeding does.
"""

from __future__ import annotations

from itertools import combinations

TEAMS = {
    9001: {"name": "Norrland", "base_elo": 1620.0},
    9002: {"name": "Castellan", "base_elo": 1480.0},
    9003: {"name": "Meridia", "base_elo": 1550.0},
    9004: {"name": "Boreas", "base_elo": 1500.0},
    9005: {"name": "Tarawak", "base_elo": 1600.0},
    9006: {"name": "Valdoria", "base_elo": 1470.0},
    9007: {"name": "Solaria", "base_elo": 1530.0},
    9008: {"name": "Kestria", "base_elo": 1510.0},
}

GROUPS = {
    "A": [9001, 9002, 9003, 9004],
    "B": [9005, 9006, 9007, 9008],
}

# Single round-robin within each group: 4 teams -> 6 matches each.
GROUP_FIXTURES = {
    group: list(combinations(team_ids, 2)) for group, team_ids in GROUPS.items()
}

# Knockout pairing rule, applied once group standings are known: cross-bracket
# pairing (group winner vs the *other* group's runner-up) so the two teams
# from the same group can't meet again before the final.
KNOCKOUT_SEEDING = [
    ("Semifinal 1", "A", 0, "B", 1),  # Group A 1st vs Group B 2nd
    ("Semifinal 2", "B", 0, "A", 1),  # Group B 1st vs Group A 2nd
]


def team_name(team_id: int) -> str:
    return TEAMS[team_id]["name"]


def compute_group_standings(team_ids: list[int], results: list[dict]) -> list[dict]:
    """results: [{"home_id", "away_id", "home_goals", "away_goals"}, ...]
    (only matches between these team_ids are considered).

    Returns a standings table sorted by points, then goal difference, then
    goals for, then head-to-head points among teams still tied after that —
    a simplified but standard tiebreak chain (no fair-play/away-goals rules).
    """
    table = {
        tid: {"team_id": tid, "played": 0, "won": 0, "draw": 0, "lost": 0,
              "gf": 0, "ga": 0, "points": 0}
        for tid in team_ids
    }

    for r in results:
        h, a = r["home_id"], r["away_id"]
        if h not in table or a not in table:
            continue
        hg, ag = r["home_goals"], r["away_goals"]
        table[h]["played"] += 1
        table[a]["played"] += 1
        table[h]["gf"] += hg
        table[h]["ga"] += ag
        table[a]["gf"] += ag
        table[a]["ga"] += hg
        if hg > ag:
            table[h]["won"] += 1; table[h]["points"] += 3
            table[a]["lost"] += 1
        elif hg < ag:
            table[a]["won"] += 1; table[a]["points"] += 3
            table[h]["lost"] += 1
        else:
            table[h]["draw"] += 1; table[h]["points"] += 1
            table[a]["draw"] += 1; table[a]["points"] += 1

    def h2h_points(tid: int, tied_with: list[int]) -> int:
        pts = 0
        for r in results:
            h, a = r["home_id"], r["away_id"]
            if tid == h and a in tied_with:
                pts += 3 if r["home_goals"] > r["away_goals"] else (1 if r["home_goals"] == r["away_goals"] else 0)
            elif tid == a and h in tied_with:
                pts += 3 if r["away_goals"] > r["home_goals"] else (1 if r["home_goals"] == r["away_goals"] else 0)
        return pts

    rows = list(table.values())
    for row in rows:
        row["gd"] = row["gf"] - row["ga"]

    # Group rows by (points, gd, gf) to find ties, then break by head-to-head.
    rows.sort(key=lambda r: (-r["points"], -r["gd"], -r["gf"]))
    i = 0
    while i < len(rows):
        j = i
        key = (rows[i]["points"], rows[i]["gd"], rows[i]["gf"])
        while j < len(rows) and (rows[j]["points"], rows[j]["gd"], rows[j]["gf"]) == key:
            j += 1
        if j - i > 1:
            tied_ids = [r["team_id"] for r in rows[i:j]]
            rows[i:j] = sorted(
                rows[i:j],
                key=lambda r: -h2h_points(r["team_id"], [t for t in tied_ids if t != r["team_id"]]),
            )
        i = j

    return rows

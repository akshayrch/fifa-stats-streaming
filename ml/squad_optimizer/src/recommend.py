"""CLI: opponent + formation (+ optional budget) -> recommended XI and its
predicted win probability vs. a naive baseline lineup.

Usage:
    python -m ml.squad_optimizer.src.recommend --opponent Arsenal
    python -m ml.squad_optimizer.src.recommend --opponent 42 --formation 4-3-3
    python -m ml.squad_optimizer.src.recommend --opponent Arsenal --budget 650

This reuses ml.match_odds's win-probability function (per the design doc:
"App 1 reuses App 2's win-probability function") rather than re-implementing
match prediction. The squad itself is synthetic (see synthetic_squad_data.py
for why); the opponent's ELO/form is real Gold data from Phase 3 whenever
available.
"""

from __future__ import annotations

import argparse

from ml.match_odds.src.features import latest_team_stats, resolve_team_id
from ml.match_odds.src.predict import get_match_probabilities, load_model
from ml.squad_optimizer.src.contribution import score_players
from ml.squad_optimizer.src.optimizer import FORMATIONS, naive_xi, select_best_xi
from ml.squad_optimizer.src.synthetic_squad_data import SQUAD_TEAM_ID, generate_squad

# How many ELO points one point of average contribution_score is worth when
# translating "this lineup is stronger than that one" into the match-odds
# model's elo_diff feature. This is a simplifying assumption, not a fitted
# coefficient — there's no historical lineup-vs-result dataset yet to fit it
# against (same data gap as ml/match_odds — see that module's README).
ELO_POINTS_PER_CONTRIBUTION_POINT = 15.0


def describe_swaps(optimized, baseline) -> list[str]:
    """Per-position diff between the optimized and naive XIs."""
    swaps = []
    for position in ["GK", "DEF", "MID", "FWD"]:
        opt_names = set(optimized[optimized["position"] == position]["player_name"])
        base_names = set(baseline[baseline["position"] == position]["player_name"])
        ins = opt_names - base_names
        outs = base_names - opt_names
        opt_rows = optimized.set_index("player_name")
        base_rows = baseline.set_index("player_name")
        for in_name, out_name in zip(sorted(ins), sorted(outs)):
            delta = opt_rows.loc[in_name, "contribution_score"] - base_rows.loc[out_name, "contribution_score"]
            swaps.append(f"{in_name} in for {out_name} ({position}): {delta:+.1f} contribution score")
    return swaps


def run(opponent_arg: str, formation: str, budget: float | None) -> None:
    from streaming.jobs.spark_session import get_spark
    spark = get_spark("squad_optimizer_recommend")

    opponent_id = resolve_team_id(spark, opponent_arg)
    if opponent_id is None:
        raise SystemExit(f"Could not resolve opponent '{opponent_arg}' in silver.dim_team")

    squad = score_players(generate_squad())
    optimized = select_best_xi(squad, formation, budget)
    baseline = naive_xi(squad, formation)

    model = load_model()
    team_stats = latest_team_stats(spark)

    avg_delta = optimized["contribution_score"].mean() - baseline["contribution_score"].mean()
    elo_offset = avg_delta * ELO_POINTS_PER_CONTRIBUTION_POINT

    opt_proba, _ = get_match_probabilities(
        spark, SQUAD_TEAM_ID, opponent_id, model=model, team_stats=team_stats,
        home_elo_offset=elo_offset,
    )
    base_proba, _ = get_match_probabilities(
        spark, SQUAD_TEAM_ID, opponent_id, model=model, team_stats=team_stats,
        home_elo_offset=0.0,
    )

    counts = FORMATIONS[formation]
    print(f"\nRecommended XI ({formation}) vs opponent team_id={opponent_id}:")
    for _, row in optimized.iterrows():
        print(f"  {row['position']:<4} {row['player_name']:<20} "
              f"rating={row['avg_rating']:.2f}  contribution={row['contribution_score']:.1f}"
              f"{'  [unavailable!]' if not row['available'] else ''}")

    print(f"\nPredicted win probability:")
    print(f"  Optimized XI : Home win {opt_proba.get('H', 0):.0%} | "
          f"Draw {opt_proba.get('D', 0):.0%} | Away win {opt_proba.get('A', 0):.0%}")
    print(f"  Naive XI     : Home win {base_proba.get('H', 0):.0%} | "
          f"Draw {base_proba.get('D', 0):.0%} | Away win {base_proba.get('A', 0):.0%}")
    print(f"  Uplift from optimization: {opt_proba.get('H', 0) - base_proba.get('H', 0):+.1%} win probability")

    swaps = describe_swaps(optimized, baseline)
    if swaps:
        print("\nKey swap explanations:")
        for s in swaps:
            print(f"  - {s}")
    else:
        print("\nOptimized XI matches the naive baseline — no swaps to explain.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Recommend a starting XI vs. an opponent.")
    parser.add_argument("--opponent", required=True, help="Opponent team name or team_id")
    parser.add_argument("--formation", default="4-4-2", choices=list(FORMATIONS))
    parser.add_argument("--budget", type=float, default=None, help="Optional total squad-cost cap")
    args = parser.parse_args()
    run(args.opponent, args.formation, args.budget)


if __name__ == "__main__":
    main()

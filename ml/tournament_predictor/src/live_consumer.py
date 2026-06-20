"""Kafka consumer that re-triggers the Monte Carlo simulation when a tracked
tournament fixture finishes.

Subscribes to:
  - football.fixtures.raw  — the only topic that carries match status
    (`fixture.status.short`). Watches for a tracked fixture's NS/1H/2H -> FT
    transition; on FT, records the final score into tournament_predictor
    state (state.py) and re-runs the simulation with the updated standings.
  - football.events.live   — goal/card/sub events for in-progress matches.
    Logged for visibility only; per the design doc, resimulating mid-match
    on every goal (rather than only at full-time) is a stretch goal, not
    the MVP.

"Tracked fixture" = both teams are in structure.py's fictional TEAMS. Real
producers (API-Football) only ever publish real club fixtures, which will
never match a fictional team id — there is nothing in the live pipeline for
this consumer to react to without help. So this module doubles as its own
test harness: `--publish-test-result` publishes one synthetic FT fixture
message, in the same envelope a real producer would use, so the trigger can
be demoed end-to-end in two terminals (one running this consumer, one firing
`--publish-test-result`).

Usage:
    python -m ml.tournament_predictor.src.live_consumer [--max-messages N] [--timeout SECONDS]
    python -m ml.tournament_predictor.src.live_consumer --publish-test-result HOME_ID AWAY_ID HOME_GOALS AWAY_GOALS
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from confluent_kafka import Consumer

from ingestion.producers.config import load_settings
from ingestion.producers.kafka_producer import build_producer, publish
from ml.tournament_predictor.src.simulate import print_report, run_simulation
from ml.tournament_predictor.src.state import load_state, record_result
from ml.tournament_predictor.src.structure import TEAMS, team_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FIXTURES_TOPIC = "football.fixtures.raw"
EVENTS_TOPIC = "football.events.live"

# Re-simulating on every live trigger needs to be fast, not exhaustive —
# 2,000 trials finishes in well under a minute vs. ~4 min for the CLI's
# 10,000-trial default (see simulate.py's N_SIMULATIONS_DEFAULT).
LIVE_TRIALS = 2_000


def _is_tracked_fixture(teams: dict) -> bool:
    home_id = teams.get("home", {}).get("id")
    away_id = teams.get("away", {}).get("id")
    return home_id in TEAMS and away_id in TEAMS


def _already_recorded(home_id: int, away_id: int) -> bool:
    """Each team pair plays at most once across the whole tournament (group
    fixtures are unique pairs; KNOCKOUT_SEEDING crosses brackets so the same
    pair can't recur in the knockout stage either). Guards against double-
    counting a result if Kafka redelivers a message or this consumer
    restarts before its last offset commit went through."""
    pair = frozenset((home_id, away_id))
    return any(
        frozenset((r["home_id"], r["away_id"])) == pair
        for r in load_state()["completed_results"]
    )


def handle_fixture_message(payload: dict, seen_status: dict[int, str]) -> bool:
    """Returns True if this message triggered a re-simulation."""
    fixture = payload.get("fixture", {})
    teams = payload.get("teams", {})
    goals = payload.get("goals", {})
    fixture_id = fixture.get("id")
    status = fixture.get("status", {}).get("short")

    if not _is_tracked_fixture(teams):
        return False

    previous_status = seen_status.get(fixture_id)
    seen_status[fixture_id] = status
    if status != "FT" or previous_status == "FT":
        return False  # only act once, on the transition into FT

    home_id, away_id = teams["home"]["id"], teams["away"]["id"]
    if _already_recorded(home_id, away_id):
        logger.info("Already recorded %s vs %s, skipping (duplicate delivery).",
                     team_name(home_id), team_name(away_id))
        return False

    home_goals, away_goals = goals.get("home"), goals.get("away")
    logger.info(
        "Tracked fixture finished: %s %d-%d %s. Recording result + re-simulating (%d trials)...",
        team_name(home_id), home_goals, away_goals, team_name(away_id), LIVE_TRIALS,
    )

    state = record_result(home_id, away_id, home_goals, away_goals)
    result = run_simulation(state["completed_results"], n_trials=LIVE_TRIALS)
    print_report(result)
    return True


def handle_event_message(key: str, payload: list[dict]) -> None:
    tracked_events = [e for e in payload if e.get("team", {}).get("id") in TEAMS]
    for event in tracked_events:
        logger.info(
            "[fixture %s] %s' %s: %s (%s)",
            key, event.get("time", {}).get("elapsed"), event.get("team", {}).get("name"),
            event.get("type"), event.get("detail"),
        )


def run(max_messages: int | None = None, timeout: float | None = None) -> int:
    settings = load_settings()
    consumer = Consumer({
        "bootstrap.servers": settings["kafka"]["bootstrap_servers"],
        "group.id": "tournament-predictor-live",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([FIXTURES_TOPIC, EVENTS_TOPIC])

    logger.info("Subscribed to %s. Waiting for tracked-fixture messages...",
                [FIXTURES_TOPIC, EVENTS_TOPIC])
    seen_status: dict[int, str] = {}
    consumed = 0
    last_message_at = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if timeout and (time.monotonic() - last_message_at) > timeout:
                    logger.info("No messages for %.0fs, exiting.", timeout)
                    return consumed
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            envelope = json.loads(msg.value())
            payload = envelope.get("payload")
            if msg.topic() == FIXTURES_TOPIC:
                handle_fixture_message(payload, seen_status)
            elif msg.topic() == EVENTS_TOPIC:
                handle_event_message(msg.key().decode() if msg.key() else "?", payload)

            consumed += 1
            last_message_at = time.monotonic()
            if max_messages and consumed >= max_messages:
                logger.info("Reached max_messages=%d, exiting.", max_messages)
                return consumed
    except KeyboardInterrupt:
        return consumed
    finally:
        consumer.close()


def publish_test_result(home_id: int, away_id: int, home_goals: int, away_goals: int) -> None:
    """Publishes one synthetic, already-finished fixture message for two
    fictional teams onto football.fixtures.raw, in the same envelope/shape
    a real producer would use. Lets `run()` be demoed without a real
    producer ever emitting data for these teams."""
    if home_id not in TEAMS or away_id not in TEAMS:
        raise SystemExit(f"Both team ids must be tournament teams: {sorted(TEAMS)}")

    settings = load_settings()
    producer = build_producer(settings["kafka"]["bootstrap_servers"])
    fixture_id = 90_000 + home_id  # arbitrary, distinct from real club fixture ids (1000s)
    fixture_payload = {
        "fixture": {"id": fixture_id, "status": {"long": "Match Finished", "short": "FT", "elapsed": 90}},
        "league": {"id": 0, "name": "Fictional Tournament"},
        "teams": {
            "home": {"id": home_id, "name": team_name(home_id), "winner": home_goals > away_goals},
            "away": {"id": away_id, "name": team_name(away_id), "winner": away_goals > home_goals},
        },
        "goals": {"home": home_goals, "away": away_goals},
    }
    publish(producer, FIXTURES_TOPIC, key=fixture_id, payload=fixture_payload, endpoint="/fixtures")
    producer.flush()
    logger.info(
        "Published test result: %s %d-%d %s (fixture_id=%d)",
        team_name(home_id), home_goals, away_goals, team_name(away_id), fixture_id,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Live tournament re-simulation consumer.")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument(
        "--publish-test-result", nargs=4, type=int, metavar=("HOME_ID", "AWAY_ID", "HOME_GOALS", "AWAY_GOALS"),
        help="Publish one synthetic finished-fixture message for two tournament "
             "team ids and exit, instead of consuming.",
    )
    args = parser.parse_args()

    if args.publish_test_result:
        publish_test_result(*args.publish_test_result)
        return

    run(max_messages=args.max_messages, timeout=args.timeout)


if __name__ == "__main__":
    main()

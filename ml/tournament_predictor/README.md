# App 3 — Live Tournament Predictor

See [design doc](../../docs/apps/03_tournament_predictor.md).

## Layout

```
tournament_predictor/
└── src/
    ├── structure.py      # tournament structure (groups, bracket rules) as data
    ├── simulate.py        # Monte Carlo simulation using ml/match_odds
    ├── state.py            # reads/writes completed-results state (JSON, no DB)
    └── live_consumer.py    # consumes football.fixtures.raw, triggers re-simulation on FT
```

Deviates from the planned layout in two ways:
- No `data/tournament_structure.example.json` — `structure.py` holds the
  8 teams / 2 groups / knockout bracket directly as Python dicts. For a
  fixed 8-team shape this is less code than a JSON loader + schema, and the
  values (team ids, base ELOs, bracket pairing) are only ever read, not
  edited by a non-developer.
- No `notebooks/01_simulation_prototype.ipynb` — went straight to the `src/`
  modules; the design was simple enough not to need notebook prototyping
  first (unlike App 2's model selection, which did).

## Why fictional teams

The design doc's example is a World Cup–shaped competition: neutral venue,
groups + knockout. That's a different competition shape from the club
leagues already in the mock pipeline (`silver.dim_team` has 4 real club
teams — half a group, and the wrong competition format). `structure.py`
invents 8 national teams (Norrland, Castellan, Meridia, Boreas, Tarawak,
Valdoria, Solaria, Kestria) with deliberately spread base ELOs
(1470–1620) across 2 groups of 4, plus a 4-team knockout (semifinals +
final, cross-bracket seeded so the same group's teams can't meet again
before the final). Same bridge pattern as `ml/match_odds` and
`ml/squad_optimizer`: synthetic where the real mock data can't cover the
shape of the problem. Since these teams don't exist in the real lakehouse,
`simulate.py` tracks their ELO/form purely in memory — no Spark dependency
in this app at all, only the trained joblib model file from Phase 4.

## Monte Carlo simulation (`simulate.py`)

For each of N trials (default 10,000, ~3.5 min):
1. Seed every team's ELO from `structure.py`'s base ELOs, apply any
   already-completed results first.
2. Simulate every remaining group fixture: get win/draw/loss probabilities
   from App 2's trained model, sample an outcome, sample a scoreline
   (`_sample_scoreline()` — a simple weighted heuristic layered on top,
   since the model only predicts the W/D/L category and group standings
   need goal difference to break ties), then update both teams' in-trial
   ELO/form so later fixtures in *that trial* — including the knockout
   stage — reflect it.
3. Compute group standings (points → goal difference → goals for →
   head-to-head), take the top 2 per group as qualifiers.
4. Build the knockout bracket from `KNOCKOUT_SEEDING`, simulate the
   semifinals and final. Knockout matches can't end in a draw; since the
   model has no signal to predict a penalty shootout, a drawn knockout
   match is resolved with a 50/50 coin flip — documented as a deliberate
   simplification, not a modeled outcome.

Aggregating across trials gives each team's probability of qualifying,
winning its group, reaching the final, and winning the tournament.

**Neutral-venue correction**: the match_odds model was trained on club
fixtures with a real home side, so it bakes in a learned home-advantage
effect. Tournament matches are neutral-venue, so `_match_outcome_probs()`
averages the team-A-as-home and team-B-as-home framings to cancel that
bias out, rather than arbitrarily picking one team as "home."

## Live re-simulation (`state.py` + `live_consumer.py`)

The design doc calls for results in `gold.tournament_state` (or "a small
Postgres table for low-latency reads"). Since this app already has no
Spark dependency, `state.py` substitutes a single JSON file under the
lakehouse root (`$LAKEHOUSE_BASE_PATH/gold/tournament_state.json`) —
`load_state()` / `save_state()` / `record_result()`, plain file I/O.

`live_consumer.py` subscribes to:
- `football.fixtures.raw` — the only topic carrying match status
  (`fixture.status.short`); `football.events.live` has no status field at
  all (confirmed against `live_events_producer.py`). Watches each tracked
  fixture (both teams in `structure.py`'s `TEAMS`) for its first
  NS/1H/2H → FT transition, then records the final score and re-runs the
  simulation (2,000 trials — fast enough to react to a live event; the CLI's
  10,000-trial default is for an on-demand full report, not a live trigger).
  Guards against double-counting a result if a message is redelivered or
  the consumer restarts before its last offset commit.
- `football.events.live` — goal/card/sub events, logged for visibility only.
  Resimulating mid-match on every goal (rather than only at full-time) is
  the design doc's stated stretch goal, not the MVP.

Real producers only ever publish real club fixtures, so nothing in the live
pipeline will ever naturally match a fictional team id. `live_consumer.py`
doubles as its own test harness for this:

```bash
python -m ml.tournament_predictor.src.live_consumer --publish-test-result 9001 9002 3 0
```

publishes one synthetic, already-finished fixture message (Norrland 3–0
Castellan) onto `football.fixtures.raw` in the same envelope a real
producer would use, so the live trigger can be demoed end-to-end without
waiting for a real match.

## Running it

```bash
export PYTHONPATH=$PWD
pip install -r ml/tournament_predictor/requirements.txt
pip install -r ml/match_odds/requirements.txt   # simulate.py needs the trained odds model
python -m ml.match_odds.src.train               # if models/match_odds_model.joblib doesn't exist yet

# Full-tournament-from-scratch report
python -m ml.tournament_predictor.src.simulate --trials 10000

# Resume from persisted live state instead of simulating from scratch
python -m ml.tournament_predictor.src.simulate --from-state

# Terminal 1 — long-running consumer
python -m ml.tournament_predictor.src.live_consumer

# Terminal 2 — fire a test result, watch terminal 1 re-simulate
python -m ml.tournament_predictor.src.live_consumer --publish-test-result 9001 9002 3 0
```

Example output:

```
Group A standings + qualification probabilities (10,000 simulations)
  Norrland   Pld 0, Pts 0   -> Qualify: 70%  | Win group: 43%
  Meridia    Pld 0, Pts 0   -> Qualify: 53%  | Win group: 26%
  Boreas     Pld 0, Pts 0   -> Qualify: 40%  | Win group: 17%
  Castellan  Pld 0, Pts 0   -> Qualify: 36%  | Win group: 14%
  ...

Tournament winner probabilities (top 5)
  Norrland: 25.1%  Tarawak: 21.3%  Meridia: 13.6%  Solaria: 11.0%  Kestria: 9.3%
```

## Status

✅ Phase 6 complete — tournament structure as data, Monte Carlo simulation
on top of App 2's win-probability model (neutral-venue corrected), JSON
state persistence, and a Kafka consumer that re-triggers simulation on
live full-time results. Verified end-to-end: full-tournament simulation
(favorites rank correctly by base ELO), `--from-state` resume, and the
live consumer against a real local Kafka broker — including the
duplicate-delivery dedupe guard and the events-topic informational logging.

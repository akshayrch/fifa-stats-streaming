# Real-time tournament simulation triggered by live match events

Phase 6 is the Live Tournament Predictor — Monte Carlo simulation of a
groups-plus-knockout tournament that re-runs when a tracked live result
comes in. It's the most structurally different of the three apps, and it's
where the synthetic-data bridge pattern stopped being about *sparse* data
and became about *wrong-shaped* data entirely.

## A different kind of data problem

The design doc assumes a World Cup-shaped competition: neutral venue,
group stage, then knockout. `silver.dim_team` has 4 real club teams at
this point in the pipeline — which isn't just "not enough," it's half a
group and the wrong format regardless of row count. Club leagues don't
have a group-and-knockout structure to begin with. So
`ml/tournament_predictor/src/structure.py` invents one: 8 fictional
national teams (Norrland, Castellan, Meridia, Boreas, Tarawak, Valdoria,
Solaria, Kestria) with deliberately spread base ELOs from 1470 to 1620,
split into 2 groups of 4, single round-robin group stage, then a
cross-bracket-seeded 4-team knockout. Because these teams don't exist in
the real lakehouse at all, this app tracks their ELO and form purely in
memory — there's no Spark dependency anywhere in App 3, only the trained
joblib model file from Phase 4. That's a meaningfully different bridge
shape than Phases 4 and 5: those apps anchored synthetic data to a real
team ID so half the calculation stayed real; this one couldn't, because
the competition shape itself doesn't exist in the real data.

## The simulation engine, and two bugs that weren't obvious upfront

`structure.py` builds the round-robin fixture list via
`itertools.combinations`, seeds the knockout bracket so a group winner
faces the *other* group's runner-up (so two teams from the same group
can't meet again before the final), and `compute_group_standings()`
implements the points → goal-difference → goals-for → head-to-head
tiebreak chain.

`simulate.py` is the actual Monte Carlo engine — by default 10,000 trials,
taking roughly 3.5 minutes. Per trial: simulate every remaining group
fixture by sampling from App 2's trained model, update each team's
in-trial ELO and form after every match (so later fixtures within that
same trial, including the knockout stage, reflect what happened earlier in
that trial), compute standings, build and simulate the knockout bracket,
and record who qualified, who won their group, and who won it all.
Aggregating across 10,000 trials gives every team a probability at every
stage.

Two correctness problems surfaced here that weren't obvious from the
design doc alone. First, neutral-venue bias: App 2's model was trained on
club fixtures with a real home side, so it has a learned home-advantage
effect baked in — which is exactly wrong for a neutral-venue international
match. `_match_outcome_probs()` fixes this by averaging the team-A-as-home
and team-B-as-home framings, canceling the home-advantage term out rather
than arbitrarily picking one team to call "home." Second, the model only
predicts a win/draw/loss category, with no scoreline — but group standings
need goal difference to break ties, and knockout matches can't end in a
draw at all. `_sample_scoreline()` layers a weighted heuristic on top
purely for tiebreak purposes, and a drawn knockout match resolves via a
50/50 coin flip, which is a documented simplification (the model has no
signal to predict a penalty shootout), not something dressed up as a
modeled outcome.

## State as a JSON file, not a database

The design doc suggested `gold.tournament_state` or "a small Postgres
table." Given that this app already has no Spark dependency by design, a
database felt like the wrong tool for the job — `state.py` instead
persists to a single JSON file under the lakehouse root
(`gold/tournament_state.json`), with `load_state()`/`save_state()`/
`record_result()` as plain file I/O. It's a deliberately smaller solution
than the design doc's first instinct, chosen because the simplest thing
that satisfies the actual requirement (a small amount of state that needs
to survive between simulation runs) beat matching the design doc literally.

## The live trigger, and testing something that will never happen naturally

`live_consumer.py` is the part of this app that earns the word "live" in
the title. It subscribes to `football.fixtures.raw` — confirmed to be the
only topic that carries match status at all, since `football.events.live`
has no status field — and watches each tracked fixture (where both teams
are in the fictional `TEAMS` list) for its first NS/1H/2H → FT transition.
On that transition it records the final score and re-simulates with a
smaller trial count (2,000, instead of the CLI's 10,000-trial default) so
it can react quickly to a live event rather than taking 3.5 minutes to
update.

Here's the problem that's specific to this app and doesn't show up
anywhere else in the project: real producers only ever publish real club
fixtures. Nothing in the live pipeline will ever naturally emit a fixture
for Norrland vs. Castellan, because those teams don't exist in API-Football.
So `live_consumer.py` ships its own test harness,
`--publish-test-result HOME AWAY HOME_GOALS AWAY_GOALS`, which publishes a
synthetic, already-finished fixture message in the exact envelope shape a
real producer would use. Verifying the live trigger end to end meant
running `live_consumer.py` in one process and the test-harness publish
command in another, against a real local Kafka broker — confirming the
consumer detected the FT transition, recorded the result, re-simulated,
and produced numbers matching a `--from-state` run against the same
recorded result. The dedupe guard (`_already_recorded()`) correctly
skipped a re-published identical result, and a second, different tracked
pair recorded alongside the first left exactly 2 entries in
`tournament_state.json` — which is the kind of idempotency check that
matters a lot more once you imagine this running unattended on a schedule.

## What it actually showed

With no matches played, qualification probabilities tracked base ELO
exactly as you'd hope — Norrland (1620) and Tarawak (1600) led their
groups at every stage, Castellan (1480) and Valdoria (1470) trailed. After
recording a real result (Norrland beating Castellan 3-0), Norrland's
qualification probability jumped from 70% to 93%, correctly reflecting one
played match plus a still-partial table for everyone else.

## What's next

All three apps are now built and independently verified, each with its
own synthetic-data bridge built to the real Gold layer's rules. What's left
is the part that turns "three scripts I run by hand" into an actual
platform: orchestration, observability, and a UI — which is also where I
sat down and wrote the project retrospective.

## LinkedIn version

Phase 6: the Live Tournament Predictor — Monte Carlo simulation of a
groups-plus-knockout tournament, re-triggered by live match events.

The data problem here wasn't "not enough rows," it was "wrong shape
entirely" — club leagues don't have a group/knockout structure, so I built
8 fictional national teams with spread ELOs (1470-1620) instead of forcing
real club data into a format it doesn't fit.

Two bugs the design doc didn't anticipate:
- The odds model has a learned home-advantage bias from club data — wrong
  for a neutral-venue match. Fixed by averaging both home/away framings.
- The model predicts win/draw/loss only, but standings need goal
  difference. Added a scoreline sampler for tiebreaks; drawn knockout
  matches resolve via a coin flip, a documented simplification.

The fun problem to test: real producers will never publish a fixture for a
fictional team, so the live consumer ships its own
`--publish-test-result` harness to simulate its own trigger.

10,000-trial simulation correctly ranks teams by ELO at every stage;
recording a real result jumps the winner's qualification odds from 70% to
93%.

Full write-up: [link to Medium article]

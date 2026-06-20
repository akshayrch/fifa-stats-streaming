# Retrospective

What worked, what I'd change, and what I learned building a real-time
football data platform end to end — Kafka, Spark Structured Streaming, a
Medallion lakehouse, 3 AI apps, Airflow orchestration, and a Streamlit UI.
Pairs with [`roadmap.md`](roadmap.md) (what got built, phase by phase) and
[`progress.md`](progress.md) (how, in detail).

## What worked

**The medallion architecture earned its keep immediately.** Splitting
Bronze (raw, replayable) / Silver (conformed dims/facts) / Gold (features)
meant every downstream consumer — 3 very different apps — read from the
same small set of well-defined feature tables instead of each having to
re-parse raw API JSON. When the API-Football mock payloads turned out to
have quirks (nested optional fields, inconsistent player-stats shapes),
fixing it once in `silver_transform.py` fixed it for every app at once.

**The synthetic-data bridge pattern paid for itself three times.** The
honest constraint of this project is that the mock pipeline only ever
produces a handful of real rows — nowhere near enough to train a
classifier, optimize a lineup, or run an 8-team tournament. Rather than
block each app on "enough real data exists" or fake the constraint away,
every app got an explicit, documented synthetic generator
(`synthetic_data.py`, `synthetic_squad_data.py`, `structure.py`'s fictional
teams) built to the *same* statistical rules as the real Gold tables (same
ELO formula, same feature schema). That meant the real swap-in path is
"replace the generator, keep everything downstream unchanged" — verified in
practice when `ml/tournament_predictor` needed *zero* changes to `simulate.py`
once `state.py` started feeding it real recorded results.

**Backtesting caught me almost picking the wrong model.** The instinct
going in was "gradient boosting will obviously beat a logistic-regression
baseline." The walk-forward backtest in `ml/match_odds/src/evaluate.py`
said otherwise — the ELO-only baseline won on log-loss, because once ELO
converges it already captures most of the team-strength signal, and the
extra rolling-form features mostly added variance, not information.
`train.py` deploys whichever the backtest actually favors, not whichever
sounds more sophisticated. That's a small thing, but it's the difference
between a portfolio project that demos a metric and one that demos a
discipline.

**Keeping Airflow in its own venv avoided a whole category of pain.**
Airflow's dependency tree is famously large and easy to break against an
existing data-science stack. Treating its DAGs as "shell out to the other
venv's Python" rather than "import the pipeline code directly" meant
Airflow could be added in Phase 7 without touching, retesting, or risking
anything from Phases 1-6.

## What I'd change

**I'd reach for `streamlit.testing.v1.AppTest` from day one of the UI work,
not as an afterthought.** Building the Streamlit app in an environment with
no browser meant verification leaned on `AppTest` to execute every page
headlessly and assert no exception — which works, and caught a real
deprecation warning (`use_container_width` -> `width=`) — but it can't
catch layout problems, so "the page renders correctly" and "the page looks
right" are still two different claims. If I were starting over I'd budget
explicit time for a real-browser pass rather than treating headless
verification as equivalent to it.

**The `_explain` / `_describe_swaps` private-helper naming was a minor
self-inflicted wound.** Both were written as CLI-only internal helpers
(leading underscore, "this is just for `predict()`/`run()`"), then needed
promoting to public functions the moment the Streamlit pages wanted to
reuse the exact same explanation logic instead of duplicating it. Mildly
annoying, but raised an actual question worth flagging for the next
project: if a function computes domain logic (not CLI-formatting), default
to a public name even when there's only one caller today — the CLI itself
is rarely the only consumer for long once a feature has any value.

**I'd build the data-quality JSON report and Streamlit consumer in the same
pass next time.** They ended up in the same Phase 7 batch here, which
worked out, but `data_quality.py` originally only printed to stdout and
exited — meaning the "Pipeline Health" page idea couldn't exist until the
report-writing refactor happened. Designing the machine-readable output
format (even a stub) at the same time as the human-readable CLI output
would have saved a structural change later.

**The architecture diagram and design docs drifted from the implementation
in small ways** (e.g. the original design assumed App 3 would trigger off
`football.events.live`; the actual implementation correctly uses
`football.fixtures.raw`, the only topic that carries match status — a
detail only discovered while building, not while designing). Worth a
deliberate "reconcile docs against what got built" pass at the end of each
phase rather than only at the very end of the project, so the gap doesn't
compound across 7 phases.

## What's next (beyond this project)

- Swap every synthetic-data bridge for the real thing once a RapidAPI key
  is producing live fixtures at volume — the explicit point of building
  each bridge to match the real schema/statistics was to make this swap a
  data-loader change, not a redesign.
- Re-run the App 2 model-selection backtest once real fixtures exist; the
  ELO-only-baseline-wins result is a property of the synthetic data's
  feature redundancy, not a law of nature, and may well flip.
- Model monitoring / drift detection on the odds predictor, multi-source
  ingestion, and a cloud deployment (MSK/Kinesis + EMR/Glue or Databricks)
  remain stretch ideas — see [`roadmap.md`](roadmap.md)'s stretch list.

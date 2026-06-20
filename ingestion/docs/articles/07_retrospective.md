# What I learned building a real-time football AI platform end to end

By the start of Phase 7, all three apps and the full medallion pipeline
were already built and verified — Phases 1 through 6 had shipped Kafka
ingestion, a Bronze/Silver/Gold lakehouse, and three working ML apps. What
was missing wasn't more pipeline, it was the operational layer around it:
a way to *run* it on a schedule instead of by hand, a way to *see* whether
it's healthy without reading Spark logs, and a way to *use* the three apps
without a terminal. This last write-up covers that polish phase, and then
the retrospective — what actually worked, and what I'd do differently next
time.

## Orchestration, kept boring on purpose

`orchestration/dags/` holds two Airflow DAGs, and the design choice I'm
most satisfied with here is how little logic lives in them. Every task is
a `BashOperator` shelling out to the exact module or script a developer
would run by hand — `medallion_pipeline` (`@daily`) runs the four
ingestion producers in parallel, then `bronze_ingest` → `silver_transform`
→ `gold_aggregate` → `data_quality`, with the last task doubling as a
quality gate for free, since it already exits non-zero on any failed
check. `match_odds_model_retrain` (`@weekly`) re-trains and re-backtests
App 2's model via the same `train.py` from Phase 4, which already deploys
whichever model wins the backtest. No pipeline logic got reimplemented
inside a DAG.

The decision I'd flag as the most transferable lesson from this phase:
Airflow runs in its own venv (`/opt/airflow-venv`), completely separate
from the Spark/ML venv, and Airflow's own process never imports `pyspark`
— it just invokes the other venv's Python via a `FIFA_PYTHON_BIN`
environment variable. Airflow's dependency tree is large and easy to
break against an existing data-science stack, and this split meant adding
orchestration in Phase 7 touched nothing from Phases 1-6. I verified both
DAGs with `airflow dags list-import-errors` (zero errors) and
`airflow tasks test` against the real pipeline — the fixtures task
actually connected to the local Kafka broker and published real messages,
the retrain task actually ran the real training script and saved a real
model file. Not a dry run; the real thing, scheduled.

## Observability and the UI

`data_quality.py` got refactored to split its 52-check core into
`run_checks()` — reusable, no printing or exiting — so the CLI, the
Airflow DAG, and a new JSON report writer could all share it.
`write_report()` snapshots every run to `gold/data_quality_report.json`,
and `notify_on_failure()` posts to a Slack webhook if one's configured,
logging a plain warning otherwise. Running it against the real lakehouse
produced 48 passed / 4 failed — all 4 failures were freshness checks on
data older than the 25-hour threshold, which is correct behavior given
how long the data had been sitting there, not a bug.

The Streamlit app (`app/`) puts all of that behind a UI: one shared Spark
session and one loaded model across pages via `st.cache_resource`, four
pages covering the three apps plus a Pipeline Health page that renders the
JSON report directly with no Spark session of its own. I also added a
public demo path for Streamlit Community Cloud, since a free hosted
container has no Java and no Kafka — `get_cached_spark()` now returns
`None` gracefully when a real session can't start, and every lookup falls
back to a static snapshot built by replaying the synthetic generator's
exact ELO-update loop and seed, so the hosted demo's numbers come from the
same math as the real Gold layer, not a separate fake.

## What worked

The Medallion split earned its keep immediately — when the mock
API-Football payloads turned out to have quirks (nested optional fields,
inconsistent player-stats shapes), fixing it once in `silver_transform.py`
fixed it for every app at once, instead of three apps each carrying their
own JSON-parsing bugs.

The synthetic-data bridge pattern paid for itself three times over, and
I'd make the same call again. Every app — `synthetic_data.py`,
`synthetic_squad_data.py`, `structure.py`'s fictional teams — got built to
the same statistical rules as the real Gold tables, which meant the real
swap-in path is "replace the generator, keep everything downstream
unchanged." That wasn't just a nice theory: when `ml/tournament_predictor`
started getting real recorded results fed into it through `state.py`,
`simulate.py` needed zero changes to consume them.

And the backtest in Phase 4 caught me almost picking the wrong model for
the wrong reason. My instinct going in was "gradient boosting obviously
beats a logistic-regression baseline" — and the walk-forward backtest said
otherwise, because once ELO converges it already captures most of the
team-strength signal, and the extra rolling-form features mostly added
variance, not information. `train.py` deploys whichever the backtest
actually favors. That's a small mechanical detail, but it's the difference
between a project that demos a metric and one that demos a discipline.

## What I'd change

If I were starting the UI work over, I'd reach for
`streamlit.testing.v1.AppTest` from day one rather than as an afterthought.
Building in an environment with no browser meant verification leaned on
`AppTest` to run every page headlessly and assert no exception — which
worked, and even caught a real deprecation warning
(`use_container_width` → `width=`) — but it can't catch layout problems.
"The page renders correctly" and "the page looks right" are still two
different claims, and I'd budget explicit time for a real-browser pass
next time rather than treating headless verification as a substitute for
it.

The `_explain`/`_describe_swaps` naming was a small self-inflicted wound
worth naming because the lesson generalizes. Both were written as
CLI-only private helpers — leading underscore, "this is just for
`predict()`/`run()`" — and then both needed promoting to public functions
the moment the Streamlit pages wanted the exact same explanation logic
instead of duplicating it. The actual lesson: if a function computes
domain logic rather than CLI formatting, default to a public name even
when there's only one caller today, because the CLI is rarely the only
consumer for long once a feature has any value.

I'd also design the data-quality JSON report and its Streamlit consumer in
the same pass next time. They landed in the same Phase 7 batch here and it
worked out, but `data_quality.py` originally only printed to stdout and
exited — so the Pipeline Health page idea literally couldn't exist until
the report-writing refactor happened. Sketching the machine-readable
output format, even as a stub, at the same time as the human-readable CLI
output would have avoided that structural detour.

Last: the architecture diagram and design docs drifted from the
implementation in small ways across the project — the clearest example is
that the original design assumed App 3 would trigger off
`football.events.live`, when the actual implementation correctly needed
`football.fixtures.raw`, the only topic that carries match status. That
gap was only discovered while building the live consumer in Phase 6, not
while designing it back in Phase 0. It got reconciled in the Phase 7 docs
pass, but it sat wrong for six phases. Next project, I'd build in a
deliberate "reconcile docs against what got built" pass at the end of each
phase, so the drift doesn't compound.

## What's next, beyond this project

The honest next step is swapping every synthetic-data bridge for the real
thing once a RapidAPI key is producing live fixtures at volume — that was
the explicit point of building each bridge to match the real schema and
statistics in the first place. Worth re-running the Phase 4 model-selection
backtest once real data exists, too — the ELO-only-baseline-wins result is
a property of this synthetic data's feature redundancy, not a law of
nature, and it may well flip once the inputs are real. Model
monitoring/drift detection on the odds predictor, multi-source ingestion,
and an actual cloud deployment (MSK/Kinesis + EMR/Glue or Databricks)
remain on the stretch list.

That's the whole build, eight phases, three apps, one recurring honest
constraint, and a few lessons I'm carrying into whatever I build next.

## LinkedIn version

Closing out an 8-part build log: a real-time football data platform, start
to finish — Kafka, Spark Structured Streaming, a Bronze/Silver/Gold
lakehouse, 3 ML apps, Airflow orchestration, a Streamlit UI.

What worked:
- The medallion split meant fixing a messy payload quirk once fixed it for
  all 3 downstream apps at once
- The synthetic-data bridge pattern meant the tournament app needed zero
  code changes once real recorded results started flowing in
- A walk-forward backtest caught my own bias — I assumed gradient boosting
  would beat an ELO-only baseline; it didn't, and the harness picked the
  real winner instead of the fancier-sounding one
- Keeping Airflow in its own venv meant orchestration touched zero code
  from Phases 1-6

What I'd change:
- Headless `AppTest` verification caught real bugs but can't catch layout
  issues — I'd still budget a real-browser pass
- Promoted two "private" CLI helpers to public the moment the UI needed to
  reuse them — should have defaulted to public from the start
- Docs drifted from the build in small ways that a per-phase "reconcile"
  pass would have caught sooner

Full retrospective, with the backtest numbers and the rest of the lessons:
[link to Medium article]

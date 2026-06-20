# I'm building a real-time football data platform — here's the architecture

I started this project with a simple goal: build a data platform end to end,
the way you'd actually build one at work, except the domain is football
instead of whatever's on a real company's roadmap. Kafka producers feeding a
Medallion lakehouse, three ML apps sitting on top of it, and enough
orchestration and observability that "it runs once on my laptop" turns into
"it runs on a schedule and tells you when it's unhealthy." This first
write-up is about the architecture I committed to before writing a line of
pipeline code, and why.

## The shape of the problem

The data source is API-Football, a REST API (via RapidAPI) that exposes
fixtures, live events, lineups, player stats, and standings. The free tier
gives you 100 requests/day, rate-limited to roughly 10/minute — which is
its own design constraint I'll get into in the next write-up. For Phase 0,
the part that mattered was: this is inherently an event-driven domain. A
match generates events (goals, cards, subs) in real time, fixtures and
standings update on a slower cadence, and downstream consumers (a match-odds
model, a squad optimizer, a tournament simulator) all want different slices
and freshness levels of the same underlying data. That's a Kafka-shaped
problem, not a "cron job hits an API and writes a CSV" problem, even at toy
scale.

So before any code, I wrote the architecture doc and the roadmap, and
sketched the table contracts. The pipeline I committed to:

```
API-Football -> Python producers -> Kafka -> Spark Structured Streaming
  -> Bronze (raw) -> Silver (conformed) -> Gold (features) -> 3 ML apps
  -> CLI / Streamlit -> Airflow schedules the batch hops
```

Five Kafka topics, one per logical entity (`football.fixtures.raw`,
`football.events.live`, `football.lineups.raw`, `football.standings.raw`,
`football.player_stats.raw`), each with its own partition count and
retention window depending on volume and replay needs. A Medallion
lakehouse — Bronze/Silver/Gold — sitting on Delta Lake, with each layer's
contract written down in `medallion/README.md` before any table existed:
`silver.dim_team`, `silver.dim_player`, `silver.fact_match`,
`silver.fact_player_match_stat`, and the Gold feature tables
(`gold.team_form_features`, `gold.elo_ratings`, `gold.head_to_head_features`,
`gold.player_season_stats`, `gold.match_prediction_features`). And three
apps designed against those Gold tables: a Squad Optimizer, a Match Odds
Predictor, and a Live Tournament Predictor — each with its own design doc
under `docs/apps/` before any model code.

## The one real decision: Spark Structured Streaming over Flink

The most consequential choice in this phase wasn't a table schema, it was
the processing engine. Flink is the more "correct" answer for genuinely
low-latency, event-at-a-time streaming, and I considered it. I went with
Spark Structured Streaming instead, for two reasons that had nothing to do
with which engine is theoretically better.

First, the actual cadence of this data doesn't need millisecond latency.
Live match events arrive on the order of seconds; fixtures and standings
update every 15-30 minutes. Structured Streaming's micro-batch model is a
comfortable fit for that cadence — I'm not trying to process a sub-second
event stream, I'm trying to process a few-seconds-to-minutes stream
correctly and replayably. Second, keeping the stack to one processing
engine matters more once you account for the fact that this is a solo
project built around a broader skills roadmap that already invests heavily
in Spark internals and tuning. Adding Flink would mean context-switching
between two streaming engines' mental models for not much practical gain
at this data volume. Flink stays as a documented comparison/theory topic
rather than a second engine to operate. (I do plan a short Spark-vs-Flink
write-up later, since "why not the trendier choice" is a more interesting
engineering story than "I used the trendier choice.")

The other early decision baked into the architecture: every table contract,
every topic, every app design got written down *before* implementation.
That's not a process flex — it's the thing that let later phases move fast,
because by the time I was writing `gold_aggregate.py` in Phase 3, the shape
of `match_prediction_features` was already settled, and the three app specs
in `docs/apps/` already told me what feature columns App 2's model would
need. Design docs that say "design only, not yet implemented" up front,
then get checked off phase by phase, turned out to be worth the time.

## The honest gap I designed around, not into

There's a pattern that runs through this whole project that I want to name
here at the start rather than bury later: the real API-Football pipeline,
even with a working mock mode, only ever produces a handful of rows. One or
two fixtures, a handful of events, four teams. That's not nearly enough to
train a classifier, optimize a lineup from a full squad, or simulate an
eight-team tournament. I knew this going into Phase 0, and the architecture
reflects it — every app's design doc, from the very first draft, explicitly
calls out a synthetic-data path built to the same statistical and schema
rules as the real Gold tables, so that swapping in real data later is a
data-loader change, not a redesign. I'd rather an architecture doc admit
that constraint on day one than have it discovered as a surprise three
phases in.

## What's next

One inaccuracy did sneak into this initial design that I'll flag now since
it's a good example of why "design doc" and "what got built" are two
different documents: the original architecture assumed the Live Tournament
Predictor would trigger off `football.events.live`. It turned out, once I
actually built that consumer in Phase 6, that match status only ever lives
in `football.fixtures.raw` — `events.live` has no status field at all. The
diagram and docs got corrected in Phase 7, but the gap existed for a while.
More on that later.

Next up: the ingestion layer itself — the Kafka producers, the topic
design, and what it's actually like building against a free-tier API with
a 100-requests/day budget.

## LinkedIn version

Kicking off a public build log: a real-time football data platform, built
the way you'd build one at work — Kafka, Spark Structured Streaming, a
Bronze/Silver/Gold lakehouse, three ML apps, orchestration, and a
Streamlit UI.

This first post covers the architecture decisions made before writing any
pipeline code:

- 5 Kafka topics, one per entity, each with its own partition count and
  retention based on access pattern
- A Medallion lakehouse with every table contract written down before
  implementation — Bronze (raw/replayable), Silver (conformed), Gold
  (features)
- Spark Structured Streaming over Flink — not because Flink is worse, but
  because this data's cadence (seconds to minutes) doesn't need sub-second
  latency
- Three app specs designed against Gold tables before any model code
  existed
- An upfront design call: the real pipeline only ever produces a handful
  of rows, so every app gets a documented synthetic-data bridge built to
  the same rules as the real data

Full write-up with the architecture diagram: [link to Medium article]

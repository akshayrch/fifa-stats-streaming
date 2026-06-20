# Articles index

These are publish-ready **drafts** of the 8 Medium write-ups (and their
LinkedIn teaser versions) planned in [`../roadmap.md`](../roadmap.md), one
per project phase. Every fact, file name, and number in them was pulled
directly from this repo's own docs — `progress.md`, `architecture.md`, and
`retrospective.md` — not invented. Before posting, the author should:

- Add their own screenshots / diagram renders / demo GIFs where useful
  (e.g. the architecture diagram, the Streamlit app, sample CLI output)
- Swap in their own voice/anecdotes where it reads generic
- Replace the literal `[link to Medium article]` placeholder in each
  LinkedIn version with the real published URL once the Medium article is
  live
- Double-check anything time-sensitive (the project's actual posting dates)
  since these drafts don't assume a specific calendar

Suggested cadence, matching the project's stated "weekly LinkedIn updates +
Medium write-ups track progress phase by phase" pattern: one phase per
week, LinkedIn teaser and Medium article posted the same week, in the order
below.

| Week | File | Title | Summary |
|---|---|---|---|
| 1 | [`00_architecture.md`](00_architecture.md) | I'm building a real-time football data platform — here's the architecture | Lays out the Kafka -> Spark -> Medallion -> 3-apps architecture and the Spark-over-Flink decision, made before any pipeline code existed. |
| 2 | [`01_kafka_producer.md`](01_kafka_producer.md) | Building a Kafka producer for live football data — topics, partitions, and the rate-limit dance with a free API | Covers the 5-topic/partition-key design, the 4 producers, the free-tier 100-requests/day budget, and the Docker Hub rate-limit workaround. |
| 3 | [`02_bronze_layer.md`](02_bronze_layer.md) | From Kafka to Lakehouse: Spark Structured Streaming into a Bronze layer | Walks through `bronze_ingest.py`, the no-Docker local lakehouse path, checkpointing per topic, and verifying exactly-once replay/resume semantics. |
| 4 | [`03_medallion.md`](03_medallion.md) | Designing a Medallion architecture for football stats: Bronze, Silver, Gold | Details `silver_transform.py`'s MERGE-upsert pattern and `gold_aggregate.py`'s ELO/form/H2H feature computation, plus the 52-check data quality suite. |
| 5 | [`04_match_odds.md`](04_match_odds.md) | Predicting match odds from a real-time feature store | Introduces the synthetic-data bridge pattern and the walk-forward backtest that surprised the author — an ELO-only baseline beating gradient boosting. |
| 6 | [`05_squad_optimizer.md`](05_squad_optimizer.md) | Optimizing a starting XI with constraint programming + ML | Covers the two-stage contribution-scoring + PuLP optimization design, and reusing App 2's prediction function instead of rebuilding it. |
| 7 | [`06_tournament_predictor.md`](06_tournament_predictor.md) | Real-time tournament simulation triggered by live match events | Covers the fictional-teams bridge, the neutral-venue bias fix, scoreline sampling for tiebreaks, and the live Kafka-triggered re-simulation. |
| 8 | [`07_retrospective.md`](07_retrospective.md) | What I learned building a real-time football AI platform end to end | The project wrap-up: orchestration/observability/UI polish, what worked, what the author would change, and what's next. |


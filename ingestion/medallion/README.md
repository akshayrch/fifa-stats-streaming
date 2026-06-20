# Medallion Lakehouse — Table Contracts

Storage: Delta Lake tables on MinIO/S3 (`s3a://fifa-lakehouse/<layer>/<table>`).
All tables partitioned by `ingest_date` (derived from `ingest_ts`) unless
noted otherwise.

## Bronze — raw, append-only

One table per Kafka topic. No transformation beyond capturing Kafka metadata.

| Table | Source topic | Schema |
|---|---|---|
| `bronze.fixtures_raw` | `football.fixtures.raw` | `key`, `value` (raw JSON string), `topic`, `partition`, `offset`, `kafka_ts`, `ingest_ts` |
| `bronze.events_raw` | `football.events.live` | same shape |
| `bronze.lineups_raw` | `football.lineups.raw` | same shape |
| `bronze.standings_raw` | `football.standings.raw` | same shape |
| `bronze.player_stats_raw` | `football.player_stats.raw` | same shape |

**Contract**: never delete or mutate. This is the replay source of truth —
Silver/Gold can always be rebuilt from Bronze.

## Silver — conformed dimensions & facts

Parsed, typed, deduplicated. Standard Kimball-style dim/fact split.

### Dimensions

| Table | Grain | Key columns |
|---|---|---|
| `silver.dim_team` | one row per team | `team_id`, `name`, `country`, `founded`, `venue_id` |
| `silver.dim_player` | one row per player | `player_id`, `name`, `birth_date`, `nationality`, `position` |
| `silver.dim_league` | one row per league/season | `league_id`, `season`, `name`, `country`, `type` |
| `silver.dim_venue` | one row per venue | `venue_id`, `name`, `city`, `capacity` |

### Facts

| Table | Grain | Key columns |
|---|---|---|
| `silver.fact_match` | one row per fixture | `fixture_id`, `league_id`, `season`, `home_team_id`, `away_team_id`, `kickoff_ts`, `status`, `home_goals`, `away_goals`, `venue_id` |
| `silver.fact_match_event` | one row per match event | `fixture_id`, `event_id`, `minute`, `team_id`, `player_id`, `type` (goal/card/sub/var), `detail` |
| `silver.fact_player_match_stat` | one row per player per fixture | `fixture_id`, `player_id`, `team_id`, `minutes`, `rating`, `goals`, `assists`, `shots`, `passes`, `tackles`, ... |
| `silver.fact_standings_snapshot` | one row per team per league per snapshot date | `league_id`, `season`, `team_id`, `snapshot_date`, `rank`, `points`, `played`, `won`, `draw`, `lost`, `gf`, `ga` |

**Contract**: deduplicated on natural key (`fixture_id` + entity key),
late-arriving updates handled via `MERGE` (upsert) keyed on natural key +
latest `ingest_ts` wins.

## Gold — features & aggregates (feeds the 3 apps)

| Table | Grain | Description | Used by |
|---|---|---|---|
| `gold.team_form_features` | team x as-of-date | Rolling last-5/10 form: PPG, GF/GA, home/away splits | App 1, 2, 3 |
| `gold.elo_ratings` | team x as-of-date | Incrementally updated ELO rating after each result | App 2, 3 |
| `gold.head_to_head_features` | team pair x as-of-date | Historical H2H record, venue-adjusted | App 2 |
| `gold.player_season_stats` | player x season | Aggregated per-player stats + recent-form trend | App 1 |
| `gold.match_prediction_features` | fixture_id (upcoming) | Joined feature row ready for App 2's model | App 2 |
| `gold.tournament_structure` | tournament reference data | Groups, fixtures, knockout rules | App 3 |
| `gold.tournament_state` | tournament x as-of-timestamp | Live standings + simulated qualification/win probabilities | App 3 |

**Contract**: Gold tables are derived/recomputable from Silver at any time;
`gold.tournament_state` additionally has a low-latency "live" path (see
[App 3 design](../docs/apps/03_tournament_predictor.md)).

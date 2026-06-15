# Data Sources

## Primary: API-Football (via RapidAPI)

- **Site**: https://www.api-football.com/
- **Access**: via RapidAPI — free tier is **100 requests/day**, rate-limited
  to ~10 req/min on the free plan. Paid tiers ("Pro" ~$X/mo) raise this
  significantly and are cheap enough to consider once the pipeline is proven
  on the free tier.
- **Auth**: `x-rapidapi-key` header (store in `ingestion/config/settings.yaml`,
  **never commit this file** — it's gitignored).

### Endpoints we care about

| Endpoint | Maps to topic | Notes |
|---|---|---|
| `/fixtures` | `football.fixtures.raw` | Filter by `league`, `season`, `date` or `next`/`live` |
| `/fixtures/events` | `football.events.live` | Goals, cards, subs, VAR — per fixture |
| `/fixtures/lineups` | `football.lineups.raw` | Starting XI, formation, coach |
| `/fixtures/players` | `football.player_stats.raw` | Per-player match stats (shots, passes, rating, etc.) |
| `/standings` | `football.standings.raw` | League table snapshot |
| `/odds` | `football.odds.raw` | Pre-match odds (availability varies by plan) |
| `/teams/statistics` | feeds Gold `team_form_features` | Aggregated team stats per league/season |

### Rate-limit strategy (free tier = 100 req/day)

With 100 requests/day, we can't poll constantly. Strategy for the MVP:

1. **Pick 1-2 leagues** to track (e.g., Premier League + a major international
   competition during a tournament window — the World Cup / Euros / Copa
   América are great "real-time" demo windows).
2. **Batch the daily budget**:
   - ~10 requests: fixtures for the day / next 7 days
   - ~10 requests: standings (once or twice a day)
   - ~10 requests: lineups (only for fixtures kicking off soon)
   - Remaining ~70 requests: reserved for live-event polling on match days
     (e.g., every 60-90 sec during a live match window)
3. On non-match days, the pipeline naturally idles — which is fine, and worth
   noting in the architecture write-up as a real-world "cost-aware ingestion"
   design decision.
4. Cache aggressively — never re-fetch fixtures/standings that haven't
   changed (`If-Modified-Since` / local TTL cache).

## Secondary / fallback options (documented for future multi-source work)

- **football-data.org** — free tier covers top European leagues (fixtures,
  results, standings, scorers). Good cross-validation source, lower detail
  than API-Football (no player-level match stats).
- **TheSportsDB** — free, simple REST API, good for static reference data
  (team metadata, logos, venues) to enrich dimension tables.

## Reference data (one-time / low-frequency loads)

Team/player metadata, logos, and venue info change rarely — these are good
candidates for a small batch/reference loader rather than streaming, landing
directly in Silver `dim_team` / `dim_player` / `dim_venue`.

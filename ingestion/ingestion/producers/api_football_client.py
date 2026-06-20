"""Thin REST client for API-Football (RapidAPI) with daily rate-limit
tracking and retry/backoff.

Free tier = 100 requests/day, ~10/min. See docs/data_sources.md for the
polling-budget strategy. This client tracks a simple in-memory daily request
counter; for production use, persist the counter (e.g. in Kafka, a small
SQLite file, or Redis) so it survives restarts.
"""

from __future__ import annotations

import json
import logging
import pathlib
from datetime import date

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

SAMPLE_DATA_DIR = pathlib.Path(__file__).resolve().parent.parent / "sample_data"

# Maps an API endpoint to its sample-data fixture file, used when mock=True.
ENDPOINT_SAMPLE_FILES = {
    "fixtures": "fixtures_response.json",
    "standings": "standings_response.json",
    "fixtures/lineups": "lineups_response.json",
    "fixtures/events": "events_response.json",
    "fixtures/players": "player_stats_response.json",
}


class RateLimitExceeded(Exception):
    pass


class APIFootballClient:
    def __init__(self, base_url: str, rapidapi_key: str, rapidapi_host: str,
                 daily_request_budget: int = 100, mock: bool = False):
        self.base_url = base_url.rstrip("/")
        self.daily_request_budget = daily_request_budget
        self.mock = mock
        self._headers = {
            "x-rapidapi-key": rapidapi_key,
            "x-rapidapi-host": rapidapi_host,
        }
        self._request_date = date.today()
        self._request_count = 0

    def _check_budget(self) -> None:
        today = date.today()
        if today != self._request_date:
            self._request_date = today
            self._request_count = 0
        if self._request_count >= self.daily_request_budget:
            raise RateLimitExceeded(
                f"Daily request budget ({self.daily_request_budget}) exhausted for {today}"
            )

    def _get_mock(self, endpoint: str, params: dict | None = None) -> dict:
        endpoint = endpoint.strip("/")
        # live=all fixtures get their own fixture file so producers can be
        # exercised against an "in-progress match" scenario.
        if endpoint == "fixtures" and (params or {}).get("live") == "all":
            filename = "live_fixtures_response.json"
        else:
            filename = ENDPOINT_SAMPLE_FILES.get(endpoint)

        if filename is None:
            raise ValueError(f"No sample data registered for endpoint '{endpoint}'")

        with open(SAMPLE_DATA_DIR / filename) as f:
            body = json.load(f)
        logger.debug("MOCK %s params=%s -> %s", endpoint, params, filename)
        return body

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def get(self, endpoint: str, params: dict | None = None) -> dict:
        """GET <base_url>/<endpoint> with the configured RapidAPI headers.

        Raises RateLimitExceeded if the daily budget is exhausted, and
        retries transient HTTP/network errors with exponential backoff.

        If `mock=True`, returns canned responses from ingestion/sample_data/
        instead of calling the real API -- useful for exercising the
        ingestion pipeline without consuming the free-tier daily budget.
        """
        if self.mock:
            return self._get_mock(endpoint, params)

        self._check_budget()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, headers=self._headers, params=params or {}, timeout=15)
        self._request_count += 1
        response.raise_for_status()
        body = response.json()

        if body.get("errors"):
            logger.warning("API-Football returned errors for %s: %s", endpoint, body["errors"])

        return body

    # Convenience wrappers
    def get_fixtures(self, league_id: int, season: int, **params) -> dict:
        return self.get("fixtures", {"league": league_id, "season": season, **params})

    def get_live_fixtures(self, league_id: int) -> dict:
        return self.get("fixtures", {"league": league_id, "live": "all"})

    def get_lineups(self, fixture_id: int) -> dict:
        return self.get("fixtures/lineups", {"fixture": fixture_id})

    def get_standings(self, league_id: int, season: int) -> dict:
        return self.get("standings", {"league": league_id, "season": season})

    def get_player_stats(self, fixture_id: int) -> dict:
        return self.get("fixtures/players", {"fixture": fixture_id})

    def get_events(self, fixture_id: int) -> dict:
        return self.get("fixtures/events", {"fixture": fixture_id})

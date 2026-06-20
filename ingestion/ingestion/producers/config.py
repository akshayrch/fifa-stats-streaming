"""Loads ingestion settings from ingestion/config/settings.yaml."""

from __future__ import annotations

import pathlib

import yaml

CONFIG_DIR = pathlib.Path(__file__).resolve().parent.parent / "config"
SETTINGS_PATH = CONFIG_DIR / "settings.yaml"
EXAMPLE_PATH = CONFIG_DIR / "settings.example.yaml"


def load_settings() -> dict:
    path = SETTINGS_PATH if SETTINGS_PATH.exists() else EXAMPLE_PATH
    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(
            f"{SETTINGS_PATH} not found. Copy {EXAMPLE_PATH} to {SETTINGS_PATH} "
            "and fill in your API-Football credentials."
        )
    with open(path) as f:
        return yaml.safe_load(f)

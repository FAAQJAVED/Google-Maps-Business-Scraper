"""
Configuration loading, deep-merging with defaults, and validation.

Loads config.yaml, fills in any missing keys from DEFAULTS, validates
required fields, and returns a single clean config dict used everywhere.
"""

from __future__ import annotations

import os
import re as _re
from pathlib import Path
from typing import Any

import yaml


# ── Defaults ──────────────────────────────────────────────────────────────────
# Every key here may be omitted from the user's config.yaml.
# User values always take priority over these.

DEFAULTS: dict[str, Any] = {
    "search": {
        "query":    "",
        "location": "",
        "mode":     "city",
    },
    "geography": {
        "lat_min": 0.0,
        "lat_max": 0.0,
        "lng_min": 0.0,
        "lng_max": 0.0,
        "region_zones":            [],
        "valid_postcode_prefixes": [],
        "postcode_pattern":        r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*\d[A-Z]{2}\b",
    },
    # Phone defaults are intentionally empty — the scraper accepts any phone
    # number format when these are blank. Set country_code, valid_prefixes,
    # and valid_lengths in your config.yaml for clean, validated output.
    # See config.yaml for examples for UK, US, Germany, Australia, and France.
    "phone": {
        "country_code":     "",
        "valid_prefixes":   [],
        "preferred_prefix": "",
        "valid_lengths":    [],
    },
    "stealth": {
        "proxies":      [],
        "rotate_every": 10,   # rotate proxy every N completed queries (0 = on failure only)
    },
    "captcha": {
        "human_solve": True,
    },
    "classification": {
        "keywords": {},
    },
    "filters": {
        "skip_domains": [
            "facebook.com","twitter.com","x.com","linkedin.com",
            "instagram.com","youtube.com","google.com","bing.com",
            "wikipedia.org","trustpilot.com","tripadvisor.com",
            "yelp.com","yell.com",
        ],
        "junk_emails": [
            "user@domain.com","test@test.com","email@example.com",
            "info@example.com","name@domain.com","example@example.com",
            "no-reply@no-reply.com","admin@admin.com",
        ],
        "junk_email_domains": [
            "sentry.io","wixpress.com","example.com","schema.org",
            "w3.org","googleapis.com","cloudflare.com","jquery.com",
            "gravatar.com","noreply.com",
        ],
    },
    "performance": {
        "headless":               True,
        "browser_channel":        "chrome",   # "chrome" = your installed Google Chrome (recommended)
                                              # "chromium" = Playwright's built-in (fallback)
        "scroll_pause":           1.5,
        "slow_connection_wait":   25.0,
        "max_stalls":             5,          # consecutive stall periods before end-of-results
        "request_delay":          0.3,
        "fetch_threads":          15,
        "http_timeout":           [2, 6],
        "hard_timeout":           8,
        "browser_restart_every":  300,
    },
    "output": {
        "format":          "csv",
        "directory":       "output",
        "filename_prefix": "MapsScrape",
    },
    "scheduling": {
        "stop_at":    None,
        "disk_min_mb": 500,
    },
    "files": {
        "checkpoint":   "scraper_checkpoint.json",
        "done_queries": "scraper_done_queries.txt",
        "command_file": "command.txt",
        "log_dir":      "logs",
    },
}


# ── Public API ────────────────────────────────────────────────────────────────

def load_config(path: str | Path = "config.yaml") -> dict[str, Any]:
    """
    Load config.yaml, merge with defaults, and validate required fields.

    Args:
        path: Path to the YAML config file.

    Returns:
        Merged and validated config dict.

    Raises:
        FileNotFoundError: Config file does not exist.
        ValueError: Required field is missing or empty.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: '{path}'\n"
            "Edit config.yaml and fill in the REQUIRED fields at the top."
        )

    with open(path, encoding="utf-8") as f:
        user_cfg: dict = yaml.safe_load(f) or {}

    cfg = _deep_merge(DEFAULTS, user_cfg)
    _validate(cfg)

    # Ensure output directory exists
    os.makedirs(cfg["output"]["directory"], exist_ok=True)
    return cfg


# ── Internals ─────────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base, returning a new dict."""
    result = base.copy()
    for key, value in override.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _validate(cfg: dict) -> None:
    """Raise ValueError for any missing or empty required field."""
    q = cfg.get("search", {}).get("query", "")
    if not q or not str(q).strip():
        raise ValueError(
            "Required field 'search.query' is empty.\n"
            "Open config.yaml and set query: \"your search term\""
        )

    loc = cfg.get("search", {}).get("location", "")
    if not loc or not str(loc).strip():
        raise ValueError(
            "Required field 'search.location' is empty.\n"
            "Open config.yaml and set location: \"your city\""
        )

    fmt = cfg["output"].get("format", "csv").lower()
    if fmt not in ("csv", "excel"):
        raise ValueError(
            f"'output.format' must be 'csv' or 'excel', got '{fmt}'."
        )
    cfg["output"]["format"] = fmt

    stop_at = cfg.get("scheduling", {}).get("stop_at")
    if stop_at is not None:
        if not _re.match(r"^\d{2}:\d{2}$", str(stop_at)):
            raise ValueError(
                f"'scheduling.stop_at' must be HH:MM 24-hour format (e.g. '06:30'), "
                f"got '{stop_at}'. Zero-pad single-digit hours: use '09:00' not '9:00'."
            )

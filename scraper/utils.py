"""
Shared utility functions used across all scraper modules.

Covers: elapsed time, phone cleaning, disk check, stop-time check,
cross-platform audio alerts, and random human-timing helpers.
"""

from __future__ import annotations

import logging
import random
import re
import shutil
import sys
import time
from datetime import datetime, timedelta
from typing import Optional


# ── Time ──────────────────────────────────────────────────────────────────────

def elapsed(start: float) -> str:
    """Return formatted elapsed time string, e.g. '[4m07s]'."""
    s = int(time.time() - start)
    return f"[{s // 60}m{s % 60:02d}s]"


def jitter(base: float, pct: float = 0.25) -> float:
    """
    Return base ± (pct * base) seconds.

    Fixed delays are a bot-detection signal. Adding small random variation
    makes timing look human without meaningfully slowing the scraper.

    Args:
        base: Centre value in seconds.
        pct:  Fraction of base to vary by (default ±25%).
    """
    delta = base * pct
    return max(0.05, random.uniform(base - delta, base + delta))


# ── Phone cleaning ─────────────────────────────────────────────────────────────

def clean_phone(
    raw: str,
    country_code: str = "",
    valid_prefixes: list[str] | None = None,
    valid_lengths: list[int] | None = None,
) -> str:
    """
    Normalise a phone number to a local digit-only string and validate it.

    Strips the country dialing code if present, removes all non-digit
    characters, and checks the result against optional prefix and length rules.

    Args:
        raw:            Raw phone string (from Maps or a website).
        country_code:   Numeric dialing code without '+', e.g. '44'. Empty = no stripping.
        valid_prefixes: List of accepted first-N-digit sequences. Empty = accept all.
        valid_lengths:  List of accepted digit counts. Empty = accept all.

    Returns:
        Normalised digit string, or empty string if invalid.
    """
    if not raw:
        return ""
    raw = str(raw).strip()

    if country_code:
        plus_cc = f"+{country_code}"
        zero_cc = f"00{country_code}"
        if raw.startswith(plus_cc):
            raw = "0" + raw[len(plus_cc):]
        elif raw.startswith(zero_cc):
            raw = "0" + raw[len(zero_cc):]

    digits = re.sub(r"[^\d]", "", raw)

    if valid_lengths and len(digits) not in valid_lengths:
        return ""
    if valid_prefixes and not any(digits.startswith(p) for p in valid_prefixes):
        return ""

    return digits


# ── System checks ─────────────────────────────────────────────────────────────

def check_disk(min_mb: int = 500) -> bool:
    """Return True if free disk space on the working directory meets the minimum."""
    try:
        return shutil.disk_usage(".").free // (1024 * 1024) >= min_mb
    except OSError:
        return True


def check_stop_time(
    stop_at: str | None,
    run_start: Optional[datetime] = None,
    _now: Optional[datetime] = None,
) -> bool:
    """
    Return True if the current clock time has reached or passed stop_at.

    Handles cross-midnight correctly: if stop_at is earlier in the day than
    the run_start time, it is interpreted as the NEXT occurrence of that
    time — i.e. the stop window wraps to the following calendar day.

    Example: run_start=23:30, stop_at="01:00" fires at 01:00 the next day,
    NOT immediately (which a plain string comparison would incorrectly do).

    Args:
        stop_at:   HH:MM 24-hour format string, or None to disable.
        run_start: The datetime when the run began. Pass the actual run start
                   for correct cross-midnight behaviour. Defaults to now.
        _now:      Override for current time (testing only). Defaults to
                   datetime.now() when None.

    Returns:
        True when the stop time has been reached.
    """
    if not stop_at:
        return False
    try:
        now  = _now if _now is not None else datetime.now()
        h, m = map(int, stop_at.split(":"))
        stop_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)

        # Cross-midnight correction: if stop_at is at or before the run start
        # time-of-day, the intended target is the next calendar day occurrence.
        ref = run_start if run_start is not None else now
        ref_tod = ref.replace(second=0, microsecond=0)
        if stop_dt <= ref_tod:
            stop_dt += timedelta(days=1)

        return now >= stop_dt
    except Exception:
        return False


# ── Audio alerts ──────────────────────────────────────────────────────────────

IS_WINDOWS = sys.platform == "win32"

_BEEP_COUNTS: dict[str, int] = {
    "start": 3, "resume": 2, "done": 4,
    "stop": 2,  "error": 1, "alert": 5, "status": 1,
}

# Windows-specific sequences: list of (frequency_hz, duration_ms)
_WIN_BEEPS: dict[str, list[tuple[int, int]]] = {
    "start":  [(500, 100), (700, 100), (900, 100), (1100, 200)],
    "resume": [(600, 150), (900, 250)],
    "done":   [(400, 80),  (600, 80),  (800, 80),  (1000, 80),  (1200, 300)],
    "stop":   [(900, 200), (600, 400)],
    "error":  [(350, 80)],
    "alert":  [(1000, 300), (1000, 300), (1000, 300)],
    "status": [(800, 100)],
}


def beep(kind: str = "error") -> None:
    """
    Emit an audio cue.

    Uses winsound tone sequences on Windows; falls back to the ASCII bell (\\a)
    on macOS and Linux (audible in most terminal emulators).

    Args:
        kind: Named pattern — 'start', 'resume', 'done', 'stop', 'error',
              'alert', 'status'.
    """
    try:
        if IS_WINDOWS:
            import winsound
            for freq, dur in _WIN_BEEPS.get(kind, [(350, 80)]):
                winsound.Beep(freq, dur)
        else:
            count = _BEEP_COUNTS.get(kind, 1)
            sys.stdout.write("\a" * count)
            sys.stdout.flush()
    except Exception:
        pass


# ── Rate-limit backoff ─────────────────────────────────────────────────────────

def backoff_sleep(consecutive_hits: int, base: float = 30.0, cap: float = 300.0) -> None:
    """
    Exponential backoff for rate-limit signals (zero results / captcha).

    Formula: min(base × 2^(hits-1), cap)

    hit 1 → 30 s  |  hit 2 → 60 s  |  hit 3 → 120 s  |  hit 4+ → 300 s

    Args:
        consecutive_hits: Number of consecutive rate-limit hits (1-based).
        base:             Base sleep duration in seconds (default 30 s).
        cap:              Maximum sleep duration in seconds (default 300 s).
    """
    import math as _math
    _log = logging.getLogger("maps_scraper")
    delay = min(base * (2 ** (consecutive_hits - 1)), cap)
    _log.warning("⏳  Rate limit backoff: sleeping %.0fs (hit #%d)", delay, consecutive_hits)
    time.sleep(delay)

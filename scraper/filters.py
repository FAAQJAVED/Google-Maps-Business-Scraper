"""
Geographic filtering, deduplication, and keyword-based category classification.

All logic is config-driven — no hardcoded regions or categories.
"""

from __future__ import annotations

import re
from typing import Any

# CSV injection prefix characters — prepend a single-quote to neutralise them
# when a business name/address starts with these chars and gets opened in Excel.
_CSV_INJECT_RE = re.compile(r'^[=+\-@\t\r]')

_PC_CACHE: dict[str, re.Pattern] = {}


def sanitize_cell(value: str) -> str:
    """
    Prevent CSV formula injection in output cells.

    Excel and LibreOffice Calc treat cells that start with =, +, -, @, tab,
    or carriage-return as formulas. A business whose name is
    '=HYPERLINK("http://evil.com","Click")' would execute when opened.
    Prefixing with a single-quote neutralises this in spreadsheet software
    while leaving the string visually intact in text editors.

    Args:
        value: Raw cell value string.

    Returns:
        Safe string with the injection prefix prepended if necessary.
    """
    if value and _CSV_INJECT_RE.match(value):
        return "'" + value
    return value


def _postcode_re(config: dict[str, Any]) -> re.Pattern | None:
    """Return compiled postcode regex from config (cached per pattern string)."""
    pat = config.get("geography", {}).get("postcode_pattern", "")
    if not pat:
        return None
    if pat not in _PC_CACHE:
        _PC_CACHE[pat] = re.compile(pat)
    return _PC_CACHE[pat]


def extract_postcode(address: str, config: dict[str, Any]) -> str:
    """
    Extract the first postcode / zipcode from an address string.

    Args:
        address: Full address string.
        config:  Full config dict (reads geography.postcode_pattern).

    Returns:
        Postcode string, or empty string if not found.
    """
    rx = _postcode_re(config)
    if not rx:
        return ""
    m = rx.search(address.upper())
    return m.group(0).strip() if m else ""


def make_uid(name: str, address: str) -> str:
    """
    Build a stable, case-insensitive deduplication key.

    Uses a pipe separator so adjacent fields cannot create false-positive
    collisions (e.g. make_uid("AB", "Crest") vs make_uid("A", "BCrest")).

    Args:
        name:    Business display name.
        address: Full address string.

    Returns:
        Lowercase pipe-separated composite key.
    """
    return (name.strip() + "|" + address.strip()).lower()


def rebuild_seen_ids(rows: list[dict]) -> set[str]:
    """
    Reconstruct the deduplication set from previously loaded output rows.

    Uses the same make_uid() call as the live scraper so keys always match
    on resume. Rows with an empty Company Name are skipped — a blank name
    produces a phantom key that could false-match unrelated businesses.

    Args:
        rows: List of row dicts loaded from a previous output file.

    Returns:
        Set of composite UID strings.
    """
    seen: set[str] = set()
    for r in rows:
        name = r.get("Company Name", "").strip()
        if not name:
            continue
        seen.add(make_uid(name, r.get("Address", "")))
    return seen


def is_in_region(place: dict, config: dict[str, Any]) -> bool:
    """
    Return True if the place falls within the configured geographic region.

    Coordinate bounding-box check takes priority over postcode prefix check.
    If neither bounding box nor postcode prefixes are set, accept all results.

    Args:
        place:  Place dict with optional 'lat', 'lng', 'address' keys.
        config: Full config dict.
    """
    geo     = config.get("geography", {})
    lat_min = geo.get("lat_min", 0)
    lat_max = geo.get("lat_max", 0)
    lng_min = geo.get("lng_min", 0)
    lng_max = geo.get("lng_max", 0)

    bbox_active = not (lat_min == lat_max == lng_min == lng_max == 0)

    if not bbox_active:
        return True

    if place.get("lat") and place.get("lng"):
        try:
            return (
                lat_min <= float(place["lat"]) <= lat_max
                and lng_min <= float(place["lng"]) <= lng_max
            )
        except (ValueError, TypeError):
            pass

    address = place.get("address", "")
    if not address:
        return False

    rx = _postcode_re(config)
    if not rx:
        return True

    m = rx.search(address.upper())
    if not m:
        return False

    pm = re.match(r"[A-Z0-9]+", m.group(1))
    if not pm:
        return False

    valid = geo.get("valid_postcode_prefixes", [])
    return not valid or any(pm.group().startswith(v) for v in valid)


def classify_company(google_cat: str, name: str, config: dict[str, Any]) -> str:
    """
    Assign a category label by keyword matching.

    Checks combined text of google_cat + business name against keyword
    lists from config['classification']['keywords']. Returns 'Other' if
    nothing matches or no keywords are configured.

    Args:
        google_cat: Category label from the Google Maps listing.
        name:       Business display name.
        config:     Full config dict.
    """
    keyword_map: dict[str, list[str]] = (
        config.get("classification", {}).get("keywords", {})
    )
    if not keyword_map:
        return "Other"

    combined = (google_cat + " " + name).lower()
    for label, keywords in keyword_map.items():
        if label == "Other":
            continue
        for kw in keywords:
            if kw.lower() in combined:
                return label
    return "Other"

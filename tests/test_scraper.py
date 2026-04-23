"""
tests/test_scraper.py
=====================
Unit tests for the Google Maps scraper.

All tests are pure-function tests — no browser, no internet, no API keys.
Completes in under 2 seconds.

Covers:
  Bug 1 — Index-based extraction / panel-link pollution
  Bug 2 — Dedup key missing separator (false-positive collisions)
  Bug 3 — rebuild_seen_ids case-sensitivity mismatch vs make_uid
  Bug 4 — Logged query only showed the bare keyword, not the full search

v1.1.0 additions:
  ProxyManagerThreshold — mark_failed respects threshold before removing proxy
  StopAtValidation      — stop_at must be HH:MM format; single-digit hour raises
  AppendRows            — append_rows creates header first call, omits on subsequent

Run with:
    pytest tests/ -v
"""

from __future__ import annotations

import os
import sys
import unittest

# ── Import path setup ─────────────────────────────────────────────────────────
# Allow running from both the project root and the tests/ directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── Imports from the scraper package ─────────────────────────────────────────
# These were previously imported from maps_scraper (monolithic).
# Updated to import from the correct modules in the scraper package.

from scraper.filters  import make_uid, rebuild_seen_ids
from scraper.utils    import clean_phone as normalize_phone
from scraper.extractor import extract_emails_from_html as extract_emails
from maps_scraper     import build_jobs_city, build_jobs_mega


# ── Helper: build a minimal config dict ──────────────────────────────────────

def _cfg(query="schools", location="London", zones=None):
    return {
        "search":    {"query": query, "location": location},
        "geography": {"region_zones": zones or []},
    }


# ============================================================================
# BUG 2 — make_uid pipe-separator prevents false-positive collisions
# ============================================================================

class TestMakeUid(unittest.TestCase):

    def test_returns_lowercase(self):
        """make_uid must always be lowercase so live results match stored keys."""
        uid = make_uid("PIZZA PALACE", "10 Main St, London")
        self.assertEqual(uid, uid.lower())

    def test_case_insensitive(self):
        """Same business in different cases must produce identical UIDs."""
        uid1 = make_uid("Pizza Palace", "10 Main St, London")
        uid2 = make_uid("PIZZA PALACE", "10 MAIN ST, LONDON")
        self.assertEqual(uid1, uid2)

    def test_separator_prevents_collision(self):
        """
        Bug 2: without a separator, make_uid("AB","Crest") == make_uid("A","BCrest").
        With the pipe separator those two must differ.
        """
        uid_ab_crest = make_uid("AB",  "Crest")
        uid_a_bcrest = make_uid("A",   "BCrest")
        self.assertNotEqual(uid_ab_crest, uid_a_bcrest,
            "Pipe separator must prevent false-positive collision between "
            "('AB','Crest') and ('A','BCrest')")

    def test_separator_more_cases(self):
        self.assertNotEqual(make_uid("ABC", "rest"),  make_uid("AB", "Crest"))
        self.assertNotEqual(make_uid("", "ABCrest"),  make_uid("ABC", "rest"))
        self.assertNotEqual(make_uid("X", ""),        make_uid("", "X"))

    def test_strips_whitespace(self):
        uid1 = make_uid("  Cafe Roma  ", "  Baker St  ")
        uid2 = make_uid("Cafe Roma",     "Baker St")
        self.assertEqual(uid1, uid2)

    def test_empty_name_produces_pipe_only_prefix(self):
        """Empty name should still produce a deterministic (though useless) key."""
        uid = make_uid("", "Some Address")
        self.assertIn("|", uid)

    def test_both_empty(self):
        uid = make_uid("", "")
        self.assertEqual(uid, "|")  # pipe only — will be filtered by rebuild_seen_ids


# ============================================================================
# BUG 3 — rebuild_seen_ids must match make_uid exactly
# ============================================================================

class TestRebuildSeenIds(unittest.TestCase):
    """
    The dedup set rebuilt from an existing CSV must contain exactly the same
    keys that the live scraper would produce via make_uid(). A mismatch
    caused every previously saved business to be re-scraped on resume.
    """

    def _rows(self, name: str, address: str) -> list[dict]:
        return [{"Company Name": name, "Address": address}]

    def test_basic_match(self):
        rows = self._rows("Pizza Palace", "10 Main St, London")
        seen = rebuild_seen_ids(rows)
        uid  = make_uid("Pizza Palace", "10 Main St, London")
        self.assertIn(uid, seen,
            "rebuild_seen_ids must produce the same key as make_uid()")

    def test_case_insensitive_match(self):
        """
        Bug 3: the old code used plain concatenation without .lower(), so a
        stored record ("Pizza Palace") never matched the live make_uid result
        ("pizza palace|..."), causing full re-scrape on every resume.
        """
        rows = self._rows("McDonald's", "Oxford St, London")
        seen = rebuild_seen_ids(rows)
        self.assertIn(make_uid("McDonald's",  "Oxford St, London"), seen)
        self.assertIn(make_uid("MCDONALD'S",  "OXFORD ST, LONDON"), seen)

    def test_empty_rows(self):
        self.assertEqual(rebuild_seen_ids([]), set())

    def test_skips_nameless_rows(self):
        """Rows with no Company Name must not create phantom UIDs."""
        rows = [{"Company Name": "", "Address": "10 Baker St"}]
        seen = rebuild_seen_ids(rows)
        self.assertNotIn(make_uid("", "10 Baker St"), seen,
            "Nameless rows should be excluded from the dedup set")

    def test_multiple_rows(self):
        rows = [
            {"Company Name": "Cafe Roma",    "Address": "1 High St"},
            {"Company Name": "Barber Kings", "Address": "2 Low St"},
        ]
        seen = rebuild_seen_ids(rows)
        self.assertIn(make_uid("Cafe Roma",    "1 High St"), seen)
        self.assertIn(make_uid("Barber Kings", "2 Low St"),  seen)
        self.assertEqual(len(seen), 2)

    def test_uses_pipe_separator(self):
        """
        The key must contain the pipe separator so that adjacent-field
        collisions (Bug 2) are also prevented in the rebuilt set.
        """
        rows = self._rows("AB", "Crest")
        seen = rebuild_seen_ids(rows)
        self.assertNotIn("abcrest", seen,
            "rebuilt keys must use pipe separator, not bare concatenation")
        self.assertIn("ab|crest", seen)


# ============================================================================
# BUG 4 — Job query construction includes the location
# ============================================================================

class TestJobBuilding(unittest.TestCase):
    """
    The full query string (keyword + location [+ zone]) must be present in
    each job dict so that the browser URL and the log line always agree.
    Bug 4: the log printed only cfg['search']['query'] (the bare keyword),
    while the browser navigated to 'keyword location'.
    """

    def test_city_job_includes_location(self):
        jobs = build_jobs_city(_cfg())
        self.assertEqual(len(jobs), 1)
        q = jobs[0]["query"]
        self.assertIn("schools", q, "Job query must contain the search keyword")
        self.assertIn("London",  q, "Job query must contain the location")
        self.assertEqual(q, "schools London")

    def test_city_job_zone_is_location(self):
        jobs = build_jobs_city(_cfg())
        self.assertEqual(jobs[0]["zone"], "London")

    def test_mega_jobs_include_zone_and_location(self):
        zones = ["EC1", "EC2", "WC1"]
        jobs  = build_jobs_mega(_cfg(zones=zones))
        self.assertEqual(len(jobs), 3)
        for job, zone in zip(jobs, zones):
            self.assertIn("schools", job["query"])
            self.assertIn(zone,      job["query"])
            self.assertIn("London",  job["query"])
            self.assertFalse(job["done"])

    def test_mega_falls_back_to_city_with_no_zones(self):
        jobs = build_jobs_mega(_cfg(zones=[]))
        self.assertEqual(len(jobs), 1)
        self.assertIn("London", jobs[0]["query"])

    def test_city_job_not_done_initially(self):
        jobs = build_jobs_city(_cfg())
        self.assertFalse(jobs[0]["done"])


# ============================================================================
# BUG 1 — scroll_and_collect: dedup roundtrip + virtualised scroll simulation
# ============================================================================

class TestScrollAndCollectContract(unittest.TestCase):
    """Full dedup roundtrip — simulates resume after a crash."""

    def test_make_uid_and_seen_ids_roundtrip(self):
        saved_rows = [
            {"Company Name": "Alpha Plumbing", "Address": "5 Park Lane, E1 6RF"},
            {"Company Name": "Beta Electrics",  "Address": "12 City Rd, EC1A 2BB"},
        ]
        seen = rebuild_seen_ids(saved_rows)
        self.assertIn(make_uid("Alpha Plumbing", "5 Park Lane, E1 6RF"), seen)
        self.assertIn(make_uid("Beta Electrics",  "12 City Rd, EC1A 2BB"), seen)

    def test_new_business_not_in_seen(self):
        saved_rows = [{"Company Name": "Alpha Plumbing", "Address": "5 Park Lane"}]
        seen       = rebuild_seen_ids(saved_rows)
        new_uid    = make_uid("Gamma Heating", "99 Commercial Rd")
        self.assertNotIn(new_uid, seen)

    def test_dedup_set_case_insensitive(self):
        saved_rows = [{"Company Name": "Sunrise Bakery", "Address": "3 Flour St"}]
        seen       = rebuild_seen_ids(saved_rows)
        self.assertIn(make_uid("SUNRISE BAKERY", "3 FLOUR ST"), seen)
        self.assertIn(make_uid("sunrise bakery", "3 flour st"), seen)
        self.assertIn(make_uid("Sunrise Bakery", "3 Flour St"), seen)


# ── Virtual page simulation ──────────────────────────────────────────────────

class _FakePage:
    """
    Minimal fake that models a virtualised results feed.
    Holds `total` cards but renders only a window of `window_size` at a time.
    Each scroll() call advances the window by `step` positions.
    """
    def __init__(self, total: int, window_size: int = 20, step: int = 5):
        self._total       = total
        self._window_size = window_size
        self._step        = step
        self._offset      = 0

    def scroll(self) -> None:
        self._offset = min(
            self._offset + self._step,
            max(self._total - self._window_size, 0),
        )

    def card_hrefs(self) -> list[str]:
        end = min(self._offset + self._window_size, self._total)
        return [
            f"https://www.google.com/maps/place/place_{i}/@51.5,0.1,17z"
            for i in range(self._offset, end)
        ]

    def at_end(self) -> bool:
        return self._offset + self._window_size >= self._total


def _incremental_collect(fake: _FakePage, stall_limit: int = 3) -> list[str]:
    """Simulation of the FIXED scroll_and_collect: harvest before each scroll."""
    collected: dict[str, None] = {}
    stall = 0

    def _harvest() -> int:
        added = 0
        for href in fake.card_hrefs():
            if href not in collected:
                collected[href] = None
                added += 1
        return added

    while True:
        new_this_step = _harvest()
        if new_this_step == 0:
            stall += 1
            if stall >= stall_limit:
                break
        else:
            stall = 0
        if fake.at_end():
            _harvest()
            break
        fake.scroll()

    _harvest()
    return list(collected.keys())


def _old_collect(fake: _FakePage, stall_limit: int = 3) -> list[str]:
    """Simulation of the OLD (broken) logic: count DOM nodes, snapshot at end."""
    last = 0
    stall = 0
    while True:
        fake.scroll()
        n = len(fake.card_hrefs())
        if n == last:
            stall += 1
            if stall >= stall_limit:
                break
        else:
            stall = 0
        last = n
    return fake.card_hrefs()


class TestVirtualisedScrolling(unittest.TestCase):
    """Proves the incremental strategy captures all cards; old logic caps out."""

    def test_old_logic_misses_virtualised_cards(self):
        fake = _FakePage(total=200, window_size=20, step=5)
        result = _old_collect(fake, stall_limit=3)
        self.assertLessEqual(len(result), 25)
        self.assertLess(len(result), 200)

    def test_new_logic_captures_all_cards(self):
        fake = _FakePage(total=200, window_size=20, step=5)
        result = _incremental_collect(fake, stall_limit=3)
        self.assertEqual(len(result), 200)

    def test_new_logic_correct_for_small_result_set(self):
        fake = _FakePage(total=16, window_size=20, step=5)
        result = _incremental_collect(fake, stall_limit=3)
        self.assertEqual(len(result), 16)

    def test_new_logic_no_duplicates(self):
        fake = _FakePage(total=50, window_size=20, step=3)
        result = _incremental_collect(fake, stall_limit=3)
        self.assertEqual(len(result), len(set(result)))

    def test_schools_london_scenario(self):
        """Reproduces the 26-result ceiling seen in scrape_20260418.log."""
        fake_old = _FakePage(total=200, window_size=20, step=5)
        self.assertLessEqual(len(_old_collect(fake_old, stall_limit=3)), 25)

        fake_new = _FakePage(total=200, window_size=20, step=5)
        self.assertEqual(len(_incremental_collect(fake_new, stall_limit=3)), 200)

    def test_preserves_insertion_order(self):
        fake = _FakePage(total=30, window_size=10, step=5)
        result = _incremental_collect(fake, stall_limit=3)
        idx_first = next(i for i, h in enumerate(result) if "place_0/" in h)
        idx_last  = next(i for i, h in enumerate(result) if "place_29/" in h)
        self.assertLess(idx_first, idx_last)


# ============================================================================
# Phone normalisation (regression guard)
# ============================================================================

class TestNormalizePhone(unittest.TestCase):

    def test_strips_uk_prefix(self):
        result = normalize_phone("+44 20 7946 0958", "44", ["02"], [11])
        self.assertEqual(result, "02079460958")

    def test_rejects_wrong_length(self):
        result = normalize_phone("123", "44", [], [10, 11])
        self.assertEqual(result, "")

    def test_rejects_wrong_prefix(self):
        result = normalize_phone("09001234567", "44", ["01", "02", "03", "07"], [11])
        self.assertEqual(result, "")

    def test_empty_input(self):
        self.assertEqual(normalize_phone("", "44", [], []), "")

    def test_no_validation_returns_digits(self):
        result = normalize_phone("(555) 123-4567", "", [], [])
        self.assertEqual(result, "5551234567")


# ============================================================================
# Email extraction (regression guard)
# ============================================================================

class TestExtractEmails(unittest.TestCase):

    def test_plain_email(self):
        html   = "<p>Contact us at info@example-business.co.uk</p>"
        emails = extract_emails(html, set(), set())
        self.assertIn("info@example-business.co.uk", emails)

    def test_mailto_href(self):
        html   = '<a href="mailto:hello@myshop.com">email us</a>'
        emails = extract_emails(html, set(), set())
        self.assertIn("hello@myshop.com", emails)

    def test_obfuscated_at(self):
        html   = "contact: admin [at] business [dot] org"
        emails = extract_emails(html, set(), set())
        self.assertIn("admin@business.org", emails)

    def test_junk_email_excluded(self):
        html   = "<p>user@domain.com</p>"
        emails = extract_emails(html, {"user@domain.com"}, set())
        self.assertNotIn("user@domain.com", emails)

    def test_junk_domain_excluded(self):
        html   = "<p>track@sentry.io</p>"
        emails = extract_emails(html, set(), {"sentry.io"})
        self.assertNotIn("track@sentry.io", emails)

    def test_image_extension_excluded(self):
        html   = "<img src='logo.png@2x.jpg'>"
        emails = extract_emails(html, set(), set())
        self.assertNotIn("logo.png@2x.jpg", emails)

    def test_deduplication(self):
        html   = "info@cafe.com info@cafe.com"
        emails = extract_emails(html, set(), set())
        self.assertEqual(emails.count("info@cafe.com"), 1)

    def test_empty_html(self):
        self.assertEqual(extract_emails("", set(), set()), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

# ============================================================================
# Config loading and validation
# ============================================================================

import tempfile
import textwrap

from scraper.config import _validate, _deep_merge, load_config


class TestValidate(unittest.TestCase):

    def _base_cfg(self, query="dentists", location="Manchester"):
        """Minimal valid config dict."""
        return {
            "search": {"query": query, "location": location},
            "output": {"format": "csv"},
        }

    def test_raises_for_empty_query(self):
        cfg = self._base_cfg(query="")
        with self.assertRaises(ValueError) as ctx:
            _validate(cfg)
        self.assertIn("search.query", str(ctx.exception))

    def test_raises_for_whitespace_only_query(self):
        cfg = self._base_cfg(query="   ")
        with self.assertRaises(ValueError):
            _validate(cfg)

    def test_raises_for_empty_location(self):
        cfg = self._base_cfg(location="")
        with self.assertRaises(ValueError) as ctx:
            _validate(cfg)
        self.assertIn("search.location", str(ctx.exception))

    def test_passes_for_valid_config(self):
        """Valid config must not raise."""
        cfg = self._base_cfg()
        try:
            _validate(cfg)
        except ValueError as exc:
            self.fail(f"_validate raised unexpectedly: {exc}")

    def test_raises_for_invalid_output_format(self):
        cfg = self._base_cfg()
        cfg["output"]["format"] = "json"
        with self.assertRaises(ValueError) as ctx:
            _validate(cfg)
        self.assertIn("output.format", str(ctx.exception))

    def test_normalises_excel_format(self):
        cfg = self._base_cfg()
        cfg["output"]["format"] = "EXCEL"
        _validate(cfg)
        self.assertEqual(cfg["output"]["format"], "excel")


class TestDeepMerge(unittest.TestCase):

    def test_override_replaces_leaf(self):
        base     = {"a": {"x": 1, "y": 2}}
        override = {"a": {"x": 99}}
        result   = _deep_merge(base, override)
        self.assertEqual(result["a"]["x"], 99)
        self.assertEqual(result["a"]["y"], 2,
            "Key not present in override must be preserved from base")

    def test_new_key_added(self):
        result = _deep_merge({"a": 1}, {"b": 2})
        self.assertIn("b", result)
        self.assertEqual(result["a"], 1)

    def test_does_not_mutate_base(self):
        base   = {"a": {"x": 1}}
        _deep_merge(base, {"a": {"x": 99}})
        self.assertEqual(base["a"]["x"], 1,
            "_deep_merge must not mutate the base dict")

    def test_nested_three_levels(self):
        base     = {"a": {"b": {"c": 1, "d": 2}}}
        override = {"a": {"b": {"c": 99}}}
        result   = _deep_merge(base, override)
        self.assertEqual(result["a"]["b"]["c"], 99)
        self.assertEqual(result["a"]["b"]["d"], 2)


class TestLoadConfig(unittest.TestCase):

    def test_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")

    def test_loads_valid_yaml(self):
        yaml_content = textwrap.dedent("""\
            search:
              query: "solicitors"
              location: "Birmingham"
        """)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(yaml_content)
            tmp_path = f.name

        try:
            cfg = load_config(tmp_path)
            self.assertEqual(cfg["search"]["query"], "solicitors")
            self.assertEqual(cfg["search"]["location"], "Birmingham")
        finally:
            import os; os.unlink(tmp_path)

    def test_defaults_are_applied(self):
        """A minimal config should still have all default keys filled in."""
        yaml_content = textwrap.dedent("""\
            search:
              query: "vets"
              location: "Leeds"
        """)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(yaml_content)
            tmp_path = f.name

        try:
            cfg = load_config(tmp_path)
            self.assertIn("performance", cfg)
            self.assertIn("headless", cfg["performance"])
            self.assertIn("output", cfg)
        finally:
            import os; os.unlink(tmp_path)


# ============================================================================
# Storage — CSV round-trip, checkpoint, output path
# ============================================================================

from scraper.storage import (
    _save_csv, _load_csv, save_checkpoint, load_checkpoint,
    clear_checkpoint, build_output_path, build_row, OUTPUT_FIELDS,
    log_done_query, load_done_queries, clear_done_queries,
)
from pathlib import Path


def _storage_cfg(tmp_dir: str) -> dict:
    """Minimal config pointing all file paths into a temp directory."""
    return {
        "search":  {"query": "dentists", "location": "Manchester"},
        "output":  {"directory": tmp_dir, "filename_prefix": "Test", "format": "csv"},
        "files":   {
            "checkpoint":   str(Path(tmp_dir) / "checkpoint.json"),
            "done_queries": str(Path(tmp_dir) / "done.txt"),
            "command_file": str(Path(tmp_dir) / "command.txt"),
            "log_dir":      tmp_dir,
        },
    }


class TestCsvRoundTrip(unittest.TestCase):

    def test_save_and_load_preserves_all_fields(self):
        rows = [
            {f: f"val_{f}" for f in OUTPUT_FIELDS},
            {f: ""         for f in OUTPUT_FIELDS},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.csv"
            _save_csv(rows, path)
            loaded = _load_csv(path)

        self.assertEqual(len(loaded), 2)
        for field in OUTPUT_FIELDS:
            self.assertEqual(loaded[0][field], f"val_{field}")

    def test_save_empty_rows_creates_no_file(self):
        from scraper.storage import save_output
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            save_output([], path, fmt="csv")
            self.assertFalse(path.exists(),
                "save_output with empty rows must not create a file")

    def test_unicode_content_survives_round_trip(self):
        rows = [{f: ("café résumé" if f == "Company Name" else "") for f in OUTPUT_FIELDS}]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "unicode.csv"
            _save_csv(rows, path)
            loaded = _load_csv(path)
        self.assertEqual(loaded[0]["Company Name"], "café résumé")


class TestCheckpoint(unittest.TestCase):

    def test_save_and_load_roundtrip(self):
        """Loaded checkpoint contains version field injected by save_checkpoint."""
        data = {"jobs": [{"query": "dentists London", "zone": "London", "done": False}]}
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            save_checkpoint(data, cfg)
            loaded = load_checkpoint(cfg)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["jobs"], data["jobs"])

    def test_load_returns_none_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            result = load_checkpoint(cfg)
        self.assertIsNone(result)

    def test_clear_removes_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            save_checkpoint({"jobs": []}, cfg)
            clear_checkpoint(cfg)
            self.assertIsNone(load_checkpoint(cfg))

    def test_atomic_write_uses_tmp_file(self):
        """Checkpoint must go through .tmp → os.replace, not direct write."""
        import os
        data = {"jobs": []}
        with tempfile.TemporaryDirectory() as tmp:
            cfg        = _storage_cfg(tmp)
            cp_path    = cfg["files"]["checkpoint"]
            tmp_path   = cp_path + ".tmp"
            save_checkpoint(data, cfg)
            self.assertTrue(os.path.exists(cp_path))
            self.assertFalse(os.path.exists(tmp_path),
                ".tmp file must be renamed away after a successful write")


class TestDoneQueries(unittest.TestCase):

    def test_append_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            log_done_query("dentists London", cfg)
            log_done_query("solicitors EC1", cfg)
            loaded = load_done_queries(cfg)
        self.assertIn("dentists London", loaded)
        self.assertIn("solicitors EC1", loaded)

    def test_returns_empty_set_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            self.assertEqual(load_done_queries(cfg), set())

    def test_clear_removes_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            log_done_query("query one", cfg)
            clear_done_queries(cfg)
            self.assertEqual(load_done_queries(cfg), set())


class TestBuildOutputPath(unittest.TestCase):

    def test_returns_csv_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg  = _storage_cfg(tmp)
            path = build_output_path(cfg)
        self.assertEqual(path.suffix, ".csv")

    def test_returns_xlsx_for_excel_format(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _storage_cfg(tmp)
            cfg["output"]["format"] = "excel"
            path = build_output_path(cfg)
        self.assertEqual(path.suffix, ".xlsx")

    def test_filename_contains_query_and_location(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg  = _storage_cfg(tmp)
            path = build_output_path(cfg)
        self.assertIn("dentists", path.name)
        self.assertIn("Manchester", path.name)


class TestBuildRow(unittest.TestCase):

    def _place(self):
        return {
            "name":       "Bright Dental",
            "address":    "12 High St, Manchester, M1 1AE",
            "website":    "https://brightdental.co.uk",
            "phone_raw":  "01612345678",
            "rating":     "4.8",
            "google_cat": "Dentist",
        }

    def _cfg(self):
        return {
            "phone":          {"country_code": "44", "valid_prefixes": [], "valid_lengths": []},
            "classification": {"keywords": {}},
            "geography":      {"postcode_pattern": r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*\d[A-Z]{2}\b"},
        }

    def test_returns_all_output_fields(self):
        row = build_row(self._place(), "hello@brightdental.co.uk", "01612345678", "Dentist", self._cfg())
        for field in OUTPUT_FIELDS:
            self.assertIn(field, row, f"build_row result missing field: {field}")

    def test_email_status_found(self):
        row = build_row(self._place(), "hello@brightdental.co.uk", "", "", self._cfg())
        self.assertEqual(row["Email Status"], "found")

    def test_email_status_notfound(self):
        row = build_row(self._place(), "", "", "", self._cfg())
        self.assertEqual(row["Email Status"], "notfound")

    def test_source_is_google_maps(self):
        row = build_row(self._place(), "", "", "", self._cfg())
        self.assertEqual(row["Source"], "Google Maps")


# ============================================================================
# Browser — _launch_with_retry (mocked, no real browser)
# ============================================================================

from unittest.mock import MagicMock, patch, call
from scraper.browser import _launch_with_retry, ProxyManager


class TestLaunchWithRetry(unittest.TestCase):

    def _make_pw(self, side_effects):
        """Build a mock playwright object whose launch raises the given sequence."""
        fake_context = MagicMock()
        fake_context.pages = [MagicMock()]

        pw = MagicMock()
        pw.chromium.launch_persistent_context.side_effect = side_effects
        return pw, fake_context

    def test_succeeds_on_first_attempt(self):
        fake_context = MagicMock()
        fake_page    = MagicMock()
        fake_context.pages = [fake_page]

        pw = MagicMock()
        pw.chromium.launch_persistent_context.return_value = fake_context

        context, page = _launch_with_retry(pw, {"user_data_dir": "/tmp/profile"})
        self.assertEqual(context, fake_context)
        self.assertEqual(page, fake_page)
        pw.chromium.launch_persistent_context.assert_called_once()

    def test_retries_on_transient_failure_then_succeeds(self):
        fake_context = MagicMock()
        fake_context.pages = [MagicMock()]

        pw = MagicMock()
        pw.chromium.launch_persistent_context.side_effect = [
            ConnectionError("CDP timeout"),   # attempt 1 fails
            fake_context,                      # attempt 2 succeeds
        ]

        with patch("scraper.browser.time.sleep"):
            context, _ = _launch_with_retry(pw, {"user_data_dir": "/tmp"}, retries=3)

        self.assertEqual(context, fake_context)
        self.assertEqual(pw.chromium.launch_persistent_context.call_count, 2)

    def test_raises_runtime_error_after_all_retries_exhausted(self):
        pw = MagicMock()
        pw.chromium.launch_persistent_context.side_effect = ConnectionError("always fails")

        with patch("scraper.browser.time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                _launch_with_retry(pw, {"user_data_dir": "/tmp"}, retries=3)

        self.assertIn("3 attempts", str(ctx.exception))

    def test_retry_sleep_is_progressive(self):
        pw = MagicMock()
        pw.chromium.launch_persistent_context.side_effect = ConnectionError("fail")

        sleep_calls = []
        with patch("scraper.browser.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            with self.assertRaises(RuntimeError):
                _launch_with_retry(pw, {"user_data_dir": "/tmp"}, retries=3)

        # Should have slept after attempt 1 (10s) and attempt 2 (20s), not after attempt 3
        self.assertEqual(len(sleep_calls), 2)
        self.assertEqual(sleep_calls[0], 10)
        self.assertEqual(sleep_calls[1], 20)

    def test_uses_new_page_when_no_existing_pages(self):
        fake_context  = MagicMock()
        fake_new_page = MagicMock()
        fake_context.pages = []
        fake_context.new_page.return_value = fake_new_page

        pw = MagicMock()
        pw.chromium.launch_persistent_context.return_value = fake_context

        _, page = _launch_with_retry(pw, {"user_data_dir": "/tmp"})
        self.assertEqual(page, fake_new_page)
        fake_context.new_page.assert_called_once()


# ── New tests for v1.1.0 upgrades ─────────────────────────────────────────────

class TestProxyManagerThreshold(unittest.TestCase):
    """Bug 1 fix: mark_failed only removes proxy after threshold calls."""

    def test_proxy_not_removed_before_threshold(self):
        mgr = ProxyManager(["http://1.1.1.1:8080"])
        mgr.mark_failed("http://1.1.1.1:8080", threshold=3)
        mgr.mark_failed("http://1.1.1.1:8080", threshold=3)
        # Two failures — should still be present
        self.assertTrue(mgr.has_proxies())

    def test_proxy_removed_at_threshold(self):
        mgr = ProxyManager(["http://1.1.1.1:8080"])
        mgr.mark_failed("http://1.1.1.1:8080", threshold=3)
        mgr.mark_failed("http://1.1.1.1:8080", threshold=3)
        mgr.mark_failed("http://1.1.1.1:8080", threshold=3)
        # Third failure hits threshold — should be gone
        self.assertFalse(mgr.has_proxies())

    def test_unknown_proxy_is_ignored(self):
        mgr = ProxyManager(["http://1.1.1.1:8080"])
        mgr.mark_failed("http://9.9.9.9:8080", threshold=1)
        self.assertTrue(mgr.has_proxies())

    def test_threshold_one_removes_immediately(self):
        mgr = ProxyManager(["http://1.1.1.1:8080"])
        mgr.mark_failed("http://1.1.1.1:8080", threshold=1)
        self.assertFalse(mgr.has_proxies())


class TestStopAtValidation(unittest.TestCase):
    """Bug 2 fix: stop_at must be HH:MM format."""

    def _cfg(self, stop_at_value):
        cfg = {
            "search":     {"query": "dentists", "location": "london"},
            "output":     {"format": "csv"},
            "scheduling": {"stop_at": stop_at_value},
        }
        return cfg

    def test_valid_stop_at_passes(self):
        from scraper.config import _validate
        cfg = self._cfg("09:00")
        _validate(cfg)   # should not raise

    def test_none_stop_at_passes(self):
        from scraper.config import _validate
        cfg = self._cfg(None)
        _validate(cfg)   # should not raise

    def test_single_digit_hour_raises(self):
        from scraper.config import _validate
        cfg = self._cfg("9:00")
        with self.assertRaises(ValueError) as ctx:
            _validate(cfg)
        self.assertIn("HH:MM", str(ctx.exception))

    def test_missing_colon_raises(self):
        from scraper.config import _validate
        cfg = self._cfg("0900")
        with self.assertRaises(ValueError):
            _validate(cfg)


class TestAppendRows(unittest.TestCase):
    """Bug 5 fix: append_rows creates header on first call, omits it on second."""

    def test_creates_file_with_header_on_first_call(self):
        import csv, tempfile, os
        from scraper.storage import append_rows, OUTPUT_FIELDS
        row = {f: f"val_{f}" for f in OUTPUT_FIELDS}
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = Path(f.name)
        path.unlink()   # start with no file
        try:
            append_rows([row], path)
            with open(path, encoding="utf-8-sig") as f:
                reader = list(csv.DictReader(f))
            self.assertEqual(len(reader), 1)
            self.assertEqual(reader[0]["Company Name"], "val_Company Name")
        finally:
            if path.exists():
                path.unlink()

    def test_appends_without_duplicate_header(self):
        import csv, tempfile
        from scraper.storage import append_rows, OUTPUT_FIELDS
        row1 = {f: "first"  for f in OUTPUT_FIELDS}
        row2 = {f: "second" for f in OUTPUT_FIELDS}
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = Path(f.name)
        path.unlink()
        try:
            append_rows([row1], path)
            append_rows([row2], path)
            with open(path, encoding="utf-8-sig") as f:
                lines = f.readlines()
            # 1 header + 2 data rows = 3 lines
            self.assertEqual(len(lines), 3)
        finally:
            if path.exists():
                path.unlink()

    def test_empty_list_does_nothing(self):
        import tempfile
        from scraper.storage import append_rows
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = Path(f.name)
        path.unlink()
        append_rows([], path)
        self.assertFalse(path.exists())


# ── v1.2.0 new tests ──────────────────────────────────────────────────────────

class TestCheckStopTimeCrossMidnight(unittest.TestCase):
    """
    utils.check_stop_time must handle cross-midnight windows correctly.

    All tests use the _now injection parameter to make them fully
    deterministic and independent of the real system clock.

    Scenario being tested: a mega run starts at 23:30 with stop_at="01:00".
    Without the cross-midnight fix, "23:30" >= "01:00" is immediately True
    and the scraper stops before it even begins.  With the fix, stop_dt is
    pushed to 01:00 the NEXT day, so the scraper runs correctly overnight.
    """

    def test_cross_midnight_does_not_fire_immediately(self):
        """
        run_start=23:30, now=23:31, stop_at="01:00" → should NOT fire yet.
        The stop time is the next calendar day; the run just started.
        """
        from datetime import datetime
        from scraper.utils import check_stop_time
        run_start = datetime(2026, 4, 22, 23, 30, 0)
        now       = datetime(2026, 4, 22, 23, 31, 0)
        self.assertFalse(check_stop_time("01:00", run_start=run_start, _now=now))

    def test_cross_midnight_fires_after_stop_time_reached(self):
        """
        run_start=23:30, now=01:05 next day, stop_at="01:00" → should fire.
        """
        from datetime import datetime
        from scraper.utils import check_stop_time
        run_start = datetime(2026, 4, 22, 23, 30, 0)
        now       = datetime(2026, 4, 23,  1,  5, 0)   # next calendar day
        self.assertTrue(check_stop_time("01:00", run_start=run_start, _now=now))

    def test_same_day_fires_when_stop_time_reached(self):
        """
        run_start=09:00, now=10:01, stop_at="10:00" → should fire (same day).
        """
        from datetime import datetime
        from scraper.utils import check_stop_time
        run_start = datetime(2026, 4, 22,  9,  0, 0)
        now       = datetime(2026, 4, 22, 10,  1, 0)
        self.assertTrue(check_stop_time("10:00", run_start=run_start, _now=now))

    def test_same_day_does_not_fire_before_stop_time(self):
        """
        run_start=09:00, now=09:30, stop_at="10:00" → should NOT fire yet.
        """
        from datetime import datetime
        from scraper.utils import check_stop_time
        run_start = datetime(2026, 4, 22,  9,  0, 0)
        now       = datetime(2026, 4, 22,  9, 30, 0)
        self.assertFalse(check_stop_time("10:00", run_start=run_start, _now=now))

    def test_none_stop_at_never_fires(self):
        from scraper.utils import check_stop_time
        self.assertFalse(check_stop_time(None))

    def test_future_stop_at_does_not_fire(self):
        """stop_at 2 hours in the future (same day) → False."""
        from datetime import datetime
        from scraper.utils import check_stop_time
        run_start = datetime(2026, 4, 22,  8,  0, 0)
        now       = datetime(2026, 4, 22,  9,  0, 0)
        self.assertFalse(check_stop_time("11:00", run_start=run_start, _now=now))


class TestSanitizeCell(unittest.TestCase):
    """filters.sanitize_cell must neutralise CSV injection prefixes."""

    def setUp(self):
        from scraper.filters import sanitize_cell
        self.s = sanitize_cell

    def test_formula_prefix_neutralised(self):
        self.assertEqual(self.s("=SUM(A1:A10)"), "'=SUM(A1:A10)")

    def test_plus_prefix_neutralised(self):
        self.assertEqual(self.s("+44 207 000 0000"), "'+44 207 000 0000")

    def test_minus_prefix_neutralised(self):
        self.assertEqual(self.s("-1234"), "'-1234")

    def test_at_prefix_neutralised(self):
        self.assertEqual(self.s("@SUM"), "'@SUM")

    def test_normal_name_unchanged(self):
        self.assertEqual(self.s("Acme Plumbing Ltd"), "Acme Plumbing Ltd")

    def test_empty_string_unchanged(self):
        self.assertEqual(self.s(""), "")

    def test_number_unchanged(self):
        self.assertEqual(self.s("123 High Street"), "123 High Street")


class TestCheckSelectorHealth(unittest.TestCase):
    """_check_selector_health should warn when >80% of places lack an address."""

    def setUp(self):
        from maps_scraper import _check_selector_health
        self.check = _check_selector_health

    def test_no_warning_on_healthy_data(self):
        """All places have addresses — no warning should be logged."""
        places = [{"name": f"Biz {i}", "address": "123 St"} for i in range(20)]
        with self.assertLogs("maps_scraper", level="WARNING") as cm:
            # Inject a dummy unrelated warning so assertLogs doesn't fail
            # if no warning fires (assertLogs requires at least 1 message)
            import logging
            logging.getLogger("maps_scraper").warning("_dummy_")
            self.check(places)
        self.assertFalse(any("SELECTOR ALERT" in m for m in cm.output))

    def test_warning_on_missing_addresses(self):
        """When >80% of places have no address, SELECTOR ALERT should fire."""
        # 10 places, 9 with no address = 90% → should warn
        places = [{"name": f"Biz {i}", "address": ""} for i in range(9)]
        places.append({"name": "Biz 9", "address": "123 St"})
        with self.assertLogs("maps_scraper", level="WARNING") as cm:
            self.check(places)
        self.assertTrue(any("SELECTOR ALERT" in m for m in cm.output))

    def test_no_check_on_small_sample(self):
        """Fewer than 5 places should not trigger any check."""
        places = [{"name": f"Biz {i}"} for i in range(4)]
        with self.assertLogs("maps_scraper", level="WARNING") as cm:
            import logging
            logging.getLogger("maps_scraper").warning("_dummy_")
            self.check(places)
        self.assertFalse(any("SELECTOR ALERT" in m for m in cm.output))


class TestCheckpointVersioning(unittest.TestCase):
    """Checkpoint version mismatch should return None and log a warning."""

    def test_wrong_version_returns_none(self):
        import json, tempfile, os
        from scraper.storage import load_checkpoint, CHECKPOINT_VERSION
        with tempfile.TemporaryDirectory() as tmp:
            cp_path = os.path.join(tmp, "cp.json")
            # Write a checkpoint with an old/different version
            old_version = CHECKPOINT_VERSION - 1
            with open(cp_path, "w") as f:
                json.dump({"version": old_version, "jobs": []}, f)
            cfg = {"files": {"checkpoint": cp_path}}
            with self.assertLogs("maps_scraper", level="WARNING") as cm:
                result = load_checkpoint(cfg)
        self.assertIsNone(result)
        self.assertTrue(any("version mismatch" in m.lower() for m in cm.output))

    def test_correct_version_loads(self):
        import tempfile
        from scraper.storage import save_checkpoint, load_checkpoint
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {"files": {"checkpoint": os.path.join(tmp, "cp.json")}}
            save_checkpoint({"jobs": [], "output_path": "/tmp/out.csv"}, cfg)
            result = load_checkpoint(cfg)
        self.assertIsNotNone(result)
        self.assertEqual(result["jobs"], [])


class TestPhoneRegexNoBraces(unittest.TestCase):
    """extract_phones_from_html must not match phone-like strings containing braces."""

    def test_brace_string_not_matched(self):
        from scraper.extractor import extract_phones_from_html
        # A JavaScript template literal that looks like a phone number
        html = "<div>call us at 0207 {var} 4567 for info</div>"
        phones = extract_phones_from_html(html, "44", ["02", "07"], [10, 11])
        # The brace-containing string should either not match or clean to empty
        for p in phones:
            self.assertNotIn("{", p)
            self.assertNotIn("}", p)

    def test_real_phone_still_matched(self):
        from scraper.extractor import extract_phones_from_html
        html = "<p>Phone: 02071234567</p>"
        phones = extract_phones_from_html(html, "44", ["02"], [11])
        self.assertIn("02071234567", phones)


import os


class TestExtractPhonesFromHtml(unittest.TestCase):
    """
    Critical regression tests for extract_phones_from_html.

    Version A had a production-breaking regex bug (unclosed character class
    from {{8,13}}) that raised re.error and zeroed 100% of phone output.
    These tests ensure the function works for all common UK phone patterns
    and that no future regex change can break silently.
    """

    def setUp(self):
        from scraper.extractor import extract_phones_from_html
        self.extract = extract_phones_from_html

    def test_uk_mobile_extracted(self):
        phones = self.extract("<p>Call 07911123456</p>", "44", ["07"], [11])
        self.assertTrue(any("07911123456" in p for p in phones), phones)

    def test_uk_landline_extracted(self):
        phones = self.extract("<p>020 7946 0000</p>", "44", ["020"], [11])
        self.assertTrue(len(phones) > 0, "Landline not extracted")
        self.assertEqual(phones[0], "02079460000")

    def test_plus44_stripped_correctly(self):
        phones = self.extract("+447700900123", "44", ["07"], [11])
        self.assertTrue(len(phones) > 0, "+44 prefix not stripped")
        self.assertEqual(phones[0], "07700900123")

    def test_0044_stripped_correctly(self):
        phones = self.extract("00447700900123", "44", ["07"], [11])
        self.assertTrue(len(phones) > 0, "0044 prefix not stripped")

    def test_invalid_length_rejected(self):
        phones = self.extract("<p>12345</p>", "44", ["01"], [10, 11])
        self.assertEqual(phones, [], "Short number should be rejected")

    def test_invalid_prefix_rejected(self):
        phones = self.extract("<p>09000000000</p>", "44", ["01", "02", "07"], [11])
        self.assertEqual(phones, [], "Premium-rate prefix should be rejected")

    def test_empty_html_returns_empty(self):
        self.assertEqual(self.extract("", "44", ["07"], [11]), [])

    def test_no_country_code_path(self):
        phones = self.extract("<p>07911123456</p>", "", [], [])
        self.assertTrue(len(phones) > 0, "No-country-code path broken")

    def test_brace_in_pattern_does_not_crash(self):
        # Regression: Version A had an unclosed character class that raised re.error
        try:
            self.extract("<p>0207{var}4567</p>", "44", ["02"], [11])
        except Exception as exc:
            self.fail(f"extract_phones_from_html raised: {exc}")

    def test_deduplication(self):
        phones = self.extract(
            "<p>07700900123</p><p>07700900123</p>", "44", ["07"], [11]
        )
        self.assertEqual(len(phones), 1, "Duplicate phone not deduplicated")


class TestCleanBusinessName(unittest.TestCase):
    """Business name cleaning strips Google SEO pipe suffixes."""

    def setUp(self):
        from scraper.extractor import _clean_business_name
        self.clean = _clean_business_name

    def test_strips_pipe_suffix(self):
        self.assertEqual(
            self.clean("Acme Lettings | Manchester Estate Agents | Letting Agents"),
            "Acme Lettings",
        )

    def test_no_pipe_unchanged(self):
        self.assertEqual(self.clean("Acme Lettings"), "Acme Lettings")

    def test_empty_string_safe(self):
        self.assertEqual(self.clean(""), "")

    def test_strips_whitespace_around_name(self):
        self.assertEqual(self.clean("  Acme Ltd   | suffix  "), "Acme Ltd")


class TestMaxStallsConfig(unittest.TestCase):
    """max_stalls must be present in DEFAULTS and overridable from config."""

    def test_default_present_and_correct(self):
        from scraper.config import DEFAULTS
        self.assertIn("max_stalls", DEFAULTS["performance"])
        self.assertEqual(DEFAULTS["performance"]["max_stalls"], 5)

    def test_max_stalls_overridable(self):
        from scraper.config import DEFAULTS, _deep_merge
        merged = _deep_merge(DEFAULTS, {"performance": {"max_stalls": 8}})
        self.assertEqual(merged["performance"]["max_stalls"], 8)

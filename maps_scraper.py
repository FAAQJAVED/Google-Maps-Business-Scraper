#!/usr/bin/env python3
"""
Google Maps Business Scraper
==============================
Scrapes Google Maps for business listings and enriches each result with
email addresses and phone numbers harvested from the business's own website.

Modes
-----
city  — Single-query search. Fast, ~20-100 results. Good for testing.
mega  — One query per zone (postcode district / zip code). Gets 10-50x more
        results by bypassing Google Maps' per-search cap.

Usage
-----
    python maps_scraper.py --mode city
    python maps_scraper.py --mode mega
    python maps_scraper.py --mode city --config my_config.yaml
    python maps_scraper.py --mode mega --fresh      # clear checkpoint, restart
    python maps_scraper.py --mode mega --dry-run    # preview queries, no browser
    python maps_scraper.py --stats                  # print output file stats

Runtime Controls
----------------
    Keyboard : P = pause   R = resume   Q = quit   S = status
    File     : echo pause  > command.txt
               echo resume > command.txt
               echo stop   > command.txt
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys
import time
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Scope warning suppression to just the class we know about
urllib3.disable_warnings(InsecureRequestWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("asyncio").setLevel(logging.ERROR)

from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError

from scraper.config   import load_config
from scraper.controls import ControlState, ControlHandler
from scraper.browser  import launch_browser, ProxyManager, is_captcha_page, handle_captcha
from scraper.extractor import scroll_and_collect, extract_place, enrich_batch
from scraper.filters  import is_in_region, classify_company, make_uid, rebuild_seen_ids
from scraper.storage  import (
    build_output_path, load_existing_output, save_output, append_rows, build_row,
    save_checkpoint, load_checkpoint, clear_checkpoint,
    log_done_query, load_done_queries, clear_done_queries,
    CHECKPOINT_VERSION,
)
from scraper.utils import beep, elapsed, check_disk, check_stop_time, clean_phone, backoff_sleep


# ── Logging ───────────────────────────────────────────────────────────────────
# setup_logging() is called from main() — NOT at module level.
# Calling it at module level creates the logs/ directory and opens a rotating
# log file on every import, which breaks tests and dry-run invocations.

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """Dual-handler logger: INFO to console, DEBUG to rotating file."""
    Path(log_dir).mkdir(exist_ok=True)
    logger = logging.getLogger("maps_scraper")
    if logger.handlers:
        return logger   # already configured (e.g. called twice)

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S"
    )

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.handlers.RotatingFileHandler(
        Path(log_dir) / f"scrape_{date.today():%Y%m%d}.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


log = logging.getLogger("maps_scraper")


# ── Job building ──────────────────────────────────────────────────────────────

def build_jobs_city(cfg: dict[str, Any]) -> list[dict]:
    """Build a single-job list for city mode."""
    q = f"{cfg['search']['query']} {cfg['search']['location']}"
    return [{"query": q, "zone": cfg["search"]["location"], "done": False}]


def build_jobs_mega(cfg: dict[str, Any]) -> list[dict]:
    """Build one job per region zone for mega mode."""
    zones: list[str] = cfg["geography"].get("region_zones", [])
    if not zones:
        log.warning("No region_zones configured — running in city mode")
        return build_jobs_city(cfg)
    q   = cfg["search"]["query"]
    loc = cfg["search"]["location"]
    return [
        {"query": f"{q} {z} {loc}", "zone": z, "done": False}
        for z in zones
    ]


# ── Batch processing ──────────────────────────────────────────────────────────

def process_places(
    raw_places: list[dict],
    cfg: dict[str, Any],
    csv_data: list[dict],
    out_path: Path,
    proxy_mgr: ProxyManager,
) -> int:
    """
    Enrich, classify, and save a batch of scraped places.

    Args:
        raw_places: List of raw place dicts from extract_place().
        cfg:        Full config dict.
        csv_data:   Accumulated rows list (mutated in-place).
        out_path:   Output file path.
        proxy_mgr:  Active proxy manager for HTTP enrichment.

    Returns:
        Number of new records added.
    """
    if not raw_places:
        return 0

    log.info("  📧  Fetching contacts for %d leads...", len(raw_places))
    enriched = enrich_batch(raw_places, cfg, proxy_mgr)

    phone_cfg = cfg.get("phone", {})
    fmt       = cfg.get("output", {}).get("format", "csv")

    for i, place in enumerate(raw_places):
        email, web_phone = enriched.get(i, ("", ""))
        maps_phone = clean_phone(
            place.get("phone_raw", ""),
            phone_cfg.get("country_code", ""),
            phone_cfg.get("valid_prefixes", []),
            phone_cfg.get("valid_lengths", []),
        )
        final_phone = web_phone if web_phone else maps_phone
        category    = classify_company(place.get("google_cat", ""), place["name"], cfg)
        csv_data.append(build_row(place, email, final_phone, category, cfg))

    new_rows = csv_data[-len(raw_places):]
    if fmt == "csv":
        append_rows(new_rows, out_path)
    else:
        save_output(csv_data, out_path, fmt)

    new_n     = len(raw_places)
    got_email = sum(1 for r in new_rows if r.get("Email"))
    got_phone = sum(1 for r in new_rows if r.get("Phone"))
    log.info(
        "  ✅  +%d  |  email: %d (%d%%)  |  phone: %d (%d%%)",
        new_n,
        got_email, int(got_email / max(new_n, 1) * 100),
        got_phone, int(got_phone / max(new_n, 1) * 100),
    )
    return new_n


# ── Progress formatting ───────────────────────────────────────────────────────

def _format_progress(
    done: int, total: int, saved: int, new_n: int,
    query_times: list[float], start: float, zone: str,
) -> str:
    """Format the one-line progress log shown after each completed zone."""
    recent  = query_times[-20:] if query_times else [1]
    avg_q   = sum(recent) / len(recent)
    rem     = total - done
    eta_m   = int(rem * avg_q / 60)
    eta_str = f"~{eta_m//60}h{eta_m%60:02d}m" if eta_m >= 60 else f"~{eta_m}m"
    rate    = round(saved / max((time.time() - start) / 60, 0.01), 1)
    pct     = int(done / total * 40) if total else 0
    bar     = "█" * pct + "░" * (40 - pct)
    return (
        f"{elapsed(start)} {bar}  {done}/{total} | +{new_n} | "
        f"total:{saved} | {rate:.1f}/min | ETA:{eta_str} | zone:{zone}"
    )


# ── Selector health check ─────────────────────────────────────────────────────

def _check_selector_health(places: list[dict]) -> None:
    """
    After extraction, warn if an unusually high fraction of collected places
    are missing their address field. This is the primary signal that Google
    changed its DOM (the name check is omitted because items without a name
    are filtered BEFORE being added to the list, making a name-missing alert
    impossible to trigger here).

    Called ONCE per zone, after the full extraction loop completes — not
    inside the per-href inner loop (which would re-run the same check on
    every new card and flood the log with repeated warnings).

    Args:
        places: Full list of places collected for the current zone.
    """
    if len(places) < 5:
        return
    no_address = sum(1 for p in places if not p.get("address"))
    pct = no_address / len(places)
    if pct >= 0.80:
        log.warning(
            "🚨 SELECTOR ALERT: %d/%d places (%d%%) have no address — "
            "Google may have changed its DOM. "
            "Check data-item-id='address' selector in extractor.py",
            no_address, len(places), int(pct * 100),
        )


# ── Main orchestration ────────────────────────────────────────────────────────

class ScraperSession:
    """
    Encapsulates a single full scrape run.

    Wrapping the run in a class eliminates 15+ local variables in a 230-line
    function, makes login_mode and run_start available to all internal methods
    without threading through every call site, and provides clean lifecycle
    management.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        mode: str,
        login_mode: bool = False,
    ) -> None:
        self.cfg        = cfg
        self.mode       = mode
        self.login_mode = login_mode
        self.perf       = cfg["performance"]

        # Runtime state
        self.start      = time.time()
        self.run_start  = datetime.now()   # for cross-midnight stop_at check
        self.state      = ControlState()
        self.proxy_mgr  = ProxyManager(cfg.get("stealth", {}).get("proxies", []))

        # Output state (resolved after checkpoint load)
        self.out_path:  Path       = build_output_path(cfg)
        self.csv_data:  list[dict] = []
        self.seen_ids:  set[str]   = set()
        self.jobs:      list[dict] = []
        self.done_count: int       = 0

        # Shared context dict for ControlHandler status display
        self.ctx: dict[str, Any] = {
            "start":       self.start,
            "total_jobs":  0,
            "done_jobs":   0,
            "total_saved": 0,
            "query_times": [],
        }
        self.ctrl = ControlHandler(self.state, self.ctx, cfg)

        # Rate-limit hit counter (Improvement F)
        self._rate_hits: int = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Execute the full scrape session."""
        self.ctrl.start()
        self._resolve_jobs()
        self._print_header()

        with sync_playwright() as pw:
            log.info("🚀  Launching browser...")
            browser, page = launch_browser(
                pw, self.cfg, self.proxy_mgr, login_mode=self.login_mode
            )
            log.info("✅  Browser ready\n")

            query_times:     list[float] = []
            q_since_restart: int         = 0

            for job in self.jobs:
                if job["done"]:
                    continue
                if self.state.stop:
                    break
                self.ctrl.wait_while_paused()
                if self.state.stop:
                    break

                if check_stop_time(
                    self.cfg.get("scheduling", {}).get("stop_at"),
                    run_start=self.run_start,
                ):
                    log.info("⏸  Scheduled stop time reached")
                    beep("stop")
                    break

                if not check_disk(self.cfg.get("scheduling", {}).get("disk_min_mb", 500)):
                    log.warning("⚠️  Low disk space — pausing")
                    beep("alert")
                    self.state.paused = True
                    self.ctrl.wait_while_paused()
                    if self.state.stop:
                        break

                if q_since_restart >= self.perf.get("browser_restart_every", 300):
                    log.info("🔄  Restarting browser (memory management)...")
                    try:
                        browser.close()
                    except Exception:
                        pass
                    time.sleep(2)
                    browser, page = launch_browser(
                        pw, self.cfg, self.proxy_mgr, login_mode=self.login_mode
                    )
                    q_since_restart = 0

                t0    = time.time()
                query = job["query"]
                log.info("🔍  Searching: %s", query)

                try:
                    # Use quote_plus to correctly encode special chars, non-ASCII,
                    # ampersands, and slashes that would otherwise break the URL.
                    page.goto(
                        f"https://www.google.com/maps/search/{quote_plus(query)}",
                        wait_until="domcontentloaded",
                        timeout=25_000,
                    )
                    time.sleep(1.0)

                    if is_captcha_page(page) and self.cfg.get("captcha", {}).get("human_solve", True):
                        browser, page = handle_captcha(page, pw, self.cfg, self.proxy_mgr)

                    card_hrefs = scroll_and_collect(
                        page, self.state,
                        scroll_pause=self.perf.get("scroll_pause", 1.5),
                        slow_wait=self.perf.get("slow_connection_wait", 25.0),
                        max_stalls=self.perf.get("max_stalls", 5),
                    )

                except TargetClosedError:
                    log.warning("Browser closed unexpectedly — restarting")
                    try:
                        browser.close()
                    except Exception:
                        pass
                    time.sleep(2)
                    browser, page = launch_browser(
                        pw, self.cfg, self.proxy_mgr, login_mode=self.login_mode
                    )
                    q_since_restart = 0
                    continue

                except Exception as exc:
                    beep("error")
                    log.warning("❌  Query failed [%s]: %s", type(exc).__name__, query[:60])
                    continue

                # Zero results — possible layout change or rate limit
                if not card_hrefs:
                    self._rate_hits += 1
                    log.warning(
                        "  ⚠️  Zero results: %s — rate limit signal #%d",
                        query[:80], self._rate_hits,
                    )
                    if self._rate_hits >= 2:
                        backoff_sleep(self._rate_hits)
                        if self.proxy_mgr.has_proxies():
                            self.proxy_mgr.rotate()
                    job["done"] = True
                    self.done_count += 1
                    self.ctx["done_jobs"] = self.done_count
                    log_done_query(query, self.cfg)
                    self._save_checkpoint()
                    continue

                total_hrefs = len(card_hrefs)
                log.info(
                    "  📋  %d cards — extraction will take ~%d min, please wait...",
                    total_hrefs,
                    max(1, round(total_hrefs * 4 / 60)),
                )

                raw_places: list[dict] = []
                consecutive_failures   = 0
                MAX_CONSECUTIVE_FAIL   = 8
                extracted_count        = 0
                LOG_EVERY              = 10   # log every N places during extraction

                for href in card_hrefs:
                    if self.state.stop:
                        break
                    self.ctrl.check()
                    self.ctrl.wait_while_paused()
                    if self.state.stop:
                        break

                    is_place_url = "/maps/place/" in href
                    place = extract_place(page, href, self.state)
                    extracted_count += 1

                    # Live extraction progress — shown every LOG_EVERY places
                    # so the terminal is never silent for more than ~40s.
                    if extracted_count % LOG_EVERY == 0 or extracted_count == 1:
                        name_hint = (
                            place.get("name", "")[:40] if place and place.get("name") else "…"
                        )
                        log.info(
                            "  ⏳  [%d/%d]  %.0f%%  — last: %s",
                            extracted_count, total_hrefs,
                            extracted_count / total_hrefs * 100,
                            name_hint,
                        )

                    if not place or not place.get("name"):
                        if is_place_url:
                            consecutive_failures += 1
                        if consecutive_failures >= MAX_CONSECUTIVE_FAIL:
                            log.warning(
                                "⚠️  %d consecutive failures — restarting browser...",
                                consecutive_failures,
                            )
                            try:
                                browser.close()
                            except Exception:
                                pass
                            time.sleep(2)
                            try:
                                browser, page = launch_browser(
                                    pw, self.cfg, self.proxy_mgr,
                                    login_mode=self.login_mode,
                                )
                                consecutive_failures = 0
                                q_since_restart      = 0
                                log.info("✅  Browser restarted — resuming extraction")
                            except Exception as restart_exc:
                                log.error("❌  Browser restart failed: %s", restart_exc)
                                self.state.stop = True
                                break
                        continue

                    consecutive_failures = 0

                    if place.get("name") and not place.get("address"):
                        log.debug("  warn (no address): %s", place["name"])

                    uid = make_uid(place["name"], place.get("address", ""))
                    if uid in self.seen_ids:
                        log.debug("  skip (seen): %s", place["name"])
                        continue
                    if not is_in_region(place, self.cfg):
                        log.debug("  skip (geo):  %s", place["name"])
                        continue
                    self.seen_ids.add(uid)
                    raw_places.append(place)

                # ── Post-extraction ───────────────────────────────────────────
                log.info(
                    "  ✔  Extraction complete: %d places visited, %d new leads",
                    extracted_count, len(raw_places),
                )
                self._rate_hits = 0   # Improvement F: successful result resets backoff counter

                # Selector health check — once per zone, after the full loop.
                # Checks address presence across ALL collected places (not just
                # the first 5 as before, and not repeatedly inside the loop).
                _check_selector_health(raw_places)

                if self.state.stop:
                    break

                new_n = process_places(
                    raw_places, self.cfg, self.csv_data, self.out_path, self.proxy_mgr
                )
                job["done"]   = True
                self.done_count  += 1
                q_since_restart  += 1
                # Improvement E — proactive proxy rotation every N completed queries
                rotate_every = self.cfg.get("stealth", {}).get("rotate_every", 0)
                if (rotate_every > 0
                        and self.proxy_mgr.has_proxies()
                        and self.done_count % rotate_every == 0):
                    self.proxy_mgr.rotate()
                self.ctx["done_jobs"]   = self.done_count
                self.ctx["total_saved"] = len(self.csv_data)
                log_done_query(query, self.cfg)
                self._save_checkpoint()

                elapsed_q = time.time() - t0
                query_times.append(elapsed_q)
                if len(query_times) > 50:
                    query_times.pop(0)
                self.ctx["query_times"] = query_times

                log.info(
                    _format_progress(
                        self.done_count, len(self.jobs), len(self.csv_data), new_n,
                        query_times, self.start, job["zone"],
                    )
                )
                self.ctrl.sleep(self.perf.get("request_delay", 0.3))

            try:
                browser.close()
            except Exception:
                pass

        self.ctrl.stop_listening()
        self._print_summary()

    # ── Private helpers ────────────────────────────────────────────────────────

    def _resolve_jobs(self) -> None:
        """
        Load checkpoint (if any) and determine the job list and output path.

        Defers the initial file load until after checkpoint resolution so that
        a resumed run reads the checkpoint's output_path rather than a fresh
        date-stamped path — avoiding a wasted file parse followed by a correct
        one when resuming from a previous day.
        """
        cp = load_checkpoint(self.cfg)

        if cp and cp.get("jobs"):
            self.jobs = cp["jobs"]
            if cp.get("output_path"):
                self.out_path = Path(cp["output_path"])
            self.csv_data  = load_existing_output(self.out_path)
            self.seen_ids  = rebuild_seen_ids(self.csv_data)
            self.done_count = sum(1 for j in self.jobs if j["done"])
            log.info("▶  RESUMING  %d/%d jobs done", self.done_count, len(self.jobs))
            beep("resume")
        else:
            self.jobs = (
                build_jobs_mega(self.cfg)
                if self.mode == "mega"
                else build_jobs_city(self.cfg)
            )
            done_qs = load_done_queries(self.cfg)
            for j in self.jobs:
                if j["query"] in done_qs:
                    j["done"] = True
            self.done_count = sum(1 for j in self.jobs if j["done"])
            # Fresh run: load output file only if it already exists (e.g. from
            # a prior run on the same day before --fresh was used).
            self.csv_data = load_existing_output(self.out_path)
            self.seen_ids = rebuild_seen_ids(self.csv_data)
            beep("start")

        self.ctx["total_jobs"]  = len(self.jobs)
        self.ctx["done_jobs"]   = self.done_count
        self.ctx["total_saved"] = len(self.csv_data)

    def _save_checkpoint(self) -> None:
        """Persist jobs list and output path to the checkpoint file."""
        save_checkpoint(
            {"jobs": self.jobs, "output_path": str(self.out_path)},
            self.cfg,
        )

    def _print_header(self) -> None:
        first_pending = next(
            (j["query"] for j in self.jobs if not j["done"]),
            self.jobs[0]["query"] if self.jobs else "(none)",
        )
        log.info("=" * 65)
        log.info("  MODE     : %s", self.mode.upper())
        log.info("  QUERY    : %s  (example job query)", first_pending)
        log.info("  LOCATION : %s", self.cfg["search"]["location"])
        log.info("  JOBS     : %d total  |  %d remaining",
                 len(self.jobs), len(self.jobs) - self.done_count)
        log.info("  OUTPUT   : %s", self.out_path)
        log.info("=" * 65)
        log.info("  Controls : P=pause  R=resume  Q=quit  S=status")
        log.info("  Remote   : echo pause > command.txt")
        log.info("=" * 65)
        if not self.cfg.get("classification", {}).get("keywords"):
            log.info(
                "  ℹ️  No classification keywords configured — "
                "all records will be categorized as 'Other'. "
                "Set classification.keywords in config.yaml to enable."
            )

    def _print_summary(self) -> None:
        all_done = all(j["done"] for j in self.jobs)
        if all_done:
            clear_checkpoint(self.cfg)
            clear_done_queries(self.cfg)
            beep("done")

        n  = len(self.csv_data)
        we = sum(1 for r in self.csv_data if r.get("Email"))
        wp = sum(1 for r in self.csv_data if r.get("Phone"))
        ww = sum(1 for r in self.csv_data if r.get("Website"))
        wc = sum(1 for r in self.csv_data if r.get("Category") != "Other")

        log.info("=" * 65)
        log.info("  %-16s : %s",    "Status",        "✅ COMPLETE" if all_done else "⏸  PARTIAL")
        log.info("  %-16s : %d",    "Total records",  n)
        log.info("  %-16s : %d (%d%%)", "With email",  we, int(we / max(n, 1) * 100))
        log.info("  %-16s : %d (%d%%)", "With phone",  wp, int(wp / max(n, 1) * 100))
        log.info("  %-16s : %d (%d%%)", "With website", ww, int(ww / max(n, 1) * 100))
        log.info("  %-16s : %d",    "Categorized",    wc)
        log.info("  %-16s : %s",    "Time elapsed",   elapsed(self.start))
        log.info("  %-16s : %s",    "Output file",    self.out_path)
        log.info("=" * 65)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="maps_scraper",
        description="Google Maps Business Scraper — extracts and enriches business contact data",
    )
    p.add_argument(
        "--mode", choices=["city", "mega"], default="city",
        help="city = single query (fast test) | mega = all zones (max yield)",
    )
    p.add_argument(
        "--config", default="config.yaml", metavar="PATH",
        help="Path to config.yaml  (default: ./config.yaml)",
    )
    p.add_argument(
        "--fresh", action="store_true",
        help="Clear checkpoint and start from the beginning",
    )
    p.add_argument(
        "--login", action="store_true",
        help=(
            "Open the browser visibly and wait for you to sign in to Google "
            "before scraping. Logged-in sessions get more results per query "
            "and almost never hit captchas. Sign in once; scraping then runs "
            "unattended in the background for the rest of the session."
        ),
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Print all job queries that would be run and exit — no browser, "
            "no scraping. Use this to verify your zone list and config before "
            "starting a full mega run."
        ),
    )
    p.add_argument(
        "--stats", action="store_true",
        help="Print statistics from the existing output file and exit.",
    )
    return p.parse_args()


def main() -> None:
    """Package entry-point — called by both ``__main__`` and the console script."""
    args = _parse_args()

    # Initialise logging now (not at module level) so that --dry-run and
    # --stats don't create a logs/ directory or open a log file.
    setup_logging()

    try:
        cfg = load_config(args.config)
    except (FileNotFoundError, ValueError) as exc:
        print(f"\n❌  Config error:\n    {exc}", file=sys.stderr)
        sys.exit(1)

    if args.fresh:
        clear_checkpoint(cfg)
        clear_done_queries(cfg)
        log.info("🔄  Checkpoint cleared — starting fresh")

    if args.dry_run:
        jobs = build_jobs_mega(cfg) if args.mode == "mega" else build_jobs_city(cfg)
        print(f"\n{'='*60}")
        print(f"  DRY RUN — {len(jobs)} jobs would be executed:")
        print(f"  Mode    : {args.mode.upper()}")
        print(f"  Output  : {build_output_path(cfg)}")
        print(f"{'='*60}")
        for i, j in enumerate(jobs, 1):
            print(f"  {i:>4}. {j['query']}")
        print(f"{'='*60}\n")
        sys.exit(0)

    if args.stats:
        out_path = build_output_path(cfg)
        data = load_existing_output(out_path)
        if not data:
            print(f"No output file found at: {out_path}")
            sys.exit(0)
        n  = len(data)
        we = sum(1 for r in data if r.get("Email"))
        wp = sum(1 for r in data if r.get("Phone"))
        ww = sum(1 for r in data if r.get("Website"))
        print(f"\n  Output : {out_path}")
        print(f"  Records: {n}")
        print(f"  Email  : {we} ({int(we/max(n, 1)*100)}%)")
        print(f"  Phone  : {wp} ({int(wp/max(n, 1)*100)}%)")
        print(f"  Website: {ww} ({int(ww/max(n, 1)*100)}%)")
        sys.exit(0)

    try:
        session = ScraperSession(cfg, mode=args.mode, login_mode=args.login)
        session.run()
    except KeyboardInterrupt:
        print("\n[Use Q key or 'echo stop > command.txt' for a clean exit]")
        sys.exit(0)
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

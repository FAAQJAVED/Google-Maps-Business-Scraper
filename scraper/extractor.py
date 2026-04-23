"""
Google Maps data extraction and website contact enrichment.

Public API
----------
  scroll_and_collect       — scroll the results panel; return list of place hrefs
  extract_place            — navigate to a place URL; extract all business fields
  enrich_batch             — parallel HTTP fetch of email/phone from business websites
  extract_emails_from_html — 5-stage email extraction pipeline
  extract_phones_from_html — phone number extraction and validation

Key improvements in this release
----------------------------------
  • MAX_STALL is now config-driven (performance.max_stalls, default 5 not 3).
    Root cause of the "73 instead of 116" gap: Maps' virtualised renderer pauses
    between card batches. 3 × 25s = 75s was not enough; 5 × 25s = 125s catches
    the second and third render batch reliably.

  • _end_of_list() tries an aria-label DOM attribute check first (O(1), no
    page.content() download) then falls back to inner_text scan. This eliminates
    the 500 KB-2 MB full-page download that Version B called on every scroll cycle.

  • Business names are cleaned at extraction time. Google Maps h1 titles often
    include SEO suffixes: "Acme Ltd | Manchester Estate Agents | Letting Agents".
    Only the segment before the first pipe is kept.

  • Fast-fail on domain-level errors (_fetch_url / enrich_one): connection-level
    errors (SSL, reset, connect timeout) bail the entire domain immediately;
    two consecutive read timeouts also bail.  Previously 8 subpaths were tried
    on unreachable domains, wasting up to 32 s per domain.

  • Cloudflare email decoder (Stage 0): decodes data-cfemail / cdn-cgi XOR-encoded
    addresses before the existing 4 plain-text stages run.

  • Smart contact page discovery: after the homepage fetch, scans <a> links for
    contact-keyword slugs (/talk-to-us, /reach-us …) and tries up to 3 extras.

  • Aggregator URL sanitization: detects URLs like
    https://www.deskjock.reviews/manlets.com/top5 and rewrites them to the real
    embedded domain before any fetch is attempted.
"""

from __future__ import annotations

import html as html_stdlib
import logging
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlsplit, urlunsplit

from playwright.sync_api import Page
from playwright._impl._errors import TargetClosedError

from .utils import clean_phone

urllib3.disable_warnings(InsecureRequestWarning)

log = logging.getLogger("maps_scraper")


# ── Selectors ─────────────────────────────────────────────────────────────────

_FEED_SEL       = 'div[role="feed"]'
_FEED_CARD_SEL  = 'div[role="feed"] a[href*="/maps/place/"]'
_LOAD_MORE = [
    'button[aria-label*="More results"]',
    'button[aria-label*="Next"]',
    'button:has-text("View all")',
    'button:has-text("Show more")',
    'button:has-text("Load more")',
]
_PLACE_ID_RE = re.compile(r"0x[0-9a-f]+:0x[0-9a-f]+")


# ── Scroll & collect ──────────────────────────────────────────────────────────

def scroll_and_collect(
    page: Page,
    state,
    scroll_pause: float = 1.5,
    slow_wait: float = 25.0,
    max_stalls: int = 5,
) -> list[str]:
    """
    Scroll the Maps results panel until all cards are loaded, then return hrefs.

    Slow-connection patience
    ─────────────────────────
    After each scroll, if no new cards appear, polls every 1 s for up to
    slow_wait seconds WITHOUT scrolling again. Scrolling during Maps' lazy-load
    spinner interrupts the network request and resets the spinner.

    max_stalls=5 (default) vs the old 3
    ─────────────────────────────────────
    Google Maps' virtualised renderer pauses between batches. With max_stalls=3
    and slow_wait=25 s the scraper gives up after 75 s of silence, missing cards
    that appear in the second or third batch. 5 × 25 s = 125 s catches them.

    _end_of_list() strategy
    ──────────────────────────
    Checks aria-label attribute on the sentinel element first (O(1) DOM read,
    no page download). Falls back to inner_text scan. Never calls page.content()
    which downloads 500 KB–2 MB of HTML per scroll cycle.

    Args:
        page:         Active Playwright page on a Maps search results view.
        state:        ControlState — checked for early exit.
        scroll_pause: Base seconds between scroll actions.
        slow_wait:    Max seconds to poll for new cards before counting a stall.
        max_stalls:   Consecutive silent periods before declaring end of results.

    Returns:
        Deduplicated list of Google Maps place page URLs.
    """
    POLL_INTERVAL_S = 1.0

    try:
        page.wait_for_selector(_FEED_SEL, timeout=10_000)
    except Exception:
        return []

    feed      = page.locator(_FEED_SEL)
    collected: dict[str, None] = {}
    stall     = 0

    def _harvest() -> int:
        added = 0
        try:
            for card in page.locator(_FEED_CARD_SEL).all():
                href = card.get_attribute("href") or ""
                if not href or href in collected:
                    continue
                if href.startswith("/"):
                    href = "https://www.google.com" + href
                collected[href] = None
                added += 1
        except (TargetClosedError, Exception):
            pass
        return added

    def _wait_for_cards() -> int:
        deadline = time.time() + slow_wait
        logged   = False
        while time.time() < deadline and not state.stop:
            time.sleep(POLL_INTERVAL_S)
            n = _harvest()
            if n > 0:
                return n
            if not logged:
                log.debug("Maps still loading — waiting up to %.0fs", slow_wait)
                logged = True
        return 0

    def _end_of_list() -> bool:
        # Fast path: aria-label attribute (O(1) DOM read, no page download)
        try:
            for label in [
                "You've reached the end of the list.",
                "No more results.",
            ]:
                if page.locator(f'[aria-label="{label}"]').count() > 0:
                    return True
        except Exception:
            pass

        # Slower fallback: inner_text scan (catches locale variants)
        try:
            for sel in [
                'p.fontBodyMedium span',
                'div[role="feed"] > div:last-child span',
                'span[jsname]',
                'div[jsaction*="mouseover"] span',
            ]:
                for el in page.locator(sel).all():
                    try:
                        txt = el.inner_text(timeout=300).strip().lower()
                        if "end of the list" in txt or "no more results" in txt:
                            return True
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    while not state.stop:
        try:
            feed.evaluate("el => el.scrollBy(0, 3000)")
        except (TargetClosedError, Exception):
            break

        time.sleep(scroll_pause)
        new_this_step = _harvest()

        if new_this_step == 0:
            new_this_step = _wait_for_cards()

        if new_this_step == 0:
            stall += 1
            log.debug("Stall %d/%d after %.0fs of waiting", stall, max_stalls, slow_wait)
            if stall >= max_stalls:
                log.debug("Max stalls (%d) reached — declaring end of results", max_stalls)
                break
        else:
            stall = 0

        if _end_of_list():
            log.debug("End-of-list marker detected")
            _harvest()
            break

        for sel in _LOAD_MORE:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=400):
                    btn.click()
                    time.sleep(1.5)
                    break
            except Exception:
                continue

    _harvest()  # final pass
    result = list(collected.keys())
    log.debug("scroll_and_collect: %d unique hrefs collected", len(result))
    return result


# ── Extract place ─────────────────────────────────────────────────────────────

def _get(page: Page, sel: str, attr: str | None = None, t: int = 1_500) -> str:
    """Safely read text or attribute from the first matching element."""
    try:
        el = page.locator(sel).first
        return (
            (el.get_attribute(attr, timeout=t) or "")
            if attr
            else el.inner_text(timeout=t).strip()
        )
    except (TargetClosedError, Exception):
        return ""


def _clean_business_name(raw: str) -> str:
    """
    Strip Google SEO suffixes from h1 business names.

    Google Maps page titles include location keywords after a pipe separator:
      "Acme Lettings | Manchester Estate Agents | Letting Agents"
    Only the actual trading name (before the first |) is useful.
    """
    if not raw:
        return raw
    return raw.split("|")[0].strip()


def extract_place(page: Page, url: str, state) -> dict | None:
    """
    Navigate to a Maps place URL and extract all available business fields.

    Uses direct URL navigation (not click-by-index) so it is immune to
    DOM virtualisation and panel-link pollution.

    Returns:
        Dict with keys: name, google_cat, address, phone_raw, website,
        rating, lat, lng, place_id. Returns None on failure.
    """
    if "/maps/place/" not in url:
        log.debug("extract_place: skipping non-place URL [%s]", url[:80])
        return None

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        time.sleep(0.6)
    except (TargetClosedError, Exception) as exc:
        log.debug("extract_place goto failed [%s]: %s", url[:80], exc)
        return None

    try:
        page.wait_for_selector("h1.DUwDvf", timeout=2_500)
    except (TargetClosedError, Exception):
        return None

    d: dict[str, str] = {}
    d["name"] = _clean_business_name(_get(page, "h1.DUwDvf"))
    if not d["name"]:
        return None

    d["google_cat"] = _get(page, "button.DkEaL")
    raw_addr        = _get(page, '[data-item-id="address"]',   attr="aria-label")
    d["address"]    = raw_addr.replace("Address: ", "").strip()
    raw_ph          = _get(page, '[data-item-id*="phone"]',    attr="aria-label")
    d["phone_raw"]  = raw_ph.replace("Phone: ", "").strip()
    d["website"]    = _get(page, '[data-item-id="authority"]', attr="href")
    d["rating"]     = _get(page, "div.F7nice > span",          t=1_000)

    d["lat"] = d["lng"] = d["place_id"] = ""
    try:
        current_url = page.url
        if "@" in current_url:
            pts = current_url.split("@")[1].split(",")
            d["lat"], d["lng"] = pts[0], pts[1]
        m = _PLACE_ID_RE.search(current_url)
        if m:
            d["place_id"] = m.group(0)
    except (TargetClosedError, Exception):
        pass

    return d


# ── Email extraction ──────────────────────────────────────────────────────────
#
# 5-stage pipeline ordered by signal reliability:
#   Stage 0 — Cloudflare XOR-decoded emails   (data-cfemail / cdn-cgi hrefs)
#   Stage 1 — mailto: hrefs                   (best plain-text signal)
#   Stage 2 — data-email attrs                (WordPress/Elementor)
#   Stage 3 — [at]/(at) variants              (obfuscation)
#   Stage 4 — plain regex on entity-decoded HTML

_AT_VARIANTS  = r"(?:\s*\[at\]\s*|\s*\(at\)\s*|\s+AT\s+|\s*@\s*)"
_DOT_VARIANTS = r"(?:\s*\[dot\]\s*|\s*\(dot\)\s*|\s+DOT\s+|\s*\.\s*)"
_OBFUSC_RE    = re.compile(
    r"[a-zA-Z0-9._%+\-]+" + _AT_VARIANTS + r"[a-zA-Z0-9.\-]+" + _DOT_VARIANTS + r"[a-zA-Z]{2,}",
    re.IGNORECASE,
)
_EMAIL_OK_RE  = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$")
_IMAGE_EXT_RE = re.compile(r"\.(png|jpg|svg|gif|css|js|webp|woff|ttf)$")


def _clean_email_candidate(raw: str) -> str:
    e = raw.lower().strip().strip('.,;"\'()<>')
    e = re.sub(r"\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+", "@", e)
    e = re.sub(r"\s*\[dot\]\s*|\s*\(dot\)\s*|\s+dot\s+", ".", e)
    return re.sub(r"\s+", "", e)


def _decode_cloudflare_email(encoded: str) -> str:
    """
    Decode a Cloudflare-protected email address.

    Cloudflare XOR cipher: first byte is the key; XOR each subsequent byte
    pair with that key to recover the original character. Commonly appears as
    ``data-cfemail="..."`` attributes or ``/cdn-cgi/l/email-protection#...`` hrefs.

    Returns decoded email string, or empty string on failure.
    """
    try:
        encoded = encoded.strip()
        if len(encoded) < 4 or len(encoded) % 2 != 0:
            return ""
        key = int(encoded[:2], 16)
        return "".join(
            chr(int(encoded[i:i + 2], 16) ^ key)
            for i in range(2, len(encoded), 2)
        )
    except (ValueError, IndexError):
        return ""


def extract_emails_from_html(
    html: str,
    junk_emails: frozenset[str],
    junk_domains: frozenset[str],
) -> list[str]:
    """
    4-stage email extraction pipeline.

    Args:
        html:         Raw HTML page content.
        junk_emails:  Exact addresses to discard.
        junk_domains: Domain substrings that flag a non-contact address.

    Returns:
        Deduplicated list of valid email strings, highest-confidence first.
    """
    if not html:
        return []

    candidates: list[str] = []

    # ── Stage 0 — Cloudflare email protection (XOR-encoded) ──────────────────
    # Many UK/EU business sites use Cloudflare, which replaces all mailto: links
    # with XOR-encoded ``data-cfemail`` attributes.  This stage decodes them
    # before the plain-text stages run, recovering emails that would otherwise
    # be invisible to all four downstream stages.
    for m in re.finditer(r'data-cfemail=["\']([0-9a-fA-F]+)["\']', html):
        decoded = _decode_cloudflare_email(m.group(1))
        if decoded and "@" in decoded:
            candidates.append(decoded)

    for m in re.finditer(r'/cdn-cgi/l/email-protection#([0-9a-fA-F]+)', html):
        decoded = _decode_cloudflare_email(m.group(1))
        if decoded and "@" in decoded:
            candidates.append(decoded)

    # ── Stage 1 — mailto: hrefs (best signal) ────────────────────────────────
    candidates += re.findall(
        r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})',
        html, re.IGNORECASE,
    )
    candidates += re.findall(
        r'data-email=["\']([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})["\']',
        html, re.IGNORECASE,
    )
    candidates += _OBFUSC_RE.findall(html)

    decoded     = html_stdlib.unescape(html)
    decoded     = re.sub(r"%20", "", decoded)
    candidates += re.findall(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", decoded,
    )

    valid: list[str] = []
    for raw in candidates:
        e = _clean_email_candidate(raw)
        if not _EMAIL_OK_RE.match(e):
            continue
        if e in junk_emails:
            continue
        domain = e.split("@")[1]
        if any(j in domain for j in junk_domains):
            continue
        if _IMAGE_EXT_RE.search(e):
            continue
        if len(e) > 80:
            continue
        # Discard Cloudflare-hashed and system hex addresses.
        # A local-part that is 20+ lowercase hex characters is never a
        # human inbox — it is a tracking hash or obfuscated system address.
        local_part = e.split("@")[0]
        if len(local_part) >= 20 and re.fullmatch(r"[0-9a-f]+", local_part):
            continue
        valid.append(e)

    return list(dict.fromkeys(valid))


def extract_phones_from_html(
    html: str,
    country_code: str,
    valid_prefixes: list[str],
    valid_lengths: list[int],
) -> list[str]:
    """
    Extract and validate phone numbers from raw HTML.

    Args:
        html:           Raw HTML page content.
        country_code:   Numeric dialing code without '+' (e.g. '44').
        valid_prefixes: Accepted first-N-digit sequences.
        valid_lengths:  Accepted digit counts after normalisation.

    Returns:
        Deduplicated list of normalised digit strings.
    """
    if not html:
        return []
    text = re.sub(r"<[^>]+>", " ", html)
    if country_code:
        pattern = (
            rf"(?<!\d)(?:\+{country_code}|00{country_code}|0)"
            rf"[\d][\d\s\-\(\)\.]{{{8},{13}}}(?!\d)"
        )
    else:
        pattern = r"(?<!\d)[\d][\d\s\-\(\)\.]{8,13}(?!\d)"
    cleaned: list[str] = []
    for p in re.findall(pattern, text):
        n = clean_phone(p, country_code, valid_prefixes, valid_lengths)
        if n:
            cleaned.append(n)
    return list(dict.fromkeys(cleaned))


# ── HTTP enrichment ───────────────────────────────────────────────────────────
#
# Performance optimisations:
#   1. Thread-local requests.Session — one TCP connection pool per worker thread
#   2. LRU domain cache (5000 entries) — franchise chains fetched only once
#   3. Filter frozensets built once per batch, not per enrich_one call

_CONTACT_PATHS = [
    "/contact", "/contact-us", "/about", "/about-us",
    "/get-in-touch", "/enquiries", "/our-team",
]

_tl          = threading.local()
_cache_lock  = threading.Lock()

_DOMAIN_CACHE_MAX   = 5000
_domain_cache: OrderedDict[str, tuple[str, str]] = OrderedDict()

_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection":      "keep-alive",
}


def _get_session(proxy_url: str | None = None) -> requests.Session:
    existing       = getattr(_tl, "session", None)
    existing_proxy = getattr(_tl, "session_proxy", None)
    if existing is None or existing_proxy != proxy_url:
        s = requests.Session()
        s.headers.update(_FETCH_HEADERS)
        s.verify = False
        if proxy_url:
            s.proxies = {"http": proxy_url, "https": proxy_url}
        _tl.session       = s
        _tl.session_proxy = proxy_url
    return _tl.session


def _domain_key(website: str) -> str:
    return re.sub(r"^(?:https?://)?(?:www\.)?", "", website).split("/")[0].lower()


def _lru_get(key: str) -> tuple[str, str] | None:
    with _cache_lock:
        if key not in _domain_cache:
            return None
        _domain_cache.move_to_end(key)
        return _domain_cache[key]


def _lru_set(key: str, value: tuple[str, str]) -> None:
    with _cache_lock:
        if key in _domain_cache:
            _domain_cache.move_to_end(key)
        _domain_cache[key] = value
        if len(_domain_cache) > _DOMAIN_CACHE_MAX:
            _domain_cache.popitem(last=False)


def _fetch_url(
    url: str,
    http_timeout: tuple[int, int],
    hard_timeout: int,
    proxy_url: str | None = None,
) -> tuple[str | None, int, str]:
    """
    Fetch with an absolute hard-kill timeout via a daemon thread.

    Guards against servers that accept the connection then stream 1 byte/s —
    a case where requests' read timeout alone doesn't protect you.

    Returns:
        (html, status_code, error_type)
        error_type: "" (success), "connect" (SSL/reset/connect timeout),
                    "read" (read timeout / hard timeout), "other"
    """
    result: dict[str, Any] = {"html": None, "status": 0, "error_type": ""}

    def _do() -> None:
        try:
            r = _get_session(proxy_url).get(url, timeout=http_timeout, allow_redirects=True)
            result["html"]   = r.text
            result["status"] = r.status_code
        except Exception as exc:
            exc_str = str(exc).lower()
            # Connection-level errors — the entire domain is unreachable;
            # all subpaths will fail identically, so bail the domain.
            if any(x in exc_str for x in [
                "ssl", "certificate", "tlsv1", "eof occurred",
                "connection aborted", "connectionreset",
                "remote end closed", "remotedisconnected",
                "connect timeout", "connection to",
            ]):
                result["error_type"] = "connect"
            elif any(x in exc_str for x in ["read timeout", "read timed out"]):
                result["error_type"] = "read"
            else:
                result["error_type"] = "other"
            result["status"] = -1
            log.debug("Fetch failed [%s]: %s", url, exc)

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=hard_timeout)
    # hard_timeout fired before _do() set a status — treat as read timeout
    if result["status"] == 0 and result["html"] is None:
        result["error_type"] = "read"
    return result["html"], result["status"], result["error_type"]


# ── Improvement D — Aggregator URL sanitization ───────────────────────────────
# Some Google Maps entries store an aggregator URL like
#   https://www.deskjock.reviews/manlets.com/top5
# where the real business domain is embedded in the path. Trying 8 paths on a
# dead aggregator wastes ~32 seconds. Detect and rewrite these up-front.

_DOMAIN_IN_PATH_RE = re.compile(
    r'^/([a-z0-9][a-z0-9\-]*\.[a-z]{2,6}(?:\.[a-z]{2})?)',
    re.IGNORECASE,
)


def _sanitize_website_url(website: str) -> str:
    """
    Detect and fix URLs where another domain is embedded in the path.

    Example:  https://www.deskjock.reviews/manlets.com/top5
              → extracted real domain: manlets.com
              → returns: https://manlets.com

    If no embedded domain is found, returns the original URL unchanged.
    """
    if not website:
        return website
    parts = urlsplit(website)
    m = _DOMAIN_IN_PATH_RE.match(parts.path)
    if m:
        extracted = m.group(1).lower()
        log.debug(
            "Aggregator URL detected — extracting real domain: %s → %s",
            website, extracted,
        )
        return "https://" + extracted
    return website


# ── Improvement C — Smart contact page discovery ──────────────────────────────
# Hardcoded paths miss custom slugs like /talk-to-us, /reach-us, /find-us.
# Scan the homepage HTML for links whose href or link text suggest a contact
# or about page, then try up to 3 additional candidate URLs.

_CONTACT_KEYWORDS = frozenset([
    "contact", "about", "enquir", "get-in-touch", "reach",
    "talk", "find-us", "our-team", "team", "staff", "office",
    "location", "directions", "visit", "meet",
])


def _find_contact_links(html: str, base_url: str) -> list[str]:
    """
    Scan homepage HTML for links likely to be contact/about pages.

    Returns up to 3 candidate absolute URLs that are:
      - same-domain as base_url
      - not already covered by _CONTACT_PATHS
      - whose href path or anchor text contains a contact keyword

    Args:
        html:     Raw homepage HTML.
        base_url: Scheme + netloc + path of the homepage (used to resolve
                  relative hrefs and enforce same-domain constraint).

    Returns:
        List of up to 3 absolute URL strings.
    """
    if not html:
        return []
    base_domain = urlsplit(base_url).netloc.lower()
    found: list[str] = []
    seen_paths: set[str] = set(_CONTACT_PATHS)

    for m in re.finditer(
        r'<a[^>]+href=["\']([^"\'#?]{1,200})["\'][^>]*>(.*?)</a>',
        html, re.IGNORECASE | re.DOTALL,
    ):
        href = m.group(1).strip()
        text = re.sub(r"<[^>]+>", "", m.group(2)).strip().lower()

        if href.startswith("http"):
            abs_url = href
        elif href.startswith("/"):
            parts = urlsplit(base_url)
            abs_url = f"{parts.scheme}://{parts.netloc}{href}"
        else:
            continue

        if urlsplit(abs_url).netloc.lower() != base_domain:
            continue

        path = urlsplit(abs_url).path.rstrip("/")
        if path in seen_paths:
            continue

        href_lower = href.lower()
        if any(kw in href_lower or kw in text for kw in _CONTACT_KEYWORDS):
            seen_paths.add(path)
            found.append(abs_url)
            if len(found) >= 3:
                break

    return found


def enrich_one(
    website: str,
    skip_domains: frozenset[str],
    junk_emails: frozenset[str],
    junk_domains: frozenset[str],
    phone_cfg: dict[str, Any],
    http_timeout: tuple[int, int],
    hard_timeout: int,
    proxy_url: str | None = None,
) -> tuple[str, str]:
    """
    Fetch email and phone from one business website.

    Checks LRU domain cache first; if not cached, tries homepage then
    contact/about pages until both email and phone are found.

    Improvements applied:
      D — Aggregator URL sanitization (rewrite embedded-domain paths)
      A — Fast-fail on connection-level errors; bail after 2 read timeouts
      C — Dynamic contact page discovery from homepage link scan

    Returns:
        (email, phone) — either may be an empty string.
    """
    # ── Improvement D: sanitize aggregator URLs before any other check ────────
    website = _sanitize_website_url(website)

    if not website or any(s in website.lower() for s in skip_domains):
        return "", ""

    if not website.startswith("http"):
        website = "https://" + website

    dk = _domain_key(website)
    cached = _lru_get(dk)
    if cached is not None:
        log.debug("Cache hit: %s", dk)
        return cached

    _parts = urlsplit(website)
    base   = urlunsplit((_parts.scheme, _parts.netloc, _parts.path.rstrip("/"), "", ""))
    emails: list[str] = []
    phones: list[str] = []
    homepage_html: str | None = None   # saved for Improvement C dynamic link scan
    read_timeout_count = 0             # Improvement A: track consecutive read timeouts

    for path in [""] + _CONTACT_PATHS:
        url  = base + path
        html, status, error_type = _fetch_url(url, http_timeout, hard_timeout, proxy_url)

        # Improvement A — connection-level error: entire domain unreachable
        if error_type == "connect":
            log.debug("Domain unreachable (connect error) — skipping all paths: %s", base)
            break

        # Improvement A — read timeout counting: allow 1, bail after 2 consecutive
        if error_type == "read":
            read_timeout_count += 1
            if read_timeout_count >= 2:
                log.debug("Domain slow (2 read timeouts) — bailing: %s", base)
                break
        else:
            read_timeout_count = 0   # reset on any non-timeout response

        if not html or status >= 400:
            continue

        # Improvement C — save homepage for dynamic link discovery
        if path == "" and html:
            homepage_html = html

        if not emails:
            emails = extract_emails_from_html(html, junk_emails, junk_domains)
        if not phones:
            phones = extract_phones_from_html(
                html,
                phone_cfg.get("country_code", ""),
                phone_cfg.get("valid_prefixes", []),
                phone_cfg.get("valid_lengths", []),
            )
        if emails and phones:
            break

    # ── Improvement C — dynamic contact page discovery ────────────────────────
    # Only runs if still missing data AND we got a homepage successfully.
    if (not emails or not phones) and homepage_html:
        dynamic_urls = _find_contact_links(homepage_html, base)
        for dyn_url in dynamic_urls:
            if emails and phones:
                break
            html, status, error_type = _fetch_url(dyn_url, http_timeout, hard_timeout, proxy_url)

            if error_type == "connect":
                log.debug(
                    "Domain unreachable (connect error) on dynamic path — bailing: %s", base,
                )
                break

            if error_type == "read":
                read_timeout_count += 1
                if read_timeout_count >= 2:
                    log.debug(
                        "Domain slow (2 read timeouts) on dynamic path — bailing: %s", base,
                    )
                    break
            else:
                read_timeout_count = 0

            if not html or status >= 400:
                continue

            if not emails:
                emails = extract_emails_from_html(html, junk_emails, junk_domains)
            if not phones:
                phones = extract_phones_from_html(
                    html,
                    phone_cfg.get("country_code", ""),
                    phone_cfg.get("valid_prefixes", []),
                    phone_cfg.get("valid_lengths", []),
                )

    preferred  = phone_cfg.get("preferred_prefix", "")
    best_phone = ""
    if phones:
        if preferred:
            for p in phones:
                if p.startswith(preferred):
                    best_phone = p
                    break
        if not best_phone:
            best_phone = phones[0]

    result = (emails[0] if emails else ""), best_phone
    _lru_set(dk, result)
    log.debug(
        "Enriched %s → email:%s  phone:%s",
        dk, "✓" if result[0] else "✗", "✓" if result[1] else "✗",
    )
    return result


def enrich_batch(
    places: list[dict],
    config: dict[str, Any],
    proxy_mgr: "Any | None" = None,
) -> dict[int, tuple[str, str]]:
    """
    Fetch email/phone from a batch of places in parallel.

    Filter frozensets are pre-computed once per batch, not inside enrich_one.

    Returns:
        Dict mapping list index → (email, phone).
    """
    results: dict[int, tuple[str, str]] = {}
    workers   = config["performance"].get("fetch_threads", 10)
    proxy_url = proxy_mgr.current() if (proxy_mgr and proxy_mgr.has_proxies()) else None

    skip_domains = frozenset(config["filters"].get("skip_domains", []))
    junk_emails  = frozenset(config["filters"].get("junk_emails", []))
    junk_domains = frozenset(config["filters"].get("junk_email_domains", []))
    phone_cfg    = config.get("phone", {})
    http_timeout = tuple(config["performance"]["http_timeout"])
    hard_timeout = config["performance"]["hard_timeout"]

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {
            ex.submit(
                enrich_one,
                p.get("website", ""),
                skip_domains,
                junk_emails,
                junk_domains,
                phone_cfg,
                http_timeout,
                hard_timeout,
                proxy_url,
            ): i
            for i, p in enumerate(places)
        }
        for fut in as_completed(fmap):
            i = fmap[fut]
            try:
                results[i] = fut.result()
            except Exception as exc:
                log.debug("Enrichment error index %d: %s", i, exc)
                results[i] = ("", "")

    return results

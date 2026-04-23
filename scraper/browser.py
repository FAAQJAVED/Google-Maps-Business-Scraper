"""
Playwright browser lifecycle: launch, resource blocking, stealth,
proxy rotation, and captcha handling.

Design principles
-----------------
  • Stealth is LIGHT — only UA rotation, random viewport, and removing the
    navigator.webdriver flag. No geolocation spoofing, no timezone override,
    no WebGL patching. Those caused Maps to serve wrong regional results.

  • Headless by default — the browser is invisible.

  • Captcha handling — if a captcha is detected mid-run, the current browser
    is closed and a new VISIBLE browser is launched so the user can solve
    the challenge. After pressing ENTER, scraping continues in that session.
    The browser stays visible for the remainder of that run (Playwright
    cannot switch headless/visible on the same session). User can minimise it.

  • Proxy rotation — round-robin through a list; marks failed proxies and
    tries the next one automatically.
"""

from __future__ import annotations

import random
import re
import threading
import time
from typing import Any

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
from playwright._impl._errors import TargetClosedError

from .utils import jitter


# ── User agents ───────────────────────────────────────────────────────────────
# Real Chrome / Firefox / Edge strings from popular platforms.
# Rotated per browser session so every restart looks like a different device.

_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

_VIEWPORTS: list[dict[str, int]] = [
    {"width": 1280, "height": 800},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]

# Resources to block — reduces page load time 3-5×
_BLOCK_URL_FRAGMENTS: frozenset[str] = frozenset({
    "google-analytics", "googletagmanager", "doubleclick",
    "googlesyndication", "adservice", "fonts.googleapis", "fonts.gstatic",
})
_BLOCK_RESOURCE_TYPES: frozenset[str] = frozenset({"image", "media", "font"})

# Consent/cookie banner selectors to auto-dismiss
_CONSENT_SELECTORS: list[str] = [
    'button[aria-label*="Accept"]',
    'button[aria-label*="Agree"]',
    'button:has-text("Accept all")',
    'button:has-text("I agree")',
    'form:has(button) button:last-child',
]

# Captcha indicators in page URL or content
_CAPTCHA_URL_MARKERS: list[str] = ["/sorry/", "captcha", "unusual traffic"]
_CAPTCHA_CONTENT_MARKERS: list[str] = ["recaptcha", "captcha", "are you a robot", "unusual traffic"]
_CAPTCHA_SELECTORS: list[str] = [
    'iframe[src*="recaptcha"]',
    'div#captcha',
    'form[action*="/sorry/"]',
]

# Only the webdriver flag removal — nothing that alters Maps' regional behaviour
_STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    if (!window.chrome) {
        window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
    }
}
"""

_MAX_LAUNCH_ATTEMPTS = 5


# ── Proxy manager ─────────────────────────────────────────────────────────────

class ProxyManager:
    """
    Round-robin proxy rotator with automatic failure removal.

    Proxy URL formats accepted:
      "http://ip:port"
      "http://user:password@ip:port"
      "socks5://ip:port"
    """

    def __init__(self, proxies: list[str]) -> None:
        self._proxies  = [p.strip() for p in proxies if p.strip()]
        self._index    = 0
        self._failures: dict[str, int] = {}
        self._lock     = threading.Lock()

    def has_proxies(self) -> bool:
        with self._lock:
            return bool(self._proxies)

    def current(self) -> str | None:
        with self._lock:
            if not self._proxies:
                return None
            return self._proxies[self._index % len(self._proxies)]

    def rotate(self) -> str | None:
        """Move to next proxy and return it."""
        with self._lock:
            if not self._proxies:
                return None
            self._index = (self._index + 1) % len(self._proxies)
            proxy = self._proxies[self._index]
            print(f"  🔄  Switching proxy → {_mask(proxy)}")
            return proxy

    def mark_failed(self, proxy: str, threshold: int = 3) -> None:
        """Track consecutive failures and remove a proxy after threshold failures."""
        with self._lock:
            if proxy not in self._proxies:
                return
            self._failures[proxy] = self._failures.get(proxy, 0) + 1
            if self._failures[proxy] >= threshold:
                self._proxies.remove(proxy)
                self._failures.pop(proxy, None)
                print(f"  ⚠️   Proxy removed after {threshold} failures: {_mask(proxy)}")
            else:
                print(f"  ⚠️   Proxy failure {self._failures[proxy]}/{threshold}: {_mask(proxy)}")

    def to_playwright_dict(self, proxy_url: str | None) -> dict | None:
        """Convert proxy URL string to Playwright's proxy config dict."""
        if not proxy_url:
            return None
        m = re.match(
            r"(?P<scheme>https?|socks5)://(?:(?P<user>[^:@]+):(?P<pw>[^@]+)@)?(?P<host>.+)",
            proxy_url,
        )
        if not m:
            return {"server": proxy_url}
        d: dict[str, str] = {"server": f"{m.group('scheme')}://{m.group('host')}"}
        if m.group("user"):
            d["username"] = m.group("user")
            d["password"] = m.group("pw") or ""
        return d


def _mask(proxy: str) -> str:
    """Hide credentials in a proxy URL for safe logging."""
    return re.sub(r"://[^@]+@", "://*****@", proxy)


# ── Captcha detection ─────────────────────────────────────────────────────────

def is_captcha_page(page: Page) -> bool:
    """Return True if the current page appears to be showing a captcha."""
    try:
        url = page.url.lower()
        if any(m in url for m in _CAPTCHA_URL_MARKERS):
            return True
        content = page.content().lower()
        if any(m in content for m in _CAPTCHA_CONTENT_MARKERS):
            return True
        for sel in _CAPTCHA_SELECTORS:
            try:
                if page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def prompt_human_solve(is_startup: bool = False) -> None:
    """
    Print a clear message asking the user to solve the captcha in the
    visible browser window, then wait for ENTER before continuing.

    After pressing ENTER, scraping resumes in the same Playwright session.
    The visible browser window will stay open — just minimise it.
    DO NOT close the browser window — that will stop the scraper.

    Args:
        is_startup: True for the initial consent check; False for mid-run captchas.
    """
    print()
    print("═" * 65)
    if is_startup:
        print("  🌐  BROWSER IS OPEN — check the window now")
        print()
        print("  1. If you see a cookie / consent banner  →  click Accept")
        print("  2. If you see a CAPTCHA                  →  solve it")
        print("  3. Once Google Maps is visible           →  come back here")
        print()
        print("  After pressing ENTER the browser will continue running")
        print("  in the background.  DO NOT close it — just minimise it.")
    else:
        print("  ⚠️   CAPTCHA DETECTED MID-RUN")
        print()
        print("  Look at the browser window that just opened.")
        print("  Solve the captcha, then press ENTER here.")
        print()
        print("  DO NOT close the browser — just minimise it after solving.")
    print("═" * 65)
    print()
    try:
        input("  >>> Press ENTER when done... ")
    except (EOFError, KeyboardInterrupt):
        pass
    print()
    time.sleep(1.0)


def prompt_login() -> None:
    """
    Ask the user to sign into Google in the visible browser, then wait.

    Why this helps
    ──────────────
    Google Maps serves more results to authenticated sessions:
      • Logged-in users get the full result set without the city-mode cap
      • Captchas are extremely rare with a real Google account
      • The session stays authenticated for the full scrape run (hours)
      • Results are based on your account's region — consistent with what
        you see when you manually search while logged in

    How to use: run with --login flag:
        python maps_scraper.py --mode mega --login

    The browser opens, you log in once, press ENTER, then the scraper runs
    normally in that authenticated session. Minimise the browser — do not
    close it.
    """
    print()
    print("═" * 65)
    print("  🔑  LOGIN MODE — sign in to Google for more results")
    print()
    print("  The browser is now open and showing Google Maps.")
    print()
    print("  Steps:")
    print("  1. Click the Sign In button (top-right of Maps)")
    print("  2. Log in with your Google account")
    print("  3. Wait until Google Maps is fully loaded and shows your")
    print("     account avatar in the top-right corner")
    print("  4. Come back here and press ENTER")
    print()
    print("  The browser will stay open in the background.")
    print("  DO NOT close it — the scraper needs it to stay running.")
    print("═" * 65)
    print()
    try:
        input("  >>> Press ENTER when you are signed in and Maps is loaded... ")
    except (EOFError, KeyboardInterrupt):
        pass
    print()
    time.sleep(1.5)   # let Maps settle after login


# ── Browser launch ────────────────────────────────────────────────────────────

def _launch_with_retry(pw, launch_kwargs: dict, retries: int = 3) -> tuple:
    """
    Launch browser with retry for Windows CDP timing issues.
    Chrome can take 30-60s to fully initialise on Windows.
    """
    import logging as _logging
    _log = _logging.getLogger("maps_scraper")
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            context = pw.chromium.launch_persistent_context(
                **launch_kwargs
            )
            page = (context.pages[0]
                    if context.pages else context.new_page())
            return context, page
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                wait = attempt * 10   # 10s, 20s, 30s
                _log.warning(
                    "⚠️  Browser launch attempt %d/%d failed: %s "
                    "— retrying in %ds...",
                    attempt, retries, exc, wait
                )
                time.sleep(wait)
    raise RuntimeError(
        f"Browser failed to start after {retries} attempts. "
        f"Last error: {last_exc}\n"
        "Try closing all Chrome windows and running again."
    ) from last_exc


def launch_browser(
    playwright,
    config: dict[str, Any],
    proxy_mgr: ProxyManager | None = None,
    force_visible: bool = False,
    login_mode: bool = False,
) -> tuple[BrowserContext, Page]:
    """
    Launch Chrome using launchPersistentContext with a dedicated profile directory.

    WHY launchPersistentContext instead of launch() + new_context()
    ───────────────────────────────────────────────────────────────
    Google sign-in requires a persistent browser session that saves cookies to
    disk between restarts. The old approach (launch() + new_context()) creates
    an in-memory context that is wiped on every run — Google sign-in never
    sticks, and "Chrome for Testing" (Playwright's Chromium) blocks sign-in
    entirely.

    launchPersistentContext() is the Playwright equivalent of TPcc.py's approach:
      - Creates a dedicated 'scraper_profile' directory on disk
      - Saves cookies and session data there persistently
      - First run with --login: you sign in once; session is saved
      - All subsequent runs: session is already authenticated, works silently

    PROFILE DIRECTORY RULE (from Playwright docs)
    ──────────────────────────────────────────────
    Must be a fresh/dedicated directory — NOT the user's real Chrome profile
    (e.g. C:/Users/You/AppData/Local/Google/Chrome/User Data).
    Pointing to the real profile causes pages not to load or Chrome to exit.
    We use 'scraper_profile/' in the project directory.

    SECURITY FLAGS (from TPcc.py — needed for Google sign-in to work)
    ──────────────────────────────────────────────────────────────────
    --disable-web-security               ← allows Google OAuth flow to complete
    --disable-features=ImprovedCookieControls   ← TPcc.py uses this
    --disable-features=PrivacySandboxSettings4  ← TPcc.py uses this
    --disable-blink-features=AutomationControlled ← remove bot detection flag

    Returns:
        (context, page) — context.close() shuts down the browser.
        The type is BrowserContext but all existing browser.close() calls
        work because BrowserContext also has a .close() method.
    """
    import os
    perf        = config["performance"]
    captcha_cfg = config.get("captcha", {})

    # Login mode and force_visible both require a visible window
    headless = not force_visible and not login_mode and perf.get("headless", True)

    vp = random.choice(_VIEWPORTS)

    # Persistent profile directory — created once, reused on every run.
    # This is where sign-in cookies are stored so you don't log in every time.
    profile_dir = os.path.join(os.getcwd(), "scraper_profile")
    os.makedirs(profile_dir, exist_ok=True)

    # ── SECURITY NOTE ─────────────────────────────────────────────────────────────
    # --disable-web-security is required for Google sign-in (OAuth cross-origin flow).
    # Side effect: JS on any scraped page can make cross-origin requests.
    # --ignore-certificate-errors is required for business sites with expired certs.
    # NEVER point this browser profile at sensitive personal accounts or banking sites.
    # The scraper_profile/ directory stores session cookies — keep it private.
    # ──────────────────────────────────────────────────────────────────────────────
    chrome_args = [
        "--no-sandbox",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",   # hide automation
        "--disable-infobars",
        "--disable-web-security",                          # needed for Google OAuth
        "--allow-running-insecure-content",
        "--allow-insecure-localhost",
        "--ignore-certificate-errors",
        "--disable-features=ImprovedCookieControls",       # TPcc.py flag
        "--disable-features=PrivacySandboxSettings4",      # TPcc.py flag
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--mute-audio",
        f"--window-size={vp['width']},{vp['height']}",
    ]

    if headless:
        chrome_args += ["--disable-gpu", "--disable-dev-shm-usage"]

    # Channel preference: real Chrome first, then Playwright's Chromium
    preferred_channel = perf.get("browser_channel", "chrome")

    current_proxy = proxy_mgr.current() if proxy_mgr and proxy_mgr.has_proxies() else None

    for attempt in range(1, _MAX_LAUNCH_ATTEMPTS + 1):
        context: BrowserContext | None = None
        try:
            proxy_dict = proxy_mgr.to_playwright_dict(current_proxy) if proxy_mgr else None
            if current_proxy:
                print(f"  🔀  Proxy: {_mask(current_proxy)}")

            # Build launchPersistentContext kwargs
            ctx_kwargs: dict = {
                "headless":   headless,
                "args":       chrome_args,
                "locale":     "en-GB",
                "user_agent": random.choice(_USER_AGENTS),
                # ── BANNER FIX ────────────────────────────────────────────────
                # Playwright injects --enable-automation into every launch by
                # default. That flag (not the page itself) is what triggers the
                # "Chrome is being controlled by automated test software" banner.
                # --disable-blink-features=AutomationControlled (in chrome_args
                # above) removes the JS-side navigator.webdriver signal, but
                # Playwright's own --enable-automation flag re-adds the banner.
                # The two flags cancel each other out — the banner persists.
                #
                # ignore_default_args tells Playwright NOT to inject that one
                # flag while keeping every other default arg intact. Combined
                # with --disable-blink-features=AutomationControlled already in
                # chrome_args, both the banner AND the JS detection signal are
                # now gone simultaneously.
                #
                # Note: --disable-infobars was removed in Chrome 65 and has no
                # effect; this is the correct modern replacement.
                "ignore_default_args": ["--enable-automation"],
            }
            if headless:
                ctx_kwargs["viewport"] = vp   # only in headless; visible Chrome sizes itself
            if proxy_dict:
                ctx_kwargs["proxy"] = proxy_dict

            # Try real Chrome first, fall back to Playwright Chromium
            used_channel = "chromium (Playwright)"
            for channel in ([preferred_channel, None] if preferred_channel != "chromium" else [None]):
                try:
                    if channel:
                        ctx_kwargs["channel"] = channel
                    else:
                        ctx_kwargs.pop("channel", None)

                    context, page = _launch_with_retry(
                        playwright,
                        {"user_data_dir": str(profile_dir), **ctx_kwargs},
                    )
                    used_channel = channel or "chromium (Playwright)"
                    break
                except Exception as ch_exc:
                    if channel:
                        print(f"  [INFO] '{channel}' not found — falling back to Playwright Chromium")
                        ctx_kwargs.pop("channel", None)
                    else:
                        raise ch_exc

            if context is None:
                raise RuntimeError("No usable browser found")

            # Inject stealth script — removes navigator.webdriver flag
            page.add_init_script(_STEALTH_JS)

            # Resource blocking at context level (avoids CancelledError spam)
            context.route("**/*", _block_resource)

            # Navigate to Google Maps
            page.goto(
                "https://www.google.com/maps",
                wait_until="domcontentloaded",
                timeout=40_000,
            )
            time.sleep(jitter(1.2))

            # Login mode: let the user sign in once; session is saved to scraper_profile/
            if login_mode:
                prompt_login()

            # Captcha/consent prompt for visible non-login mode
            elif force_visible or (captcha_cfg.get("human_solve") and not headless):
                prompt_human_solve(is_startup=True)
                if is_captcha_page(page):
                    print("  ⚠️   Captcha still detected — please solve it completely")
                    prompt_human_solve(is_startup=False)

            # Auto-dismiss consent banners (including iframe-based GDPR banners)
            _dismiss_consent(page)

            mode_label = "headless" if headless else "visible"
            print(f"  ✅  Browser ready [{mode_label} | {used_channel} | profile: scraper_profile/]")
            return context, page

        except Exception as exc:
            print(f"  [BROWSER attempt {attempt}/{_MAX_LAUNCH_ATTEMPTS}] {exc}")
            if context:
                try: context.close()
                except Exception: pass
            if proxy_mgr and proxy_mgr.has_proxies() and current_proxy:
                proxy_mgr.mark_failed(current_proxy)
                current_proxy = proxy_mgr.rotate()
            time.sleep(jitter(5))

    raise RuntimeError(
        f"Browser failed to launch after {_MAX_LAUNCH_ATTEMPTS} attempts.\n"
        "  • Make sure Google Chrome is installed.\n"
        "  • Close any Chrome windows that are already open and try again.\n"
        "  • Or set browser_channel: 'chromium' in config.yaml to use Playwright's built-in."
    )


def handle_captcha(page: Page, playwright, config: dict, proxy_mgr: ProxyManager | None) -> tuple[Browser, Page]:
    """
    Called when a captcha is detected mid-run.

    Closes the current headless browser, reopens a VISIBLE one, navigates
    to the same URL so the user can solve the challenge, then returns the
    new (browser, page) pair.

    Args:
        page:       The current page (will be closed).
        playwright: Playwright context.
        config:     Full config dict.
        proxy_mgr:  Optional proxy manager.

    Returns:
        New (context, page) tuple after user has solved the captcha.
    """
    captcha_url = page.url   # remember where we were

    # Close the current context (which closes the browser)
    try:
        page.context.close()
    except Exception:
        pass

    time.sleep(1)

    # Relaunch visibly
    print("\n  🔐  Captcha detected — relaunching browser in visible mode")
    context, new_page = launch_browser(playwright, config, proxy_mgr, force_visible=True)

    # Navigate back to where we were
    try:
        new_page.goto(captcha_url, wait_until="domcontentloaded", timeout=25_000)
        time.sleep(jitter(1.0))
    except Exception:
        pass

    return context, new_page


# ── Internal helpers ─────────────────────────────────────────────────────────

def _dismiss_consent(page: Page) -> None:
    """
    Click through cookie / consent dialogs if visible.

    Google serves two different consent banner implementations:
      1. Inline in the page DOM — standard selectors work fine.
      2. Inside an iframe (consent.google.com) — shown to European users,
         including anyone connecting through a European VPN exit node.
         page.locator() cannot reach inside iframes, so the old approach
         left the scraper stuck on the banner indefinitely.

    Fix: after trying page-level selectors, also check for a consent iframe
    and try the same selectors inside every frame on the page.
    """
    # Stage 1: try page-level selectors (works without VPN / in non-EU regions)
    for selector in _CONSENT_SELECTORS:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=1_000):
                btn.click()
                time.sleep(jitter(0.6))
                return
        except Exception:
            pass

    # Stage 2: consent iframe (European GDPR flow, e.g. when using a EU VPN)
    # Google loads consent.google.com in an iframe — we must enter that frame.
    try:
        frames = page.frames
        for frame in frames:
            if not frame.url:
                continue
            if "consent.google" not in frame.url and "accounts.google" not in frame.url:
                continue
            for selector in _CONSENT_SELECTORS:
                try:
                    btn = frame.locator(selector).first
                    if btn.is_visible(timeout=1_000):
                        btn.click()
                        time.sleep(jitter(0.6))
                        return
                except Exception:
                    continue
    except Exception:
        pass

    # Stage 3: brute-force — try every frame on the page with every selector.
    # Catches consent dialogs from other hosting patterns.
    try:
        for frame in page.frames:
            if frame == page.main_frame:
                continue  # already tried above
            for selector in _CONSENT_SELECTORS:
                try:
                    btn = frame.locator(selector).first
                    if btn.is_visible(timeout=500):
                        btn.click()
                        time.sleep(jitter(0.6))
                        return
                except Exception:
                    continue
    except Exception:
        pass


def _block_resource(route) -> None:
    """Abort non-essential resource requests to speed up page loads."""
    url = route.request.url
    if any(frag in url for frag in _BLOCK_URL_FRAGMENTS):
        route.abort()
        return
    if route.request.resource_type in _BLOCK_RESOURCE_TYPES:
        route.abort()
        return
    route.continue_()

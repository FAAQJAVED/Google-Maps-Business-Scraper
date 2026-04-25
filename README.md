# Google Maps Business Scraper

Headless Google Maps business scraper with parallel email enrichment, atomic resume checkpointing, and configurable phone/postcode validation — built for any business type, any country, overnight multi-zone lead generation.

---

## What it does

1. **Scrapes Google Maps** for any search query in any city (property managers, dentists, solicitors, restaurants — anything)
2. **Enriches each result** by fetching the business's own website to extract email and phone
3. **Saves to CSV or Excel** with columns: Company Name, Phone, Email, Website, Postcode, Category, Rating, Address

---

## Use cases

| Market | Example query | Country |
|---|---|---|
| Property management | `property managers` | UK, US, Australia |
| Dental practices | `dentists near me` | UK, US, Germany |
| Restaurants | `italian restaurants` | Any |
| Legal services | `solicitors` / `attorneys` | UK / US |
| Plumbers | `emergency plumbers` | Any |
| Accountants | `chartered accountants` | UK, Australia |
| Gyms & fitness | `personal trainers` | Any |
| Estate agents | `real estate agents` | US, Australia |

---

## Quick start

### 1. Install dependencies

```bash
pip install playwright requests pyyaml
playwright install chromium
```

Google Chrome installed on your machine is recommended over Playwright's built-in Chromium — it gets more results and avoids captchas more reliably.

### 2. Configure

Open `config.yaml` and fill in the two required fields:

```yaml
search:
  query: "dentists"            # ★ what to search for
  location: "Manchester"       # ★ city or area
```

For clean phone output, set your country's phone config (UK is pre-filled in `config.yaml`):

```yaml
phone:
  country_code: "44"
  valid_prefixes: ["011","012","013","014","015","016","017","018","019",
                   "020","021","022","023","024","028","029","030","033","07"]
  valid_lengths: [10, 11]
```

### 3. Run

```bash
# City mode — single query, ~60-120 results, good for testing
python maps_scraper.py --mode city

# Mega mode — one query per zone, 500-5000+ results, use overnight
python maps_scraper.py --mode mega
```

Results are saved to `output/MapsScrape_<query>_<location>_<date>.csv`.

---

## All flags

| Flag | Description |
|---|---|
| `--mode city` | Single-query search (default) |
| `--mode mega` | One query per zone — massively more results |
| `--config PATH` | Use a different config file |
| `--fresh` | Clear checkpoint, start from scratch |
| `--login` | Open browser visibly to sign in to Google before scraping |
| `--dry-run` | Preview all job queries without opening a browser |
| `--stats` | Print statistics from the existing output file |

---

## Preview

**Live scraping progress:**
![Terminal progress](assets/terminal_progress.png)

**Sample Excel output:**
![Excel output](assets/output_preview.png)

## Runtime controls

While the scraper is running you can control it without stopping it:

| Action | Keyboard | File |
|---|---|---|
| Pause | `P` | `echo pause > command.txt` |
| Resume | `R` | `echo resume > command.txt` |
| Quit cleanly | `Q` | `echo stop > command.txt` |
| Status | `S` | — |

The scraper saves a checkpoint after every completed zone. If you stop it (or it crashes), just re-run the same command to resume from where it left off.

---

## Mega mode — getting 10-50× more results

Google Maps caps each individual search at roughly 60-120 results regardless of scroll depth. Mega mode works around this by running one search per postcode district (or zip code, or borough), then deduplicating everything:

```yaml
geography:
  region_zones:
    # London (UK) — postcode districts
    - "E1"
    - "EC1"
    - "N1"
    - "NW1"
    - "SE1"
    - "SW1"
    - "W1"
    - "WC1"

    # New York (US) — zip codes
    # - "10001"
    # - "10002"
    # - "10003"
    # - "10004"

    # Berlin (Germany) — PLZ
    # - "10115"
    # - "10117"
    # - "10119"
    # - "10178"
    # - "10179"
```

Run with:
```bash
python maps_scraper.py --mode mega
```

Use `--dry-run` first to see all the queries that will be executed.

---

## Config reference

All keys with their defaults:

| Key | Default | Description |
|---|---|---|
| `search.query` | *(required)* | What to search for |
| `search.location` | *(required)* | City or area |
| `phone.country_code` | `"44"` | Dialing code without + |
| `phone.valid_prefixes` | `[see config]` | Accepted local prefixes |
| `phone.valid_lengths` | `[10, 11]` | Accepted digit counts |
| `phone.preferred_prefix` | `""` | Prefer this prefix if multiple phones found |

Common country phone configs:

| Country | `country_code` | `valid_lengths` | `valid_prefixes` |
|---|---|---|---|
| UK | `"44"` | `[10, 11]` | `["01","02","03","07"]` (see config for full list) |
| US | `"1"` | `[10]` | `[]` (accept all — filter by length) |
| Germany | `"49"` | `[10, 11, 12]` | `["015","016","017","030","040","089"]` |
| Australia | `"61"` | `[9, 10]` | `["02","03","04","07","08"]` |
| France | `"33"` | `[9, 10]` | `["01","02","03","04","05","06","07","09"]` |
| `geography.lat_min/max/lng_min/max` | `0.0` | Bounding box (0 = disabled) |
| `geography.region_zones` | `[]` | Zone list for mega mode |
| `geography.valid_postcode_prefixes` | `[]` | Postcode whitelist |
| `classification.keywords` | `{}` | Category labels and keyword lists |
| `performance.headless` | `true` | Invisible browser (faster) |
| `performance.browser_channel` | `"chrome"` | `"chrome"` or `"chromium"` |
| `performance.scroll_pause` | `1.5` | Seconds between scroll actions |
| `performance.slow_connection_wait` | `25` | Seconds to wait per stall |
| `performance.max_stalls` | `5` | Stall periods before end-of-results |
| `performance.fetch_threads` | `15` | Parallel enrichment workers |
| `performance.browser_restart_every` | `300` | Zones between browser restarts |
| `scheduling.stop_at` | `null` | Auto-stop at HH:MM (24-hour, zero-padded) |
| `scheduling.disk_min_mb` | `500` | Pause if disk space below this (MB) |
| `output.format` | `"csv"` | `"csv"` or `"excel"` |
| `output.directory` | `"output"` | Output folder (auto-created) |
| `stealth.proxies` | `[]` | Proxy list — see `docs/proxy_guide.md` |
| `stealth.rotate_every` | `10` | Rotate proxy every N queries (0 = on failure only) |
| `captcha.human_solve` | `true` | Pause on captcha for manual solve |

---

## Headless mode and detection

**`headless: true` is the recommended setting** (and the default). Contrary to a common assumption, headless Chrome is *not* more likely to be detected and blocked by Google Maps. The scraper:

- Removes the `navigator.webdriver` flag via an init script
- Disables the automation banner (`--disable-blink-features=AutomationControlled`)
- Rotates User-Agent strings across 7 real browser fingerprints
- Rotates viewport sizes
- Blocks tracking/analytics resources to reduce load time

The only reason to run with `headless: false` is debugging, or the `--login` flow.

### Google sign-in (`--login` flag)

Signing in to a real Google account before scraping significantly increases results per zone and nearly eliminates captchas:

```bash
python maps_scraper.py --mode mega --login
```

The browser opens visibly, you sign in once, press Enter, then it runs headlessly in the background for the rest of the session. Your login is saved to `scraper_profile/` for future runs.

---

## Output columns

| Column | Description |
|---|---|
| Company Name | Business trading name (pipe-suffixes stripped) |
| Phone | Cleaned local number (country code stripped) |
| Email | Contact email from the business's website |
| Website | Website URL from the Maps listing |
| Postcode | Extracted from address |
| Category | Keyword-classified label (or "Other") |
| Rating | Google Maps star rating |
| Address | Full address string |
| Email Status | `found` or `notfound` |
| Phone Status | `found` or `notfound` |
| Source | Always `Google Maps` |

---

## Performance

Typical figures on a standard broadband connection (no proxy):

| Mode | Result set | Time |
|---|---|---|
| City | 60-120 results | 8-15 min |
| Mega (20 zones) | 500-900 results | 2-4 hours |
| Mega (100 zones) | 2000-5000 results | 8-20 hours |

Enrichment runs in parallel (15 threads by default). The biggest time cost is Maps extraction (~4-5 seconds per place), not enrichment.

---

## File structure

```
maps_scraper.py          # entry point
dedupe_tool.py           # standalone deduplication / merge / subtract tool
config.yaml              # all configuration
scraper/
  browser.py             # Playwright browser lifecycle, proxy rotation, captcha
  config.py              # config loading, deep-merge, validation
  controls.py            # P/R/Q/S keyboard controls + command.txt
  extractor.py           # scroll, extract_place, enrich_batch
  filters.py             # geographic filter, dedup, classification
  storage.py             # CSV/Excel output, checkpoint, done-queries log
  utils.py               # phone cleaning, disk check, stop_at, beep, backoff
tests/
  test_scraper.py        # 130+ unit tests, all pure-function (no browser needed)
docs/
  proxy_guide.md         # proxy setup and formatting
output/                  # results saved here (auto-created)
logs/                    # rotating log files (auto-created)
```

---

## Running tests

```bash
pip install pytest
pytest tests/ -v
```

All tests are pure-function and run in under 3 seconds with no browser or internet required.

---

## Requirements

- Python 3.10+
- See `requirements.txt` for full list
- Google Chrome installed (optional but recommended over Playwright Chromium)
- For Excel output: `pip install openpyxl`

---

## Troubleshooting

**Getting fewer results than expected?**  
Increase `max_stalls` in `config.yaml` (try 7-8) and `slow_connection_wait` to 35. Large result sets on slow connections can stall between card batches.

**Phone numbers look wrong (missing leading zero, or have country code)?**  
Set `phone.country_code: "44"` and `phone.valid_lengths: [10, 11]` in `config.yaml`.

**Browser won't launch?**  
Set `browser_channel: "chromium"` in `config.yaml` to use Playwright's built-in browser instead of Google Chrome.

**Captcha appearing frequently?**  
Use the `--login` flag to sign in to a Google account. Authenticated sessions almost never hit captchas.

**Email column shows agency emails across multiple businesses?**  
Add the agency domain to `filters.junk_email_domains` in `config.yaml`.

**Running outside the UK?**  
Set `phone.country_code` to your country's dialing code and update `valid_prefixes` and `valid_lengths` accordingly. See the phone config table above for common country examples.

---

## Enrichment improvements

This release ships four targeted improvements to the website contact-enrichment pipeline:

### Fast-fail on domain-level errors

Previously, when a business website was completely unreachable (dead SSL certificate, connection reset, or connect timeout), the scraper continued trying all 7 additional subpaths (`/contact`, `/about`, etc.), wasting up to 32 seconds per domain.

The enricher now classifies every fetch failure:

- **Connection-level errors** (SSL, ConnectionReset, ConnectTimeout): the entire domain is bailed immediately — subpaths are guaranteed to fail identically.
- **Read timeouts**: one further attempt is allowed (a later path may succeed, as confirmed in testing). If a second consecutive read timeout occurs, the domain is bailed.
- **4xx status codes**: treated as "path not found" — the loop continues to the next path normally.

Typical saving: 8–32 seconds per unreachable domain, which adds up significantly on large overnight runs.

### Cloudflare email decoder (Stage 0)

Many UK and EU business websites use Cloudflare's email-protection feature. Cloudflare replaces every `mailto:` link in the HTML with an XOR-encoded `data-cfemail="…"` attribute (or a `/cdn-cgi/l/email-protection#…` href), making the email address invisible to all plain-text extraction methods.

A new Stage 0 decodes these XOR-encoded addresses *before* the existing four plain-text stages run, recovering real email addresses from Cloudflare-protected sites that previously returned `email: ✗` even on successful page loads.

The email pipeline is now a 5-stage pipeline:

| Stage | Source | Signal |
|---|---|---|
| 0 | Cloudflare XOR (`data-cfemail`, `cdn-cgi` hrefs) | Very high |
| 1 | `mailto:` hrefs | High |
| 2 | `data-email` attributes (WordPress/Elementor) | High |
| 3 | `[at]`/`(at)` obfuscation variants | Medium |
| 4 | Plain regex on entity-decoded HTML | Low |

### Smart contact page discovery

The hardcoded contact-path list (`/contact`, `/about`, etc.) misses custom slugs common on UK business sites: `/talk-to-us`, `/reach-us`, `/find-us`, `/enquire`, `/get-a-quote`, and others.

After fetching the homepage, the enricher now scans all `<a>` links for hrefs or anchor text that contain contact-related keywords. Up to 3 candidate same-domain URLs are tried as additional contact pages, using the same fast-fail error classification as the main loop.

Keywords scanned: `contact`, `about`, `enquir`, `get-in-touch`, `reach`, `talk`, `find-us`, `our-team`, `team`, `staff`, `office`, `location`, `directions`, `visit`, `meet`.

### Aggregator URL sanitization

Google Maps occasionally stores a review-aggregator URL as a business's website — for example `https://www.deskjock.reviews/manlets.com/top5`, where the real business domain is embedded in the path. The scraper would previously spend ~32 seconds attempting 8 paths on the dead aggregator.

The sanitizer detects this pattern at the start of `enrich_one()` and rewrites the URL to the real embedded domain (`https://manlets.com`) before any fetch is attempted.

---

## Deduplication tool

`dedupe_tool.py` is a standalone utility for merging, deduplicating, and comparing scraper output files. It works with both `.csv` and `.xlsx` inputs.

```bash
# Merge and deduplicate multiple files
python dedupe_tool.py output/file1.csv output/file2.csv

# With a custom output path
python dedupe_tool.py file1.csv file2.csv --output merged_clean.csv

# Subtract a known list (e.g. remove existing clients from a new leads file)
python dedupe_tool.py map_list.csv --subtract existing_clients.csv

# Use a custom dedup key (default is Name + Address)
python dedupe_tool.py file1.csv file2.csv --key "Name,Phone"
```

**Dedup key** — the `--key` flag specifies which columns identify a unique record. Values are lowercased and whitespace-normalised before comparison, so `"Acme Ltd "` and `"acme ltd"` are treated as the same record.

**Subtract mode** (`--subtract`) — loads a second file and removes any rows from the merged output whose key matches a row in the subtract file. Useful for removing overlap between a freshly scraped list and a block-management or existing-client list.

The output is always a UTF-8 CSV saved to `output/merged_YYYYMMDD_HHMMSS.csv` by default, or to the path given by `--output`.

---

## Disclaimer

This tool is for personal research and lead generation only. Check Google's Terms of Service before use. Rate-limit your requests using `scroll_pause` and `request_delay` to be respectful of their infrastructure.

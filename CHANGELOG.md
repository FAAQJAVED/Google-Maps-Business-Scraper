# Changelog

## v2.1.0 — Interactive Search Configuration

### Added
- **Interactive city-mode search configuration** — running
  `python maps_scraper.py --mode city` now prompts the user to enter the
  business type and location instead of requiring manual edits to
  `config.yaml` before every city-mode run.
- **Automatic `config.yaml` update** — the entered business type and location
  are automatically written to the `search.query` and `search.location`
  fields in `config.yaml`.
- **Input validation** — empty business type or location values are rejected
  and the user is prompted again until valid values are provided.
- **Configuration validation after update** — the updated configuration is
  loaded through the existing configuration validation system before the
  scraper starts.
- **Non-interactive modes preserved** — `mega`, `--dry-run`, and `--stats`
  continue to use the existing configuration workflow without interactive
  prompts.
- **Reusable `ask_search_details()` helper** — added a dedicated function to
  collect search details, update the configuration dictionary, save the YAML
  file, and display the selected search parameters.

---

## v2.0.0 — Final merged release

This release merges the best elements from three parallel development branches
(v1.2.0 / Arena-A / Arena-B) into a single production-ready codebase.

### Fixed — critical
- **`extract_phones_from_html` regex** (Version A regression): `{{8,13}}` inside
  an rf-string produced an unclosed character class (`[\d\s\-\(\)\.{8,13}` with
  no closing `]`), raising `re.error` on every call and silently zeroing 100% of
  phone and email output. Fixed by rewriting the pattern without rf-string
  interpolation of quantifier braces.
- **`Path` import missing in `browser.py`** (Version B regression): `launch_browser`
  called `Path()` without importing it, causing a `NameError` crash on fresh installs.
  Fixed (Version C used `os.path` throughout, so this was never present here).
- **`clean_phone` not wired in enrichment** (Version B regression): `enrich_one`
  stripped digits raw without calling `clean_phone()`, producing UK phone numbers
  missing their leading `0` (e.g. `1619743232` instead of `01619743232`). All
  output phone numbers were un-dialable. Fixed (Version C always wired `clean_phone`).
- **`clear_done_queries` not called on clean completion** (Version B regression):
  After a full run, `scraper_done_queries.txt` was never cleared. The next day's
  run (without `--fresh`) immediately skipped every query and produced zero records.
  Fixed (Version C calls `clear_done_queries` inside `_print_summary` when
  `all_done` is True).

### Fixed — important
- **`max_stalls` hardcoded to 3** (Version C original): Root cause of the
  "73 instead of 116" result gap. Google Maps' virtualised renderer pauses
  between card batches; 3 × 25 s = 75 s was not enough. Default raised to 5
  (= 125 s patience) and exposed as `performance.max_stalls` in `config.yaml`.
- **`page.content()` on every scroll iteration** (Version B regression): Version B's
  `_end_of_list()` called `page.content()` on every scroll cycle, downloading
  500 KB–2 MB of HTML each time and slowing scrolling 3–5×. Replaced with an
  `aria-label` DOM attribute check (O(1)) with `inner_text` fallback. No `page.content()`
  is ever called in the scroll loop.
- **React/TypeScript files in project root** (Version A): `index.html`, `package.json`,
  `package-lock.json`, `tsconfig.json`, `vite.config.ts`, `src/App.tsx`, etc. were
  accidentally bundled into Version A's zip. Removed entirely.
- **Business names include Google SEO pipe suffixes** (all versions): Google Maps
  page titles often contain `"Acme Ltd | Manchester Estate Agents | Letting Agents"`.
  `_clean_business_name()` now strips everything after the first `|` at extraction time.

### Changed
- **`headless: true` is now the default** in `config.yaml`. Headless Chrome is faster
  and is not detectably more likely to be blocked than visible Chrome when combined
  with the existing stealth measures (UA rotation, webdriver flag removal, viewport
  randomisation, resource blocking).
- **Live extraction logging** now shows the business name of the last extracted place
  (`[23/116]  19%  — last: Acme Lettings`) instead of a time estimate. This means the
  terminal is never silent for more than ~40 s during extraction.
- **Progress rate** in the progress bar now shows records/min (total records saved
  divided by elapsed minutes) rather than queries/min, which is more useful for a
  lead-generation tool.
- **Phone config pre-filled** for UK in `config.yaml` (`country_code: "44"`,
  `valid_lengths: [10, 11]`, standard UK prefixes). No config change needed for UK
  users to get clean phone output out of the box.
- **Junk filter lists expanded**: `"mysite.com"`, `"checkatrade.com"`,
  `"rated-people.com"`, `"mailchimp.com"`, `"sendgrid.net"`, `"transactional."` added.

### Added
- `performance.max_stalls` config key (default `5`) — controls how many consecutive
  stall periods the scroll engine waits before declaring end of results.
- `tests/test_scraper.py` — three new test classes:
  - `TestExtractPhonesFromHtml` (10 tests) — covers UK mobile, landline, `+44` strip,
    `0044` strip, length rejection, prefix rejection, empty input, no-country-code path,
    brace-crash regression, and deduplication.
  - `TestCleanBusinessName` (4 tests) — pipe suffix stripping, no-pipe passthrough,
    empty string safety, whitespace trimming.
  - `TestMaxStallsConfig` (2 tests) — default value present, override from user config.
  

---

## v1.2.0 — Version C baseline

- All 15 C-section bugs from the audit resolved
- 106 unit tests passing in < 2 s
- Atomic checkpoint with version checking
- 4-stage email pipeline
- LRU domain cache (5000 entries)
- Thread-local sessions
- Cross-midnight `stop_at` handling
- 3-stage consent banner dismissal (page → consent iframe → all frames)
- Clean terminal output (no `urllib3` warning spam)

## v1.0.0 — Initial release

- City and mega mode
- Basic scroll and extract
- CSV and Excel output

# Contributing

Thank you for considering a contribution. Here is everything you need to know.

---

## Quick start

```bash
# TODO: update this URL before publishing
git clone https://github.com/your-github-username/google-maps-scraper.git
cd google-maps-scraper
pip install -r requirements-dev.txt
pytest tests/ -v          # all tests must pass before you open a PR
```

---

## What to work on

# TODO: update this URL before publishing
Check the [Issues](https://github.com/your-github-username/google-maps-scraper/issues) tab.
Issues labelled **good first issue** are well-scoped starting points.

**Before starting large changes**, open an issue first so we can align on approach.
This avoids wasted work if the direction doesn't fit the project.

---

## Ground rules

| Rule | Detail |
|---|---|
| **Tests must pass** | Run `pytest tests/ -v` before every commit. All existing tests must stay green. |
| **New behaviour = new test** | Any bug fix or feature should include a test that would have caught the bug or proves the feature works. |
| **Pure-function tests only** | Tests must not open a browser, hit the internet, or require API keys. Use `unittest.mock` for Playwright. |
| **Preserve existing comments** | Inline comments and docstrings explain design decisions — don't remove them unless the code they describe is also removed. |
| **One concern per PR** | A PR fixing a bug should not also refactor unrelated code. Keeps reviews fast and reversions clean. |

---

## Code style

- **PEP 8** — standard Python style. No formatter is enforced, but keep line length under ~100 chars.
- **Type hints** — add them for all new public functions. Existing code uses them throughout.
- **Docstrings** — all public functions need a one-line summary plus `Args` / `Returns` sections.

---

## Adding a test

Tests live in `tests/test_scraper.py`. All tests extend `unittest.TestCase` and are
pure-function (no browser, no network).

```python
class TestMyNewFeature(unittest.TestCase):

    def test_it_does_the_thing(self):
        result = my_function("input")
        self.assertEqual(result, "expected output")
```

Run the full suite to confirm nothing is broken:

```bash
pytest tests/ -v
```

---

## Commit message format

```
<type>: <short summary>

[optional body — explain why, not what]
```

Types: `fix`, `feat`, `test`, `docs`, `refactor`, `chore`

Examples:
```
fix: handle empty address in make_uid
feat: add --fresh flag to city mode
test: cover checkpoint atomic write edge case
docs: add US phone config example to README
```

---

## Submitting a PR

1. Fork the repo and create a branch: `git checkout -b fix/my-bug`
2. Make your changes
3. Run `pytest tests/ -v` — all green
4. Push and open a Pull Request against `main`
5. Fill in the PR template describing what changed and why

A maintainer will review within a few days.

---

## Updating selectors when Google changes its DOM

Google periodically renames its obfuscated CSS classes. If the scraper starts
returning empty names or addresses, you will see a `🚨 SELECTOR ALERT` warning
in the terminal. Update these constants in `scraper/extractor.py`:

| Variable | Selector | Extracts |
|---|---|---|
| `_FEED_CARD_SEL` | `div[role="feed"] a[href*="/maps/place/"]` | Card hrefs — unlikely to change |
| (h1 selector) | `h1.DUwDvf` | Business name |
| (category) | `button.DkEaL` | Google category label |
| (feed) | `div[role="feed"]` | Results panel — stable |
| (address) | `[data-item-id="address"]` | Full address |
| (phone) | `[data-item-id*="phone"]` | Phone number |
| (website) | `[data-item-id="authority"]` | Website URL |
| (rating) | `div.F7nice > span` | Star rating |

**How to find the new selector:** Open Google Maps in Chrome, right-click the
business name on an open place panel → Inspect. Find the `h1` or `h2` element
and copy its class name. Update the constant and run a city-mode test to confirm.

After updating, run `pytest tests/ -v` — all existing tests must still pass.
Add a comment noting the date of the selector update so future contributors
can track DOM change history.

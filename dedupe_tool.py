#!/usr/bin/env python3
"""
Deduplication and cross-list comparison tool for Maps Scraper output files.

Usage examples
--------------

  # Merge and deduplicate multiple files:
  python dedupe_tool.py output/file1.csv output/file2.csv

  # With custom output path:
  python dedupe_tool.py file1.csv file2.csv --output merged_clean.csv

  # Remove rows that appear in a "subtract" list (e.g. remove block management
  # overlaps from a map list):
  python dedupe_tool.py map_list.csv --subtract block_management.csv

  # Use custom dedup key columns:
  python dedupe_tool.py file1.csv --key "Name,Phone"

Works with both .csv and .xlsx input files.
The dedup key is case-insensitive and whitespace-normalised before comparison.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path


# ── Key construction ──────────────────────────────────────────────────────────

def _make_key(row: dict, key_cols: list[str]) -> str:
    """
    Build a normalised dedup key from one or more columns.

    Values are lowercased, leading/trailing whitespace stripped, and
    internal runs of whitespace collapsed to a single space before joining
    with '||'. This ensures "Acme Ltd " and "acme ltd" compare as equal.
    """
    parts = []
    for col in key_cols:
        val = str(row.get(col, "")).strip().lower()
        val = re.sub(r"\s+", " ", val)
        parts.append(val)
    return "||".join(parts)


# ── File I/O ──────────────────────────────────────────────────────────────────

def load_file(path: Path) -> list[dict]:
    """
    Load a CSV or XLSX file into a list of row dicts.

    UTF-8 BOM is stripped automatically from CSV files.  XLSX files are
    read in read-only mode for efficiency; headers come from row 1.
    """
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        try:
            import openpyxl
        except ImportError:
            print("  ⚠️  openpyxl is required for .xlsx files:  pip install openpyxl")
            sys.exit(1)
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [str(h) if h is not None else "" for h in rows[0]]
        return [
            {k: (str(v) if v is not None else "") for k, v in zip(headers, row)}
            for row in rows[1:]
        ]
    else:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))


def save_csv(rows: list[dict], path: Path) -> None:
    """Write a list of row dicts to a UTF-8-BOM CSV file."""
    if not rows:
        print("  No rows to save.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dedupe_tool",
        description="Deduplicate and merge Maps Scraper output files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "inputs", nargs="+", metavar="FILE",
        help="Input CSV or XLSX files to merge and deduplicate.",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output file path (default: output/merged_YYYYMMDD_HHMMSS.csv).",
    )
    parser.add_argument(
        "--subtract", "-s", default=None, metavar="FILE",
        help=(
            "Remove rows that appear in this file from the merged output.  "
            "Useful for subtracting a known list (e.g. existing clients)."
        ),
    )
    parser.add_argument(
        "--key", "-k", default="Name,Address",
        help=(
            "Comma-separated column names used as the dedup key "
            "(default: 'Name,Address').  "
            "Keys are case-insensitive and whitespace-normalised."
        ),
    )
    args = parser.parse_args()

    key_cols = [c.strip() for c in args.key.split(",")]

    print()
    print(f"  Dedup key  : {key_cols}")
    print(f"  {'─' * 55}")

    # ── Load input files ──────────────────────────────────────────────────────
    all_rows: list[dict] = []
    for path_str in args.inputs:
        path = Path(path_str)
        if not path.exists():
            print(f"  ⚠️  File not found — skipping: {path}")
            continue
        rows = load_file(path)
        print(f"  Loaded {len(rows):>6,} rows  ←  {path.name}")
        all_rows.extend(rows)

    total_in = len(all_rows)
    print(f"  {'─' * 55}")
    print(f"  Total loaded : {total_in:,}")

    if not all_rows:
        print("  No rows loaded — nothing to write.\n")
        sys.exit(0)

    # ── Deduplication ─────────────────────────────────────────────────────────
    seen: set[str] = set()
    deduped: list[dict] = []
    for row in all_rows:
        k = _make_key(row, key_cols)
        if k and k not in seen:
            seen.add(k)
            deduped.append(row)
        # Rows with an empty key (all key columns blank) are passed through
        # once; subsequent blanks are dropped as likely garbage rows.
        elif not k and "" not in seen:
            seen.add("")
            deduped.append(row)

    removed_dup = total_in - len(deduped)
    print(f"  Duplicates   : {removed_dup:,} removed")
    print(f"  After dedup  : {len(deduped):,}")

    # ── Subtract list ─────────────────────────────────────────────────────────
    if args.subtract:
        sub_path = Path(args.subtract)
        if not sub_path.exists():
            print(f"  ⚠️  Subtract file not found: {sub_path}")
        else:
            sub_rows = load_file(sub_path)
            sub_keys  = {_make_key(r, key_cols) for r in sub_rows}
            before    = len(deduped)
            deduped   = [r for r in deduped if _make_key(r, key_cols) not in sub_keys]
            removed_sub = before - len(deduped)
            print(f"  Subtract     : {sub_path.name} ({len(sub_rows):,} rows)")
            print(f"  Subtracted   : {removed_sub:,} rows removed")

    # ── Save output ───────────────────────────────────────────────────────────
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.output) if args.output else Path(f"output/merged_{ts}.csv")
    save_csv(deduped, out_path)

    print(f"  {'─' * 55}")
    print(f"  ✅  Final rows : {len(deduped):,}")
    print(f"  Output         : {out_path}")
    print()


if __name__ == "__main__":
    main()

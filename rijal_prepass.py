#!/usr/bin/env python3
"""
Rijāl Name Pre-Pass (v2)
========================
Builds two lookup structures from rijal_entries.json using pure text
parsing. NO LLM required — runs in seconds.

The Tehran edition number (n3) has ~3,100 duplicates across 15,545 entries,
so we use the sequential `idx` (0-based JSON position) as the primary key.

Outputs:
    rijal_name_index.json  — {idx → {name_ar, n3, n1, n2}} for all entries
    rijal_n3_index.json    — {n3 → [list of idx values]} for cross-ref lookup

rijal_builder.py uses both:
  - rijal_name_index.json to look up names by idx
  - rijal_n3_index.json   to resolve "الآتي [n3]" cross-references to idx values

v2 changes:
  - Improved extract_name() to strip embedded next-entry numbers from text
    (~5,000 entries have the next entry's n3 embedded at the start)
  - Extended Arabic-Indic digit pattern to also catch Eastern Arabic digits
"""

import argparse
import json
import re
import sys
from pathlib import Path

SCRIPT_DIR      = Path(__file__).parent
ENTRIES_FILE    = SCRIPT_DIR / "rijal_entries.json"
NAME_INDEX_FILE = SCRIPT_DIR / "rijal_name_index.json"
N3_INDEX_FILE   = SCRIPT_DIR / "rijal_n3_index.json"

# Arabic-Indic and Extended Arabic-Indic digits (U+0660–U+0669, U+06F0–U+06F9)
# Also match optional whitespace between digits
AR_DIGIT_PATTERN = re.compile(r'^[\u0660-\u0669\u06F0-\u06F9\s]+')
WHITESPACE = re.compile(r'\s+')


def extract_name(text: str) -> str:
    """
    Extract the narrator name from raw entry text.

    Entry format (typical):
        [optional_tehran_num]NAME [kunyah] [laqab] [nisba] :biography text...

    The name is everything between the optional leading numeral and the
    first colon (:) or dash (-) separator.

    ~5,000 entries have the NEXT entry's n3 embedded at the start of the
    text (e.g., "٢٢أبان بن أبي عمران"). We strip those leading digits
    aggressively.
    """
    text = text.strip()

    # Remove leading Arabic-Indic numerals (Tehran edition number sometimes
    # embedded at the start of the text field — may be the current OR next
    # entry's number)
    text = AR_DIGIT_PATTERN.sub('', text).strip()

    # Take everything before the first colon (primary separator)
    if ':' in text:
        name = text.split(':')[0]
    elif ' -' in text:
        # Some entries use " -" as separator when no colon present
        name = text.split(' -')[0]
    else:
        # Fallback: take up to 100 characters
        name = text[:100]

    # Normalize internal whitespace
    name = WHITESPACE.sub(' ', name).strip()
    return name


def build_index() -> None:
    if not ENTRIES_FILE.exists():
        print(f"ERROR: {ENTRIES_FILE} not found.", file=sys.stderr)
        print("Make sure rijal_entries.json is in the same directory.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {ENTRIES_FILE.name} ...")
    with open(ENTRIES_FILE, 'r', encoding='utf-8') as f:
        entries = json.load(f)

    print(f"Building name index for {len(entries):,} entries ...")
    print(f"  Primary key: 'idx' (0-based sequential, always unique)")
    print(f"  Secondary key: 'n3' (Tehran edition, has ~3,100 duplicates)")

    # Primary index: idx → entry info
    name_index: dict[str, dict] = {}
    # Secondary index: n3 → list of idx values (for cross-ref resolution)
    n3_index: dict[str, list] = {}

    for entry in entries:
        idx  = str(entry['idx'])    # 0-based JSON array position (always unique)
        n3   = entry['n3']          # Tehran edition number (may have duplicates)
        n1   = entry['n1']
        n2   = entry['n2']
        name = extract_name(entry['text'])

        name_index[idx] = {
            "name_ar": name,
            "n3": n3,
            "n1": n1,
            "n2": n2,
        }

        if n3 not in n3_index:
            n3_index[n3] = []
        n3_index[n3].append(idx)

    with open(NAME_INDEX_FILE, 'w', encoding='utf-8') as f:
        json.dump(name_index, f, ensure_ascii=False, indent=2)
    with open(N3_INDEX_FILE, 'w', encoding='utf-8') as f:
        json.dump(n3_index, f, ensure_ascii=False, indent=2)

    # Stats
    dup_n3 = {n: idxs for n, idxs in n3_index.items() if len(idxs) > 1}
    print(f"\n✓ Name index: {len(name_index):,} entries → {NAME_INDEX_FILE.name}")
    print(f"✓ N3 index:   {len(n3_index):,} unique n3 values → {N3_INDEX_FILE.name}")
    print(f"  ({len(dup_n3):,} n3 values have multiple entries — this is normal)")

    # ── Verification sample ──────────────────────────────────────────────────
    print("\nSample (entries 0–14):")
    for idx_str, v in list(name_index.items())[:15]:
        print(f"  [idx={idx_str:>5}, n3={v['n3']:>5}] {v['name_ar'][:60]}")

    # ── Sanity checks ────────────────────────────────────────────────────────
    empty = [i for i, v in name_index.items() if not v['name_ar']]
    if empty:
        print(f"\n⚠  {len(empty)} entries had empty names (idx): {empty[:10]}")
    else:
        print("\n✓ All entries have non-empty names.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build rijal name/n3 index")
    parser.add_argument("--entries-file", type=Path, default=None,
                        help="Path to entries JSON (default: rijal_entries.json)")
    args = parser.parse_args()

    if args.entries_file:
        ENTRIES_FILE    = args.entries_file.resolve()
        stem            = ENTRIES_FILE.stem          # e.g. "rijal_entries_mujam"
        NAME_INDEX_FILE = ENTRIES_FILE.parent / f"rijal_name_index_{stem.split('_', 2)[-1]}.json"
        N3_INDEX_FILE   = ENTRIES_FILE.parent / f"rijal_n3_index_{stem.split('_', 2)[-1]}.json"

    build_index()

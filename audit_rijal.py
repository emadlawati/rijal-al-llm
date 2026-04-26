"""
Rijal Audit Script
==================
1. Finds all gaps in n3 (Tehran edition) numbering in rijal_entries.json
2. Checks which entries appear multiple times (same n3)
3. Compares entries vs database for missing/extra keys
4. Outputs a full gap report to audit_report.txt
"""

import json
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).parent

# ── Arabic numeral helpers ─────────────────────────────────────────────────────
_AR = {'٠':0,'١':1,'٢':2,'٣':3,'٤':4,'٥':5,'٦':6,'٧':7,'٨':8,'٩':9}

def ar_to_int(s: str) -> int:
    result = 0
    for c in str(s):
        if c in _AR:
            result = result * 10 + _AR[c]
        elif c.isdigit():
            result = result * 10 + int(c)
    return result

def int_to_ar(n: int) -> str:
    _INT = {v:k for k,v in _AR.items()}
    if n <= 0:
        return '٠'
    digits = []
    while n:
        digits.append(_INT[n % 10])
        n //= 10
    return ''.join(reversed(digits))

# ── Load data ──────────────────────────────────────────────────────────────────
print("Loading rijal_entries.json...")
with open(SCRIPT_DIR / "rijal_entries.json", 'r', encoding='utf-8') as f:
    entries = json.load(f)

print("Loading rijal_database.json...")
with open(SCRIPT_DIR / "rijal_database.json", 'r', encoding='utf-8') as f:
    db = json.load(f)

print(f"  Entries: {len(entries):,}")
print(f"  Database records: {len(db):,}")
print()

# ── Analysis ───────────────────────────────────────────────────────────────────
# Convert all n3 values to integers
entries_by_n3_int = defaultdict(list)  # int -> list of array-idx
for array_idx, e in enumerate(entries):
    n3_int = ar_to_int(e['n3'])
    entries_by_n3_int[n3_int].append(array_idx)

all_n3_ints = sorted(entries_by_n3_int.keys())
min_n3 = all_n3_ints[0]
max_n3 = all_n3_ints[-1]

print(f"N3 range: {min_n3} – {max_n3}")
print(f"Unique n3 values: {len(all_n3_ints):,}")
print()

# Find gaps (numbers in range with NO entry)
gaps = []
for n in range(min_n3, max_n3 + 1):
    if n not in entries_by_n3_int:
        gaps.append(n)

# Find duplicates (same n3 appears more than once as separate array elements)
duplicates = {n: idxs for n, idxs in entries_by_n3_int.items() if len(idxs) > 1}

# Find fused entries (entries whose text contains multiple entry-number sequences)
import re
fused_pattern = re.compile(r'[\u0660-\u0669]{3,4}(?:\s*-\s*[\u0660-\u0669]{3,4}){1,}')
fused_entries = []
for array_idx, e in enumerate(entries):
    n3_int = ar_to_int(e['n3'])
    text = e['text'][:200]  # check start of text for embedded numbers
    # Look for sequences like ١٠٤٣ج١٠٤٢٣ or similar number clusters
    # Also look for "standalone newline-separated" number runs in text
    if fused_pattern.search(text):
        fused_entries.append((array_idx, e['n3'], text[:300]))

# Database key analysis
db_int_keys = set()
db_overflow_keys = []
for k in db.keys():
    if '_overflow_' in k:
        db_overflow_keys.append(k)
    else:
        db_int_keys.add(ar_to_int(k))

# Which n3 values are in entries but NOT in database
entries_n3_set = set(all_n3_ints)
db_n3_set = db_int_keys

processed_range_max = max(db_n3_set) if db_n3_set else 0
# Only check gaps within the processed range
missing_from_db = []
for n in range(min_n3, processed_range_max + 1):
    if n in entries_n3_set and n not in db_n3_set:
        missing_from_db.append(n)

# ── Write report ───────────────────────────────────────────────────────────────
report_path = SCRIPT_DIR / "audit_report.txt"
with open(report_path, 'w', encoding='utf-8') as f:
    f.write("=" * 70 + "\n")
    f.write("  RIJAL ENTRIES AUDIT REPORT\n")
    f.write("=" * 70 + "\n\n")

    f.write(f"  Total array entries in rijal_entries.json : {len(entries):,}\n")
    f.write(f"  Unique n3 (Tehran) values                 : {len(all_n3_ints):,}\n")
    f.write(f"  N3 range                                  : {min_n3} – {max_n3}\n")
    f.write(f"  Total numbers expected in range           : {max_n3 - min_n3 + 1:,}\n")
    f.write(f"  Database records (processed so far)       : {len(db):,}\n")
    f.write(f"  Overflow entries in DB                    : {len(db_overflow_keys)}\n")
    f.write("\n")

    # ── Gaps ──
    f.write("─" * 70 + "\n")
    f.write(f"  GAPS IN N3 SEQUENCE: {len(gaps)} missing numbers\n")
    f.write("─" * 70 + "\n")
    if gaps:
        # Group consecutive gaps into ranges for readability
        gap_ranges = []
        start = gaps[0]
        end = gaps[0]
        for g in gaps[1:]:
            if g == end + 1:
                end = g
            else:
                gap_ranges.append((start, end))
                start = g
                end = g
        gap_ranges.append((start, end))

        for gs, ge in gap_ranges:
            if gs == ge:
                f.write(f"  Missing: {int_to_ar(gs)} ({gs})\n")
            else:
                f.write(f"  Missing: {int_to_ar(gs)}–{int_to_ar(ge)} ({gs}–{ge})  [{ge-gs+1} entries]\n")
    else:
        f.write("  No gaps found — sequence is complete!\n")
    f.write("\n")

    # ── Duplicates ──
    f.write("─" * 70 + "\n")
    f.write(f"  DUPLICATE N3 VALUES (same number, multiple array entries): {len(duplicates)}\n")
    f.write("─" * 70 + "\n")
    if duplicates:
        for n3_int, idxs in sorted(duplicates.items()):
            f.write(f"  n3={int_to_ar(n3_int)} ({n3_int})  →  array indices: {idxs}\n")
            for idx in idxs:
                e = entries[idx]
                preview = e['text'].replace('\n', ' ').replace('\r', '')[:120].strip()
                f.write(f"    [{idx}] {preview}\n")
    else:
        f.write("  No duplicate n3 values.\n")
    f.write("\n")

    # ── Missing from DB ──
    f.write("─" * 70 + "\n")
    f.write(f"  ENTRIES IN RANGE BUT MISSING FROM DATABASE: {len(missing_from_db)}\n")
    f.write("─" * 70 + "\n")
    if missing_from_db:
        for n in missing_from_db[:50]:  # cap at 50
            ar = int_to_ar(n)
            idxs = entries_by_n3_int.get(n, [])
            if idxs:
                preview = entries[idxs[0]]['text'].replace('\n', ' ')[:80].strip()
                f.write(f"  n3={ar} ({n})  array_idx={idxs}: {preview}\n")
            else:
                f.write(f"  n3={ar} ({n})  [not in entries either — pure gap]\n")
        if len(missing_from_db) > 50:
            f.write(f"  ... and {len(missing_from_db)-50} more\n")
    else:
        f.write("  All entries in range are present in database.\n")
    f.write("\n")

    # ── Overflow keys ──
    if db_overflow_keys:
        f.write("─" * 70 + "\n")
        f.write(f"  OVERFLOW ENTRIES IN DATABASE (fused OCR entries):\n")
        f.write("─" * 70 + "\n")
        for k in sorted(db_overflow_keys):
            rec = db[k]
            f.write(f"  Key: {k}  →  {rec.get('name_ar','?')} / {rec.get('name_en','?')}\n")
        f.write("\n")

    # ── Show surrounding context for first 20 gaps ──
    f.write("─" * 70 + "\n")
    f.write("  DETAIL: ENTRIES ADJACENT TO FIRST 20 GAPS\n")
    f.write("─" * 70 + "\n")
    shown = 0
    for gap_n in gaps[:20]:
        f.write(f"\n  [GAP] n3={int_to_ar(gap_n)} ({gap_n}) is MISSING from rijal_entries.json\n")
        # Show entry before and after
        prev_idxs = entries_by_n3_int.get(gap_n - 1, [])
        next_idxs = entries_by_n3_int.get(gap_n + 1, [])
        if prev_idxs:
            e = entries[prev_idxs[-1]]
            preview = e['text'].replace('\n', ' ')[:200].strip()
            f.write(f"  BEFORE [{prev_idxs[-1]}] n3={e['n3']}: {preview}\n")
        if next_idxs:
            e = entries[next_idxs[0]]
            preview = e['text'].replace('\n', ' ')[:200].strip()
            f.write(f"  AFTER  [{next_idxs[0]}] n3={e['n3']}: {preview}\n")
        shown += 1

print(f"Report written to: {report_path}")
print()
print(f"SUMMARY:")
print(f"  Gaps in n3 sequence : {len(gaps)}")
print(f"  Duplicate n3 values : {len(duplicates)}")
print(f"  Missing from DB     : {len(missing_from_db)}")
print(f"  Overflow DB entries : {len(db_overflow_keys)}")

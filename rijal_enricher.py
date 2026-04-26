"""
rijal_enricher.py
=================
Adds `_raw_full` to every entry in rijal_database.json from the scraped
Mu'jam Rijal Markdown files.

Fast pre-indexed approach:
  1. Concatenate all 24 volume MDs, strip page markers.
  2. One regex pass on the RAW corpus → list of (pos, header_text) for every
     entry header line found.
  3. Build index: norm_first2words → sorted list of char positions.
  4. For each DB entry (in _entry_idx order), dict-lookup then pick the
     first position >= the previous match → always moves forward.

Run:
  python rijal_enricher.py           # enrich & save
  python rijal_enricher.py --stats   # dry-run, show match rate only
"""

import bisect
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────
DB_PATH      = Path("rijal_database.json")
BOOKS_MD_DIR = Path("books_md")
VOLUME_IDS   = list(range(5176, 5200))
STATS_ONLY   = "--stats" in sys.argv

# ── Arabic normalisation  (keys only — never applied to corpus) ───────────────
_DIAC = re.compile(r'[\u064b-\u065f\u0670]')
_ALEF = re.compile(r'[إأآا]')

def norm(text: str) -> str:
    """Normalise for comparison: strip diacritics, unify alef, collapse spaces."""
    text = _DIAC.sub('', text)
    text = _ALEF.sub('ا', text)
    text = text.replace('ـ', '')
    return re.sub(r'[ \t]+', ' ', text).strip()   # spaces only, NOT newlines

def key2(name: str) -> str:
    return ' '.join(norm(name).split()[:2])

def key3(name: str) -> str:
    return ' '.join(norm(name).split()[:3])

# ── Build corpus (raw, newlines preserved) ───────────────────────────────────
print("Loading MD volumes …", flush=True)
parts: list[str] = []
for vid in VOLUME_IDS:
    p = BOOKS_MD_DIR / f"{vid}.md"
    if not p.exists():
        continue
    raw = p.read_text(encoding="utf-8")
    # Remove page-separator lines only, keep content lines intact
    stripped = re.sub(r'---\r?\n\r?\n## Page [^\n]+\n', '\n', raw)
    stripped = re.sub(r'^# [^\n]+\n> [^\n]+\n', '', stripped, flags=re.MULTILINE)
    parts.append(stripped)

corpus = "\n".join(parts)
print(f"  Corpus: {len(corpus):,} chars across {len(parts)} volumes", flush=True)

# ── Single-pass: index all entry headers ──────────────────────────────────────
# Entry header: line that starts with digits, dash, Arabic name, colon.
# We search on the raw corpus — positions are valid for slicing corpus.
HEADER_RE = re.compile(
    r'(?m)^\s*(\d+)\s*[-–]\s*([^\n:]{3,80}?)\s*:[ \t]*(?:\r?\n|$)'
)

print("Indexing entry headers …", flush=True)

headers_pos:  list[int]  = []   # sorted char positions of each header
headers_name: list[str]  = []   # normalised header name at same index

for m in HEADER_RE.finditer(corpus):
    hname = norm(m.group(2))
    headers_pos.append(m.start())
    headers_name.append(hname)

print(f"  Found {len(headers_pos):,} candidate headers", flush=True)

if len(headers_pos) == 0:
    # Debug: show a slice of the corpus so we can see the actual format
    sample = corpus[corpus.find('آدم') - 100 : corpus.find('آدم') + 200] if 'آدم' in corpus else corpus[:500]
    print("  DEBUG sample around first Arabic name:")
    print(repr(sample[:400]))
    sys.exit(1)

# Build key → sorted list of positions index
key_to_idx: dict[str, list[int]] = defaultdict(list)
for i, hname in enumerate(headers_name):
    k = ' '.join(hname.split()[:2])
    key_to_idx[k].append(i)   # index into headers_pos / headers_name

def entry_text(header_idx: int) -> str:
    """Extract raw corpus text for the entry at headers_pos[header_idx]."""
    start = headers_pos[header_idx]
    # End = start of next header, or end of corpus
    if header_idx + 1 < len(headers_pos):
        end = headers_pos[header_idx + 1]
    else:
        end = len(corpus)
    return corpus[start:end].strip()

# ── Load DB ───────────────────────────────────────────────────────────────────
print("Loading DB …", flush=True)
with DB_PATH.open(encoding="utf-8") as f:
    db = json.load(f)

entries_sorted = sorted(db.values(), key=lambda e: e.get("_entry_idx", 0))
print(f"  {len(entries_sorted):,} entries", flush=True)

# ── Match entries ─────────────────────────────────────────────────────────────
print("Matching …", flush=True)
matched    = 0
not_found  = 0
search_idx = 0    # we advance this so we always move FORWARD through the corpus

for i, entry in enumerate(entries_sorted):
    name_ar = entry.get("name_ar", "")
    if not name_ar:
        not_found += 1
        continue

    k = key2(name_ar)
    idxs = key_to_idx.get(k, [])  # list of header array indices

    # Among candidates, pick the first one whose position index >= search_idx
    chosen = None
    for hidx in idxs:
        if hidx >= search_idx:
            chosen = hidx
            break

    if chosen is None:
        # Nothing found forward — fallback: try first occurrence anywhere
        if idxs:
            chosen = idxs[0]
        else:
            # Try with 3-word key
            k3 = key3(name_ar)
            idxs3 = key_to_idx.get(k3, [])
            for hidx in idxs3:
                if hidx >= search_idx:
                    chosen = hidx
                    break
            if chosen is None and idxs3:
                chosen = idxs3[0]

    if chosen is None:
        not_found += 1
        if not_found <= 20:
            safe = name_ar[:50].encode('ascii', errors='replace').decode()
            print(f"  NOT FOUND: idx={entry['_entry_idx']} | {safe}", flush=True)
        continue

    raw_text = entry_text(chosen)

    if not STATS_ONLY:
        entry["_raw_full"] = raw_text

    matched    += 1
    search_idx  = chosen + 1   # next match must come after this header

    if (i + 1) % 2000 == 0:
        pct = (i + 1) / len(entries_sorted) * 100
        print(f"  {i+1:5,}/{len(entries_sorted):,} ({pct:.0f}%)  "
              f"matched={matched:,}  not_found={not_found:,}", flush=True)

print(f"\nResults: {matched:,} matched | {not_found:,} not found | {len(entries_sorted):,} total")
print(f"Match rate: {matched / len(entries_sorted) * 100:.1f}%")

if STATS_ONLY:
    print("\n[dry-run — DB not written]")
    sys.exit(0)

# ── Save ──────────────────────────────────────────────────────────────────────
print(f"\nSaving {DB_PATH} ...", flush=True)
with DB_PATH.open("w", encoding="utf-8") as f:
    json.dump(db, f, ensure_ascii=False, indent=2)
size_mb = DB_PATH.stat().st_size / 1_000_000
print(f"Done.  {size_mb:.1f} MB   (backup: rijal_database_backup_20260415.json)", flush=True)

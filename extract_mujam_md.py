#!/usr/bin/env python3
"""
Extract narrator entries from the full Mu'jam Rijal al-Hadith (24 volumes).

Input:  books_md/5176.md  …  books_md/5199.md
Output: rijal_entries_mujam.json  (same {idx, n1, n2, n3, text} schema as
        rijal_entries.json so rijal_builder.py accepts it unchanged)

Entry format in the md files:
    NNNN - اسم الراوي :          ← entry header (line ends with " :")
    [content lines, aliases, footnotes ...]
    [blank lines / page markers / --- separators]
    NEXT_ENTRY or end-of-section

Volume 1 (5176.md) has a long scholarly intro before narrator entries;
all other volumes start with entries almost immediately.
"""

import json
import os
import re
import sys
from pathlib import Path

BOOKS_DIR   = Path(__file__).parent / "books_md"
OUTPUT_FILE = Path(__file__).parent / "rijal_entries_mujam.json"
# Existing name index lets us back-fill n1/n2 edition numbers
NAME_INDEX_FILE = Path(__file__).parent / "rijal_name_index.json"
N3_INDEX_FILE   = Path(__file__).parent / "rijal_n3_index.json"

if sys.platform == "win32":
    import io
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

# ── Arabic-Indic helpers ───────────────────────────────────────────────────────
_TO_AR = {"0":"٠","1":"١","2":"٢","3":"٣","4":"٤",
          "5":"٥","6":"٦","7":"٧","8":"٨","9":"٩"}

def to_arabic_indic(n: int) -> str:
    return "".join(_TO_AR[c] for c in str(n))


# ── Patterns ──────────────────────────────────────────────────────────────────

# Narrator entry header:  "NNNN - NAME :"   (line ends with space-colon)
ENTRY_RE = re.compile(r"^(\d+) - (.+) :$")

# Section header that marks the start of narrator entries in vol 1
# e.g. "( أ ) - باب الألف"
SECTION_HEADER_RE = re.compile(r"^\(\s*[\u0600-\u06ff]\s*\)")

# Standalone content lines that signal the END of the current entry's content —
# scholarly/appendix sections appended after the last entry in some volumes.
# When seen as accumulated content, flush the current entry and stop accumulating.
ENTRY_CONTENT_STOP_PHRASES = [
    "تفصيل طبقات الرواة",   # tabaqah analysis section (appended after last entry in vol 1)
    "رموز الكتاب",            # abbreviation legend (front matter of next volume)
    "فهرست كتب أجزاء الكافي", # al-Kafi section index (front matter)
]

# Lines that indicate a non-narrator numbered item (end-of-volume index etc.)
NON_NARRATOR_PHRASES = [
    "كتب الجزء",
    "الجزء الأول", "الجزء الثاني", "الجزء الثالث",
    "الجزء الرابع", "الجزء الخامس", "الجزء السادس",
    "الجزء السابع", "الجزء الثامن",
    "الروضة",
]

# Patterns that indicate a hadith citation in the entry header position,
# not a real narrator entry.  These appear in al-Kashshi intro sections at
# the start of each volume and look like:
#   5 - " محمد بن مسعود ، قال : حدثني ... :
#   3 - حدثني محمد بن قولويه ... :
#   1 - قال أبو عمرو الكشي : سألت ... :
HADITH_CITATION_RE = re.compile(
    r'^["\u201c\u00ab]|'   # starts with any quote character
    r'^حدثني|^حدثنا|'      # starts with narration verb
    r'^أخبرني|^أخبرنا|'    # starts with narration verb
    r'\bقال\b',             # contains the word "said" (verb, never in a name)
)

# Lines to strip from entry content
PAGE_MARKER_RE = re.compile(r"^## Page ")


# ── Extraction ────────────────────────────────────────────────────────────────

def is_non_narrator(name_part: str) -> bool:
    return any(phrase in name_part for phrase in NON_NARRATOR_PHRASES)

def is_hadith_citation(name_part: str) -> bool:
    """True when the 'name' field of a matched entry is actually a hadith chain,
    not a narrator name — common in al-Kashshi intro sections at volume starts."""
    return bool(HADITH_CITATION_RE.search(name_part.strip()))


def clean_content_line(line: str) -> str:
    """Return cleaned line for inclusion in entry text, or '' to skip."""
    stripped = line.strip()
    if not stripped:
        return ""
    if PAGE_MARKER_RE.match(stripped):
        return ""
    if stripped == "---":
        return ""
    return stripped


def extract_volume(filepath: Path, vol_label: str) -> list[dict]:
    """
    Extract all narrator entries from one .md volume file.
    Returns list of {_n3_int, n3, text}.
    """
    with open(filepath, encoding="utf-8") as f:
        lines = f.readlines()

    # Vol 1 has a long intro; skip to the section header
    entry_start_line = 0
    if "5176" in filepath.name:
        for i, line in enumerate(lines):
            if SECTION_HEADER_RE.match(line.strip()):
                entry_start_line = i
                break

    entries: list[dict] = []
    current_n3_int: int | None = None
    current_text_lines: list[str] = []
    skip_entry: bool = False   # True while inside a hadith-citation false-entry

    def flush():
        if current_n3_int is None:
            return
        # Join non-empty content lines; collapse runs of blanks
        text = " ".join(l for l in current_text_lines if l).strip()
        if text:
            entries.append({
                "_n3_int": current_n3_int,
                "n3":      to_arabic_indic(current_n3_int),
                "text":    text,
            })

    for i, raw_line in enumerate(lines):
        if i < entry_start_line:
            continue

        stripped = raw_line.rstrip().strip()
        m = ENTRY_RE.match(stripped)

        if m:
            n3_int    = int(m.group(1))
            name_part = m.group(2)

            if is_non_narrator(name_part):
                # This signals end-of-section index; flush and stop
                flush()
                break

            if is_hadith_citation(name_part):
                # Al-Kashshi intro citation masquerading as a numbered entry —
                # skip it and its content lines without ending the volume
                skip_entry = True
                continue

            skip_entry = False
            flush()
            current_n3_int    = n3_int
            current_text_lines = [stripped]   # header line is part of text

        elif not skip_entry and current_n3_int is not None:
            cleaned = clean_content_line(raw_line)
            if cleaned:
                # Check if this line starts a scholarly appendix section
                if any(phrase in cleaned for phrase in ENTRY_CONTENT_STOP_PHRASES):
                    flush()
                    current_n3_int = None
                    current_text_lines = []
                else:
                    current_text_lines.append(cleaned)

    flush()
    return entries


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # ── Collect all volume files in sorted order ─────────────────────────────
    vol_files = sorted(
        p for p in BOOKS_DIR.iterdir()
        if p.suffix == ".md"
    )
    if not vol_files:
        print(f"No .md files found in {BOOKS_DIR}")
        sys.exit(1)

    print(f"Found {len(vol_files)} volume files in {BOOKS_DIR}")

    # ── Extract entries from every volume ─────────────────────────────────────
    all_entries: list[dict] = []
    for vf in vol_files:
        vol_entries = extract_volume(vf, vf.stem)
        print(f"  {vf.name}: {len(vol_entries):4d} entries")
        all_entries.extend(vol_entries)

    print(f"\nTotal raw entries: {len(all_entries)}")

    # ── De-duplicate by n3 (keep LAST occurrence — later volumes may have
    #    richer text for the same n3 due to re-printing corrections) ──────────
    by_n3: dict[int, dict] = {}
    for e in all_entries:
        by_n3[e["_n3_int"]] = e
    all_entries = list(by_n3.values())
    print(f"After de-dup by n3: {len(all_entries)}")

    # ── Sort by n3 so idx is meaningful ───────────────────────────────────────
    all_entries.sort(key=lambda e: e["_n3_int"])

    # ── Attempt to back-fill n1/n2 from existing name index ──────────────────
    n3_to_editions: dict[str, tuple[str | None, str | None]] = {}
    if N3_INDEX_FILE.exists() and NAME_INDEX_FILE.exists():
        with open(N3_INDEX_FILE, encoding="utf-8") as f:
            n3_index = json.load(f)
        with open(NAME_INDEX_FILE, encoding="utf-8") as f:
            name_index = json.load(f)
        for n3_ar, idx_list in n3_index.items():
            if idx_list:
                info = name_index.get(idx_list[0])
                if info:
                    n3_to_editions[n3_ar] = (info.get("n1"), info.get("n2"))
        print(f"Loaded edition cross-ref for {len(n3_to_editions):,} n3 values")
    else:
        print("Name index not found — n1/n2 will be null")

    # ── Assign sequential idx and final fields ────────────────────────────────
    output: list[dict] = []
    for idx, e in enumerate(all_entries):
        n3_ar  = e["n3"]
        n1, n2 = n3_to_editions.get(n3_ar, (None, None))
        output.append({
            "idx":  idx,
            "n1":   n1,
            "n2":   n2,
            "n3":   n3_ar,
            "text": e["text"],
        })

    # ── Write output ──────────────────────────────────────────────────────────
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nWrote {len(output):,} entries → {OUTPUT_FILE}")

    # ── Quick stats ───────────────────────────────────────────────────────────
    avg_len = sum(len(e["text"]) for e in output) / len(output) if output else 0
    max_len = max(len(e["text"]) for e in output) if output else 0
    print(f"Avg entry length: {avg_len:.0f} chars  |  Max: {max_len:,} chars")
    long_entries = sum(1 for e in output if len(e["text"]) > 4000)
    print(f"Entries > 4000 chars: {long_entries} ({long_entries/len(output)*100:.1f}%)")


if __name__ == "__main__":
    main()

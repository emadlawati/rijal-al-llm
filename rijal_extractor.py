#!/usr/bin/env python3
"""
rijal_extractor.py  —  Path 1: Deterministic field extraction from _raw_full
===========================================================================
Extracts fields from the full Mu'jam Rijal text (_raw_full) that can be
determined with high accuracy from regex patterns alone, without an LLM.

Fields extracted:
  has_book          — له كتاب / له أصل
  hadith_count      — روى N رواية
  tariq_status      — طريق صحيح / ضعيف
  books             — hadith collection names mentioned
  grandfather       — third level of nasab chain in entry header
  narrates_from_narrators  — روى عن X (non-Imam)
  narrated_from_by         — روى عنه X

Merge rules:
  • Boolean/int/string fields: only written if currently null/False/[]
  • List fields: merged (union), never cleared
  • status / name_ar / name_en / _raw* : never touched

Run:
  python rijal_extractor.py --dry-run   # show counts, don't write
  python rijal_extractor.py             # run and save
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict

DB_PATH   = Path("rijal_database.json")
DRY_RUN   = "--dry-run" in sys.argv

# ─── Arabic constants ─────────────────────────────────────────────────────────

# Imam titles / markers — if an extracted name contains these it's an Imam
IMAM_MARKERS_RE = re.compile(
    r'\(ع\)|عليه السلام|عليه الصلاة والسلام|صلى الله عليه وآله|'
    r'\(ص\)|صلوات الله عليه|عليهما السلام|رضوان الله عليه'
)
IMAM_TITLES = {
    'أبي عبد الله', 'أبي جعفر', 'أبي الحسن', 'أبي محمد', 'أبي عبدالله',
    'الصادق', 'الباقر', 'الكاظم', 'الرضا', 'الجواد', 'الهادي', 'العسكري',
    'المهدي', 'القائم', 'صاحب الأمر', 'أمير المؤمنين', 'الحسن', 'الحسين',
    'السجاد', 'النبي', 'رسول الله',
}

# Book names that may appear in روى عن context as citation sources
CITATION_BOOKS = {
    'الكافي', 'التهذيب', 'الاستبصار', 'الفقيه', 'الكشي', 'النجاشي',
    'الفهرست', 'الرجال', 'العدة', 'الخلاف', 'المبسوط', 'النهاية',
    'المقنعة', 'الغيبة', 'الإرشاد', 'بصائر الدرجات', 'أمالي', 'الخصال',
    'علل الشرائع', 'معاني الأخبار', 'كامل الزيارات', 'تفسير القمي',
    'الوافي', 'الوسائل', 'البحار',
}

# Hadith collections to detect in the text (for the `books` field)
BOOK_DETECT = [
    'الكافي', 'التهذيب', 'الاستبصار', 'من لا يحضره الفقيه',
    'كامل الزيارات', 'تفسير القمي',
]
BOOK_CANONICAL = {
    'الكافي':                    'الكافي',
    'التهذيب':                   'التهذيب',
    'الاستبصار':                 'الاستبصار',
    'من لا يحضره الفقيه':       'الفقيه',
    'كامل الزيارات':             'كامل الزيارات',
    'تفسير القمي':               'تفسير القمي',
}

# ─── Normalisation helpers ────────────────────────────────────────────────────

_DIAC = re.compile(r'[\u064b-\u065f\u0670]')

def norm(t: str) -> str:
    return _DIAC.sub('', t).replace('ـ', '').strip()

def clean_extracted_name(raw: str) -> str:
    """
    Clean up a name extracted from a روى عن / روى عنه segment.
    Strips: trailing cross-ref numbers ( N ), extra punctuation, honorifics.
    """
    name = raw.strip()
    # Remove cross-ref numbers: (123) or ( ١٢٣ )
    name = re.sub(r'\(\s*[\d٠-٩]+\s*\)', '', name)
    # Remove trailing honorifics and punctuation
    name = re.sub(r'[\.\،\,\;\:\!\?]+$', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def is_imam(name: str) -> bool:
    """Return True if the extracted name is an Imam."""
    if IMAM_MARKERS_RE.search(name):
        return True
    n = norm(name)
    for title in IMAM_TITLES:
        if title in n:
            # Extra guard: if name has بن + word + بن, likely a regular narrator
            if len(re.findall(r'\bبن\b', n)) >= 2:
                continue
            return True
    return False

def is_book_citation(name: str) -> bool:
    """Return True if the extracted name looks like a book/source title, not a person."""
    n = norm(name).split()[0] if name else ''
    return n in CITATION_BOOKS or name.strip() in CITATION_BOOKS

def is_valid_narrator_name(name: str) -> bool:
    """
    Minimal sanity check: a narrator name should have at least one Arabic word
    of ≥ 3 chars and NOT be just a number or short particle.
    """
    name = name.strip()
    if not name or len(name) < 3:
        return False
    arabic_words = re.findall(r'[\u0600-\u06ff]{3,}', name)
    return len(arabic_words) >= 1

# ─── Extraction functions ─────────────────────────────────────────────────────

# 1. has_book
HAS_BOOK_RE = re.compile(r'(?:له|لها|لهم)\s+(?:كتاب|أصل|أصول|مصنفات|تصانيف|مؤلفات)')

def extract_has_book(text: str) -> bool:
    return bool(HAS_BOOK_RE.search(text))


# 2. hadith_count
HADITH_COUNT_RE = re.compile(r'روى\s+([٠-٩\d]+)\s+رواية')

def extract_hadith_count(text: str) -> int | None:
    m = HADITH_COUNT_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    _AR = {'٠':0,'١':1,'٢':2,'٣':3,'٤':4,'٥':5,'٦':6,'٧':7,'٨':8,'٩':9}
    val = 0
    for c in raw:
        val = val * 10 + (_AR.get(c, int(c) if c.isdigit() else 0))
    return val if val > 0 else None


# 3. tariq_status
TARIQ_SAHIH_RE = re.compile(r'طريق[^.،\n]{0,150}صحيح')
TARIQ_DAIF_RE  = re.compile(r'طريق[^.،\n]{0,150}ضعيف')

def extract_tariq_status(text: str) -> str | None:
    if TARIQ_SAHIH_RE.search(text):
        return 'sahih'
    if TARIQ_DAIF_RE.search(text):
        return 'daif'
    return None


# 4. books
def extract_books(text: str) -> list[str]:
    found = []
    for bname, canonical in BOOK_CANONICAL.items():
        if bname in text and canonical not in found:
            found.append(canonical)
    return found


# 5. grandfather — from nasab chain in entry header line
#    Header: "N - Name بن Father بن Grandfather ..."
NASAB_RE = re.compile(
    r'(?:بن|ابن)\s+'
    r'([\u0600-\u06ff]+(?:\s+[\u0600-\u06ff]+){0,3}?)\s+'
    r'(?:بن|ابن)\s+'
    r'([\u0600-\u06ff]+(?:\s+[\u0600-\u06ff]+){0,3}?)'
    r'(?=\s*(?:بن|ابن|:|\.|،|$))'
)

def extract_grandfather(raw_full: str, known_father: str | None) -> str | None:
    """
    Try to extract grandfather from the entry header line.
    Validates that the first captured group matches known_father if available.
    """
    header_line = raw_full.split('\n')[0]

    best_grandfather = None
    for m in NASAB_RE.finditer(header_line):
        father_candidate    = m.group(1).strip()
        grandfather_candidate = m.group(2).strip()

        if known_father:
            # Validate: father_candidate should match the DB's father field
            if (norm(known_father) in norm(father_candidate) or
                    norm(father_candidate) in norm(known_father)):
                return grandfather_candidate
        else:
            # No known father: take the first plausible match
            if best_grandfather is None:
                best_grandfather = grandfather_candidate

    return best_grandfather


# 6. narrates_from_narrators  — روى عن X (non-Imam)
# 7. narrated_from_by         — روى عنه X

# Pattern for روى عن segments (including وروى عن, وعن continuations)
# Capture everything up to the next chain link (عن), citation (في + book), or sentence end
_ROW_AN_RE = re.compile(
    r'(?:(?:و)?روى\s+عن|حدّث\s+عن)\s+'
    r'([^.،\n\(]+?)'
    r'(?=\s*(?:عن\s|في\s|وروى|الكافي|التهذيب|الاستبصار|الفقيه|\(|\.|،|\n|$))',
    re.MULTILINE | re.UNICODE
)
# Continuation "وعن X" after a روى عن sentence
_W_AN_RE = re.compile(
    r'وعن\s+'
    r'([^.،\n\(]+?)'
    r'(?=\s*(?:عن\s|في\s|وروى|\(|\.|،|\n|$))',
    re.MULTILINE | re.UNICODE
)
# روى عنه X
_ROW_ANHU_RE = re.compile(
    r'(?:(?:و)?روى\s+عنه|حدّث\s+عنه|حدّث\s+عنه)\s+'
    r'([^.،\n\(]+?)'
    r'(?=\s*(?:في\s|وروى|\(|\.|،|\n|$))',
    re.MULTILINE | re.UNICODE
)

def _extract_from_pattern(pattern, text: str) -> list[str]:
    names = []
    for m in pattern.finditer(text):
        raw = m.group(1)
        # May contain multiple names joined by و (and)
        # Split by وعن or و at word boundary
        parts = re.split(r'\s+و(?:عن\s+)?', raw)
        for part in parts:
            name = clean_extracted_name(part)
            if not is_valid_narrator_name(name):
                continue
            names.append(name)
    return names

def extract_narration_connections(text: str) -> tuple[list[str], list[str], list[str]]:
    """
    Returns (narrates_from_narrators, narrates_from_imams_new, narrated_from_by).
    Imams detected in روى عن contexts are separated out.
    """
    raw_from    = _extract_from_pattern(_ROW_AN_RE,   text)
    raw_from   += _extract_from_pattern(_W_AN_RE,     text)
    raw_from_by = _extract_from_pattern(_ROW_ANHU_RE, text)

    narrators, imams = [], []
    for name in raw_from:
        if is_imam(name):
            # Normalise Imam name (strip (ع) suffix for clean storage)
            clean = re.sub(r'\s*\(ع\)\s*$', '', name).strip()
            imams.append(clean)
        elif is_book_citation(name):
            pass  # citation source, not a personal narrator chain
        else:
            narrators.append(name)

    narrated_by = [n for n in raw_from_by
                   if not is_imam(n) and not is_book_citation(n)]

    return narrators, imams, narrated_by


# ─── Merge helpers ────────────────────────────────────────────────────────────

def merge_list(existing: list, new_items: list) -> tuple[list, int]:
    """Add items not already present. Returns (merged_list, count_added)."""
    existing_norm = {norm(x) for x in existing}
    added = 0
    result = list(existing)
    for item in new_items:
        if norm(item) not in existing_norm:
            result.append(item)
            existing_norm.add(norm(item))
            added += 1
    return result, added


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading rijal_database.json …")
    with DB_PATH.open(encoding='utf-8') as f:
        db = json.load(f)
    print(f"  {len(db):,} entries")

    stats = defaultdict(int)

    for key, entry in db.items():
        raw_full = entry.get('_raw_full', '')
        if not raw_full:
            stats['no_raw_full'] += 1
            continue

        changed = False

        # ── 1. has_book ──────────────────────────────────────────────────
        if not entry.get('has_book', False):
            val = extract_has_book(raw_full)
            if val:
                if not DRY_RUN:
                    entry['has_book'] = True
                stats['has_book'] += 1
                changed = True

        # ── 2. hadith_count ───────────────────────────────────────────────
        if entry.get('hadith_count') is None:
            val = extract_hadith_count(raw_full)
            if val is not None:
                if not DRY_RUN:
                    entry['hadith_count'] = val
                stats['hadith_count'] += 1
                changed = True

        # ── 3. tariq_status ───────────────────────────────────────────────
        if entry.get('tariq_status') is None:
            val = extract_tariq_status(raw_full)
            if val:
                if not DRY_RUN:
                    entry['tariq_status'] = val
                stats['tariq_status'] += 1
                changed = True

        # ── 4. books ──────────────────────────────────────────────────────
        existing_books = entry.get('books') or []
        new_books = extract_books(raw_full)
        merged_books, n_added_books = merge_list(existing_books, new_books)
        if n_added_books > 0:
            if not DRY_RUN:
                entry['books'] = merged_books
            stats['books'] += n_added_books
            changed = True

        # ── 5. grandfather ────────────────────────────────────────────────
        if entry.get('grandfather') is None:
            val = extract_grandfather(raw_full, entry.get('father'))
            if val and len(val) >= 3:
                if not DRY_RUN:
                    entry['grandfather'] = val
                stats['grandfather'] += 1
                changed = True

        # ── 6 & 7. narration connections ──────────────────────────────────
        narrators, new_imams, narrated_by = extract_narration_connections(raw_full)

        # narrates_from_narrators
        existing_nfn = entry.get('narrates_from_narrators') or []
        merged_nfn, n_nfn = merge_list(existing_nfn, narrators)
        if n_nfn > 0:
            if not DRY_RUN:
                entry['narrates_from_narrators'] = merged_nfn
            stats['narrates_from_narrators'] += n_nfn
            changed = True

        # narrates_from_imams (newly detected)
        existing_nfi = entry.get('narrates_from_imams') or []
        merged_nfi, n_nfi = merge_list(existing_nfi, new_imams)
        if n_nfi > 0:
            if not DRY_RUN:
                entry['narrates_from_imams'] = merged_nfi
            stats['narrates_from_imams'] += n_nfi
            changed = True

        # narrated_from_by
        existing_nfb = entry.get('narrated_from_by') or []
        merged_nfb, n_nfb = merge_list(existing_nfb, narrated_by)
        if n_nfb > 0:
            if not DRY_RUN:
                entry['narrated_from_by'] = merged_nfb
            stats['narrated_from_by'] += n_nfb
            changed = True

        if changed:
            stats['entries_changed'] += 1

    # ── Report ────────────────────────────────────────────────────────────────
    print()
    print("=== Extraction Results ===")
    print(f"  Entries with _raw_full:       {len(db) - stats['no_raw_full']:,}")
    print(f"  Entries changed:              {stats['entries_changed']:,}")
    print()
    print(f"  has_book newly set:           {stats['has_book']:,}")
    print(f"  hadith_count newly set:       {stats['hadith_count']:,}")
    print(f"  tariq_status newly set:       {stats['tariq_status']:,}")
    print(f"  book citations added:         {stats['books']:,}")
    print(f"  grandfather newly set:        {stats['grandfather']:,}")
    print(f"  narrates_from_narrators added:{stats['narrates_from_narrators']:,}")
    print(f"  narrates_from_imams added:    {stats['narrates_from_imams']:,}")
    print(f"  narrated_from_by added:       {stats['narrated_from_by']:,}")

    if DRY_RUN:
        print("\n[dry-run — database not written]")
        return

    print(f"\nSaving {DB_PATH} …")
    with DB_PATH.open('w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    size_mb = DB_PATH.stat().st_size / 1_000_000
    print(f"Done.  {size_mb:.1f} MB")


if __name__ == '__main__':
    main()

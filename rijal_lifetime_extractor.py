#!/usr/bin/env python3
"""
Rijal Lifetime Extractor
========================

Extracts hijri birth_year and death_year for narrators in the rijal database
by mining the free-text `_raw_full` and `_raw` fields, plus inheriting
window estimates from `tabaqah` for narrators without explicit dates.

This is the data foundation for Phase 3 (transmission feasibility): once every
narrator has a (windowed) lifetime, `transmission_validator.py` can answer
"could narrator A have actually met narrator B?".

Sources, in order of precedence:
  1. Explicit death-year regex from biographical text          source = "raw_explicit"
  2. Age-at-death + death-year → birth year                    source = "age_derived"
  3. Tabaqah → AH window inheritance from BURUJIRDI_AH_RANGES  source = "tabaqah_window"

Schema added to each rijal entry:
    birth_year_hijri:        int | null
    death_year_hijri:        int | null
    birth_year_window:       [low, high] | null
    death_year_window:       [low, high] | null
    birth_year_source:       "raw_explicit" | "age_derived" | "tabaqah_window" | null
    death_year_source:       "raw_explicit" | "tabaqah_window" | null
    lifetime_confidence:     "certain" | "likely" | "windowed" | "unknown"
    lifetime_evidence:       "<exact quote from _raw_full or rule applied>"

CLI:
    python rijal_lifetime_extractor.py --stats           # coverage report
    python rijal_lifetime_extractor.py --dry-run         # extract but don't save
    python rijal_lifetime_extractor.py --apply           # extract and persist
    python rijal_lifetime_extractor.py --rebuild --apply # clear existing then re-extract
"""

import json
import re
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rijal_resolver import DATABASE_FILE
from tabaqah_inference import BURUJIRDI_AH_RANGES

SCRIPT_DIR = Path(__file__).resolve().parent

# ─── Numeral conversion ──────────────────────────────────────────────────────

_AR_DIGIT_MAP: Dict[str, str] = {}
for i in range(10):
    _AR_DIGIT_MAP[chr(0x0660 + i)] = str(i)   # Arabic-Indic
    _AR_DIGIT_MAP[chr(0x06F0 + i)] = str(i)   # Extended (Persian)


def ar_to_int(s: str) -> Optional[int]:
    """Convert any mixture of Arabic/Persian/Latin digits to int. Returns None on failure."""
    if not s:
        return None
    cleaned = ''.join(_AR_DIGIT_MAP.get(c, c) for c in s)
    cleaned = re.sub(r'[^0-9]', '', cleaned)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


# ─── Reasonable hijri-year filter ─────────────────────────────────────────────

# Narrators in this corpus span ~10–500 AH. Anything outside is OCR noise (e.g.
# the Mufid PDF often has page numbers like "208" embedded inside dates as
# "(۸۰۲هـ)" — Arabic-Persian digit confusion produces 802 from 208).

MIN_PLAUSIBLE_AH = 1
MAX_PLAUSIBLE_AH = 500


def _plausible(year: Optional[int]) -> bool:
    return year is not None and MIN_PLAUSIBLE_AH <= year <= MAX_PLAUSIBLE_AH


# ─── Death-year patterns ──────────────────────────────────────────────────────

# Each pattern captures one digit group. The patterns are tried in order and
# the FIRST match for a narrator wins (don't aggregate; conflicting dates are
# more likely OCR artifacts than genuine multiple-source disagreement).

_NUM = r'[0-9٠-٩۰-۹]+'

# "توفي [في] سنة 220 هـ" / "توفي سنة 220" / "توفّي سنة (220هـ)"
_DEATH_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(
        r'(?:تُوفِّ?ي|توفّ?ي|توفي|تُوُفِّي)\s+(?:في\s+)?سنة\s*\(?\s*('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'تُوفِّي سنة'),

    # "مات [في] سنة 220" / "مات سنة (220هـ)"
    (re.compile(
        r'(?<![؀-ۿ])مَ?اتَ?\s+(?:في\s+)?سنة\s*\(?\s*('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'مات سنة'),

    # "وفاته سنة 220" / "وفاته في سنة 220"
    (re.compile(
        r'وفاتُ?هُ?\s+(?:في\s+)?سنة\s*\(?\s*('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'وفاته سنة'),

    # "المتوفى سنة 220" / "المتوفّى (220هـ)" — common in citations of teachers
    (re.compile(
        r'الم[ُتَ]وَ?فَّ?ى\s*\(?\s*(?:سنة\s*)?('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'المتوفى سنة'),

    # "قُتل سنة 61" — martyrdom (used for Karbala/political deaths)
    (re.compile(
        r'(?<![؀-ۿ])(?:قُ?تِلَ|قتل)\s+(?:في\s+)?سنة\s*\(?\s*('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'قتل سنة'),

    # "استشهد سنة 61"
    (re.compile(
        r'استُش?هِدَ?\s+(?:في\s+)?سنة\s*\(?\s*('
        + _NUM + r')\s*(?:هـ|هـ\.|ه)?\s*\)?'
    ), 'استشهد سنة'),
]


def extract_death_year(text: str) -> Optional[Tuple[int, str, str]]:
    """Find the narrator's own death year in the text.

    Returns (year, evidence_quote, rule_label) or None.

    Heuristic to avoid grabbing a TEACHER's death year that's mentioned in
    passing: only accept matches in the FIRST 1500 characters of the entry.
    Biographical entries put the narrator's own dates near the beginning;
    teacher/student dates typically appear later in transmission discussion.
    Plus: the *first* matched pattern wins.
    """
    if not text:
        return None

    head = text[:1500]
    for pat, label in _DEATH_PATTERNS:
        m = pat.search(head)
        if not m:
            continue
        year = ar_to_int(m.group(1))
        if not _plausible(year):
            continue
        # Quote ~80 chars of context for provenance
        start = max(0, m.start() - 20)
        end = min(len(head), m.end() + 20)
        quote = head[start:end].replace('\n', ' ').strip()
        return year, quote, label
    return None


# ─── Age-at-death patterns ────────────────────────────────────────────────────

# These yield a birth_year derivation when combined with a known death_year:
#     birth = death − age
# The patterns capture an integer age in years.

_AGE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    # "وعمره 88 سنة" / "وهو ابن 88 سنة"
    (re.compile(
        r'(?:وَ?عُمْ?رُ?هُ?|وَ?هُو(?:َ)?\s+ابنُ?)\s*\(?\s*('
        + _NUM + r')\s*\)?\s+سنة'
    ), 'عمره X سنة'),

    # "بلغ 90 سنة" / "بلغ من العمر 90 سنة"
    (re.compile(
        r'بَلَغَ\s+(?:من\s+العمر\s+)?\(?\s*('
        + _NUM + r')\s*\)?\s+سنة'
    ), 'بلغ X سنة'),

    # "وله 88 سنة" — common phrasing
    (re.compile(
        r'(?<![؀-ۿ])وَ?لَهُ\s+\(?\s*('
        + _NUM + r')\s*\)?\s+سنة'
    ), 'له X سنة'),
]

# Narrative age phrases without a number — too vague to derive birth_year, but
# valuable as a "long-lived" marker. We don't materialize a birth_year from these
# but record that lifetime_evidence saw such a phrase.
_VAGUE_OLD_AGE = re.compile(
    r'(?:نَ?يِّفاً?\s+وَ?(?:تسعين|ثمانين|سبعين)|عُمِّرَ\s+(?:طويلاً?|كثيراً?)|قَ?ضَى\s+عُمراً?\s+طويلاً?)'
)


def extract_age_at_death(text: str) -> Optional[Tuple[int, str, str]]:
    """Find the narrator's age at death. Returns (age, quote, label) or None."""
    if not text:
        return None
    head = text[:2000]
    for pat, label in _AGE_PATTERNS:
        m = pat.search(head)
        if not m:
            continue
        age = ar_to_int(m.group(1))
        # Sanity: ages 30–120 are plausible. Below 30 is rarely a death-age
        # and over 120 is almost certainly an OCR error.
        if age is None or not (30 <= age <= 120):
            continue
        start = max(0, m.start() - 20)
        end = min(len(head), m.end() + 20)
        quote = head[start:end].replace('\n', ' ').strip()
        return age, quote, label
    return None


# ─── Per-entry extraction ─────────────────────────────────────────────────────

def extract_lifetime_for_entry(entry: dict) -> Dict[str, object]:
    """Compute the lifetime fields for a single rijal entry.

    Returns a dict with the new schema fields. Caller decides whether to write.
    Does NOT mutate the entry.
    """
    raw = entry.get('_raw_full') or entry.get('_raw') or ''
    out: Dict[str, object] = {
        'birth_year_hijri':    None,
        'death_year_hijri':    None,
        'birth_year_window':   None,
        'death_year_window':   None,
        'birth_year_source':   None,
        'death_year_source':   None,
        'lifetime_confidence': 'unknown',
        'lifetime_evidence':   None,
    }

    death_info = extract_death_year(raw)
    age_info = extract_age_at_death(raw)
    evidence_parts: List[str] = []

    if death_info:
        death_year, dquote, dlabel = death_info
        out['death_year_hijri'] = death_year
        out['death_year_source'] = 'raw_explicit'
        evidence_parts.append(f'{dlabel}: "{dquote}"')

        if age_info:
            age, aquote, alabel = age_info
            birth_year = death_year - age
            if _plausible(birth_year):
                out['birth_year_hijri'] = birth_year
                out['birth_year_source'] = 'age_derived'
                evidence_parts.append(f'{alabel}: "{aquote}"')

        out['lifetime_confidence'] = 'certain' if age_info else 'likely'

    # Ṭabaqah window fallback — applied if no explicit data, OR to fill
    # whichever of (birth, death) is still missing.
    tabaqah = entry.get('tabaqah')
    if tabaqah is not None:
        try:
            t = int(tabaqah)
        except (TypeError, ValueError):
            t = None

        if t is not None and t in BURUJIRDI_AH_RANGES:
            t_low, t_high = BURUJIRDI_AH_RANGES[t]
            # Birth window: a narrator active in tabaqah-window [a,b] was likely
            # born ~30–60 years before being active. Conservative: born up to
            # 60 years before window-start, up to 10 years before window-end.
            birth_window = (max(MIN_PLAUSIBLE_AH, t_low - 60), max(MIN_PLAUSIBLE_AH, t_high - 10))
            # Death window: narrator can plausibly have died from window-start
            # up to window-end + 30 (lived past the active period).
            death_window = (t_low, min(MAX_PLAUSIBLE_AH, t_high + 30))

            if out['birth_year_hijri'] is None:
                out['birth_year_window'] = list(birth_window)
                if out['birth_year_source'] is None:
                    out['birth_year_source'] = 'tabaqah_window'
            if out['death_year_hijri'] is None:
                out['death_year_window'] = list(death_window)
                if out['death_year_source'] is None:
                    out['death_year_source'] = 'tabaqah_window'

            if out['lifetime_confidence'] == 'unknown':
                out['lifetime_confidence'] = 'windowed'

    # Vague old-age marker (informational; doesn't change confidence)
    if not death_info and _VAGUE_OLD_AGE.search(raw[:2000]):
        evidence_parts.append('عُمّر طويلاً')

    if evidence_parts:
        out['lifetime_evidence'] = ' | '.join(evidence_parts)

    return out


# ─── DB-level driver ──────────────────────────────────────────────────────────

def apply_lifetime_to_db(db: Dict[str, dict], dry_run: bool = False) -> Dict[str, int]:
    """Run extraction on every entry. Returns counts by outcome."""
    counts: Dict[str, int] = defaultdict(int)

    for entry in db.values():
        result = extract_lifetime_for_entry(entry)

        # Categorize for stats
        if result['death_year_hijri'] is not None and result['birth_year_hijri'] is not None:
            counts['both_explicit'] += 1
        elif result['death_year_hijri'] is not None:
            counts['death_only'] += 1
        elif result['birth_year_window'] is not None or result['death_year_window'] is not None:
            counts['windowed_only'] += 1
        else:
            counts['unknown'] += 1

        if not dry_run:
            for k, v in result.items():
                entry[k] = v

    return dict(counts)


def clear_lifetime_fields(db: Dict[str, dict]) -> int:
    """Remove all lifetime fields. Used by --rebuild before re-extraction."""
    fields = (
        'birth_year_hijri', 'death_year_hijri',
        'birth_year_window', 'death_year_window',
        'birth_year_source', 'death_year_source',
        'lifetime_confidence', 'lifetime_evidence',
    )
    cleared = 0
    for entry in db.values():
        had_any = any(f in entry for f in fields)
        for f in fields:
            entry.pop(f, None)
        if had_any:
            cleared += 1
    return cleared


def print_coverage(db: Dict[str, dict], label: str = "Coverage"):
    total = len(db)
    by_conf: Dict[str, int] = defaultdict(int)
    has_explicit_death = 0
    has_explicit_birth = 0
    has_windowed = 0

    for e in db.values():
        by_conf[e.get('lifetime_confidence') or 'unknown'] += 1
        if e.get('death_year_hijri') is not None:
            has_explicit_death += 1
        if e.get('birth_year_hijri') is not None:
            has_explicit_birth += 1
        if e.get('death_year_window') or e.get('birth_year_window'):
            has_windowed += 1

    print(f"\n{label}")
    print(f"  Total entries:       {total:,}")
    print(f"  Explicit death year: {has_explicit_death:,}  ({has_explicit_death*100//max(total,1)}%)")
    print(f"  Explicit birth year: {has_explicit_birth:,}  ({has_explicit_birth*100//max(total,1)}%)")
    print(f"  Tabaqah-windowed:    {has_windowed:,}")
    print(f"  Confidence breakdown:")
    for c, n in sorted(by_conf.items(), key=lambda x: -x[1]):
        print(f"    {c:<15}: {n:,}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Extract hijri lifetime years for rijal narrators")
    p.add_argument('--db', type=str, default=str(DATABASE_FILE),
                   help='Rijal database path (default: rijal_database_merged.json)')
    p.add_argument('--apply', action='store_true', help='Persist changes to DB (default: dry-run)')
    p.add_argument('--stats', action='store_true', help='Show current coverage and exit')
    p.add_argument('--rebuild', action='store_true', help='Clear all lifetime fields before extracting')
    p.add_argument('--sample', type=int, default=0,
                   help='Print N random extracted lifetimes for spot-checking')
    args = p.parse_args()

    db_path = Path(args.db)
    print(f"Loading {db_path} …", flush=True)
    with open(db_path, 'r', encoding='utf-8') as f:
        db = json.load(f)
    print(f"  {len(db):,} entries loaded")

    if args.stats:
        print_coverage(db, "Current state")
        return

    dry = not args.apply
    if args.rebuild:
        if dry:
            print("[--rebuild + dry-run: not clearing existing fields]")
        else:
            n = clear_lifetime_fields(db)
            print(f"Cleared lifetime fields on {n:,} entries")

    print(f"\nExtracting lifetime data ({'dry-run' if dry else 'apply'}) …", flush=True)
    counts = apply_lifetime_to_db(db, dry_run=dry)
    print(f"  both_explicit:   {counts.get('both_explicit', 0):,}")
    print(f"  death_only:      {counts.get('death_only', 0):,}")
    print(f"  windowed_only:   {counts.get('windowed_only', 0):,}")
    print(f"  unknown:         {counts.get('unknown', 0):,}")

    if args.sample:
        import random
        with_data = [e for e in db.values()
                     if e.get('death_year_hijri') is not None
                     or e.get('birth_year_hijri') is not None]
        print(f"\n— Random sample of {min(args.sample, len(with_data))} extracted lifetimes —")
        for e in random.sample(with_data, min(args.sample, len(with_data))):
            print(f"\n  {e.get('name_ar', '?')}")
            print(f"    birth: {e.get('birth_year_hijri') or e.get('birth_year_window')} "
                  f"({e.get('birth_year_source')})")
            print(f"    death: {e.get('death_year_hijri') or e.get('death_year_window')} "
                  f"({e.get('death_year_source')})")
            print(f"    confidence: {e.get('lifetime_confidence')}")
            ev = e.get('lifetime_evidence')
            if ev:
                print(f"    evidence: {ev[:200]}")

    print_coverage(db, "After extraction")

    if dry:
        print("\n[dry-run: nothing written. Pass --apply to persist.]")
        return

    print(f"\nSaving {db_path} …")
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"Done. {db_path.stat().st_size / 1e6:.1f} MB")


if __name__ == '__main__':
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

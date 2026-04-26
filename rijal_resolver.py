#!/usr/bin/env python3
"""
Rijāl Name Resolver v2 — Hadith Authentication Engine
======================================================
Resolves partial narrator names from hadith chains to their full identities.

Given a partial name like "أحمد بن محمد" appearing in a chain between
specific narrators, returns the most likely full narrator identity with
confidence score and status ruling.

This is the primary tool for hadith chain authentication.

v2 changes:
  - Compatible with idx-keyed database (from rijal_builder v2)
  - Falls back gracefully for n3-keyed databases (v1)
  - Displays both idx and n3 (Tehran edition) numbers for reference

Requirements:
    python rijal_prepass.py         (builds rijal_name_index.json)
    python rijal_builder.py ...     (builds rijal_database.json)
    python rijal_disambiguate.py    (builds rijal_identities.json)

Usage:
    # Build the resolver index (one-time):
    python rijal_resolver.py --build

    # Interactive resolution:
    python rijal_resolver.py --resolve

    # Direct lookup:
    python rijal_resolver.py --name "أحمد بن محمد" --from "الحسين بن سعيد" --by "سعد بن عبد الله"

    # Stats:
    python rijal_resolver.py --stats
"""

import json
import re
import sys
import argparse
import unicodedata
from pathlib import Path
from collections import defaultdict
from typing import Optional

SCRIPT_DIR       = Path(__file__).parent
DATABASE_FILE    = SCRIPT_DIR / "rijal_database_merged.json"
IDENTITIES_FILE  = SCRIPT_DIR / "rijal_identities.json"
RESOLVER_FILE    = SCRIPT_DIR / "rijal_resolver_index.json"

# Import the new database loader for lazy loading
try:
    from database_loader import DatabaseLoader, get_loader
except ImportError:
    # Fallback if database_loader is not available
    DatabaseLoader = None
    get_loader = None


# ─── Arabic text normalization ─────────────────────────────────────────────────

_ALEF_VARIANTS = re.compile(r'[أإآٱ]')
_TATWEEL       = re.compile(r'ـ')
_DIACRITICS    = re.compile(r'[\u064B-\u065F\u0670]')  # tashkeel / harakat / superscript alef

# OCR / Quranic-encoding artefact: "اللّٰه" strips diacritics to "الل ه"
# (a space is left inside الله).  Fix this specific pattern only.
_SPLIT_ALLAH = re.compile(r'الل\s+ه\b')
# Common city/name spelling variants that appear inconsistently in the database
# نيسابور / نيشابور (same city, sin vs shin)
_NISABUR   = re.compile(r'نيسابور')   # normalise to نيشابور
# رحمان / رحمن (elongated alef in Abd al-Rahman names)
_RAHMAN    = re.compile(r'رحمان')     # normalise to رحمن
# Terminal hamza after alef: "القلاء" → "القلا" (db omits hamza)
_TERMINAL_HAMZA = re.compile(r'اء(?=\s|$)')

def normalize_ar(text: str) -> str:
    """
    Normalize Arabic text for fuzzy matching:
    - Collapse alef variants (أ إ آ ٱ → ا)
    - Remove tatweel (ـ)
    - Remove all diacritics / harakat (including superscript alef U+0670)
    - Repair Quranic split-الله artefact ("الل ه" → "الله")
    - Canonicalize common spelling variants (نيسابور→نيشابور, رحمان→رحمن)
    - Collapse whitespace
    """
    if not text:
        return ""
    text = _ALEF_VARIANTS.sub('ا', text)
    text = _TATWEEL.sub('', text)
    text = _DIACRITICS.sub('', text)
    text = _SPLIT_ALLAH.sub('الله', text)
    text = _NISABUR.sub('نيشابور', text)
    text = _RAHMAN.sub('رحمن', text)
    text = _TERMINAL_HAMZA.sub('ا', text)  # القلاء → القلا
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def tokenize(text: str) -> list[str]:
    """Split normalized Arabic text into tokens (words)."""
    return normalize_ar(text).split()


# ─── Partial name form generation ─────────────────────────────────────────────

IBN_STOPWORDS = {'بن', 'ابن', 'بنت', 'أبي', 'أبو', 'ام', 'أم'}

def generate_name_forms(entry: dict) -> list[str]:
    """
    Generate all plausible partial name forms that this narrator might appear
    as in a hadith chain.

    Chains often abbreviate to:
      - Full name
      - Name without nisba
      - Name without laqab
      - Name without nisba and laqab
      - First 2 generations (X b. Y) — VERY common in long chains
      - Kunyah alone
      - Kunyah + name
      - "Ibn X" (patronymic short form) where X is father or grandfather
    """
    forms = set()
    name_ar = entry.get('name_ar', '')
    if not name_ar:
        return []

    norm_full = normalize_ar(name_ar)
    forms.add(norm_full)

    # Build components
    tokens    = tokenize(name_ar)
    kunyah    = normalize_ar(entry.get('kunyah', '') or '')
    laqab     = normalize_ar(entry.get('laqab', '') or '')
    nisba     = normalize_ar(entry.get('nisba', '') or '')
    father    = normalize_ar(entry.get('father', '') or '')
    grandfather = normalize_ar(entry.get('grandfather', '') or '')

    # Name without nisba
    if nisba and nisba in norm_full:
        forms.add(norm_full.replace(nisba, '').strip())

    # Name without laqab
    if laqab and laqab in norm_full:
        forms.add(norm_full.replace(laqab, '').strip())

    # Name without both nisba and laqab
    stripped = norm_full
    if nisba:  stripped = stripped.replace(nisba, '')
    if laqab:  stripped = stripped.replace(laqab, '')
    stripped = stripped.strip()
    if stripped:
        forms.add(stripped)

    # First 2 words of the main name (X b. Y form)
    # Filter out prepositions
    content_tokens = [t for t in tokens if t not in IBN_STOPWORDS]
    if len(content_tokens) >= 2:
        # X ibn Y — reassemble with بن
        first_name = content_tokens[0]
        second_name = content_tokens[1]
        forms.add(f"{first_name} بن {second_name}")
        forms.add(normalize_ar(f"{first_name} بن {second_name}"))

    # Kunyah forms
    if kunyah:
        forms.add(kunyah)
        if nisba:
            forms.add(f"{kunyah} {nisba}".strip())
        if laqab:
            forms.add(f"{kunyah} {laqab}".strip())

    # Ibn [father] short form
    if father:
        forms.add(f"ابن {father}")

    # Abu [X] alone if kunyah is أبو/أبي X
    # Also add ابي variants with nisba/laqab so "ابي سعيد القماط" matches correctly
    if kunyah and kunyah.startswith('ابو '):
        abi_form = kunyah.replace('ابو ', 'ابي ')
        forms.add(abi_form)
        if nisba:
            forms.add(f"{abi_form} {nisba}".strip())
        if laqab:
            forms.add(f"{abi_form} {laqab}".strip())

    # Aliases: each alias gets the same form treatment as the main name
    for alias in (entry.get('aliases') or []):
        alias_norm = normalize_ar(alias)
        if alias_norm:
            forms.add(alias_norm)
            # Also strip nisba from alias
            if nisba and nisba in alias_norm:
                forms.add(alias_norm.replace(nisba, '').strip())

    # Clean up: remove empty strings and very short strings (less than 3 chars)
    forms = {f for f in forms if len(f) >= 3}
    return list(forms)


# ─── Utility: get display number (n3 Tehran) from entry ──────────────────────

def get_display_number(entry: dict, entry_key: str) -> str:
    """Get the Tehran edition number for display.
    Works with both v1 (n3-keyed) and v2 (idx-keyed) databases."""
    # v2 format: _num_tehran metadata field
    if '_num_tehran' in entry:
        return entry['_num_tehran']
    # v1 format: the key itself IS the n3 number
    return entry_key


# Status priority for best-entry selection within a canonical cluster.
# Lower rank = better (thiqah is best, daif is worst).
_STATUS_RANK = {
    'thiqah':      0,
    'mamduh':      1,
    'hasan':       2,
    'muwaththaq':  3,
    'unspecified': 4,
    'majhul':      5,
    'daif':        6,
}


# ─── Build resolver index ─────────────────────────────────────────────────────

def build_resolver_index(db: dict, identities: dict) -> dict:
    """
    Build a partial-name → candidates index.

    Structure:
    {
      "normalized_partial_name": [
        {
          "entry_key": "1234",
          "canonical_key": "1234",
          "n3_display": "١٢٣٤",
          "name_ar": "أحمد بن محمد بن عيسى الأشعري القمي",
          "name_en": "Ahmad ibn Muhammad ibn 'Isa al-Ash'ari al-Qummi",
          "status": "thiqah",
          "narrates_from_imams": [...],
          "narrates_from_narrators": [...],
          "narrated_from_by": [...],
          "books": [...],
          "has_book": true,
          "period_hint": "...",
        },
        ...
      ]
    }
    """
    entry_to_canonical = identities.get('entry_to_canonical', {})
    cluster_data       = identities.get('clusters', {})

    # For each entry, look up its cluster's merged narration network
    def get_cluster_data(entry_key: str) -> dict:
        canonical = entry_to_canonical.get(entry_key, entry_key)
        return cluster_data.get(canonical, {})

    index: dict[str, list] = defaultdict(list)
    # Track which canonical keys we've already indexed (avoid duplicates)
    indexed_canonicals: set[str] = set()

    for entry_key, entry in db.items():
        canonical_key = entry_to_canonical.get(entry_key, entry_key)

        # Only index each canonical once (even if it has many aliases)
        # All aliases map to the same canonical
        if canonical_key in indexed_canonicals:
            # Still add alias name forms pointing to the canonical record
            pass
        else:
            indexed_canonicals.add(canonical_key)

        cdata = get_cluster_data(entry_key)

        # Merged narration data from the cluster (more complete than single entry)
        merged_imams   = sorted(set(
            (entry.get('narrates_from_imams') or []) +
            cdata.get('narrates_from_imams', [])
        ))
        merged_teachers = sorted(set(
            (entry.get('narrates_from_narrators') or []) +
            cdata.get('narrates_from_narrators', [])
        ))
        merged_students = sorted(set(
            (entry.get('narrated_from_by') or []) +
            cdata.get('narrated_from_by', [])
        ))
        merged_books = sorted(set(
            (entry.get('books') or []) +
            cdata.get('books', [])
        ))

        candidate = {
            "entry_key":              entry_key,
            "canonical_key":          canonical_key,
            "n3_display":             get_display_number(entry, entry_key),
            "name_ar":                entry.get('name_ar', ''),
            "name_en":                entry.get('name_en', ''),
            "status":                 entry.get('status', 'unspecified'),
            "narrates_from_imams":    merged_imams,
            "narrates_from_narrators": merged_teachers,
            "narrated_from_by":       merged_students,
            "books":                  merged_books,
            "has_book":               entry.get('has_book', False),
            "hadith_count":           entry.get('hadith_count'),   # needed for famousness bonus
            "period_hint":            entry.get('period_hint'),
            "identity_confidence":    entry.get('identity_confidence', 'unique'),
            "disambiguation_notes":   entry.get('disambiguation_notes'),
            "tabaqah":                entry.get('tabaqah'),
        }

        # Generate all name forms for this entry
        for form in generate_name_forms(entry):
            norm_form = normalize_ar(form)
            # Per (form, canonical_key) pair keep only the BEST-STATUS entry.
            # This matters when the identities file clusters multiple db entries
            # (e.g. several "عبد الله بن سنان" entries) under one canonical —
            # without this, whichever entry is processed first wins, even if it
            # is "unspecified" while a later entry is "thiqah".
            existing_idx = None
            for ci, c in enumerate(index[norm_form]):
                if c['canonical_key'] == canonical_key:
                    existing_idx = ci
                    break
            if existing_idx is None:
                index[norm_form].append(candidate)
            elif (_STATUS_RANK.get(candidate['status'], 99) <
                  _STATUS_RANK.get(index[norm_form][existing_idx]['status'], 99)):
                index[norm_form][existing_idx] = candidate  # better status wins

    # Convert defaultdict to plain dict for JSON serialization
    final_index = {k: v for k, v in index.items() if v}
    return final_index


# ─── Scoring ──────────────────────────────────────────────────────────────────

def score_candidate(candidate: dict, query_norm: str,
                    narrates_from: list[str], narrated_by: list[str],
                    book: Optional[str],
                    expected_tabaqah_range: Optional[tuple[int, int]] = None
                    ) -> tuple[float, list[str]]:
    """
    Score a candidate narrator against resolution context.
    Returns (score, reasons).
    """
    score   = 0.0
    reasons = []

    name_norm = normalize_ar(candidate['name_ar'])
    query_tokens = set(tokenize(query_norm))
    name_tokens  = set(tokenize(name_norm))

    # ── Name match score ─────────────────────────────────────────────────────
    # Exact normalized match → highest
    if query_norm == name_norm:
        score += 3.0
        reasons.append(f"Exact name match: {candidate['name_ar']}")
    else:
        # Token overlap
        overlap = query_tokens & name_tokens
        non_stop = overlap - IBN_STOPWORDS
        if non_stop:
            token_score = len(non_stop) / max(len(query_tokens - IBN_STOPWORDS), 1)
            score += token_score * 2.0
            reasons.append(f"Partial name match ({len(non_stop)}/{len(query_tokens)} tokens)")

    # ── Position bonus: query tokens must appear as a PREFIX of the name ─────
    # This prevents e.g. "زرارة" from matching "رومي بن زرارة بن أعين" equally
    # with "زرارة بن أعين" — the token appears at the START of the correct entry.
    name_content  = [t for t in tokenize(name_norm)  if t not in IBN_STOPWORDS]
    query_content = [t for t in tokenize(query_norm) if t not in IBN_STOPWORDS]
    if (query_content and name_content and
            name_content[:len(query_content)] == query_content):
        score += 1.5
        reasons.append("Name prefix match")

    # ── Network match: teachers (narrates_from) ──────────────────────────────
    known_teachers = set(
        normalize_ar(t) for t in
        (candidate['narrates_from_narrators'] + candidate['narrates_from_imams'])
    )
    for teacher in narrates_from:
        nt = normalize_ar(teacher)
        if nt in known_teachers:
            score += 2.0
            reasons.append(f"Known teacher match: {teacher}")
        else:
            # Partial teacher name match
            teacher_tokens = set(tokenize(nt)) - IBN_STOPWORDS
            for kt in known_teachers:
                kt_tokens = set(tokenize(kt)) - IBN_STOPWORDS
                if teacher_tokens and teacher_tokens.issubset(kt_tokens):
                    score += 1.0
                    reasons.append(f"Partial teacher match: {teacher}")
                    break

    # ── Network match: students (narrated_by) ───────────────────────────────
    known_students = set(normalize_ar(s) for s in candidate['narrated_from_by'])
    for student in narrated_by:
        ns = normalize_ar(student)
        if ns in known_students:
            score += 2.0
            reasons.append(f"Known student match: {student}")
        else:
            student_tokens = set(tokenize(ns)) - IBN_STOPWORDS
            for ks in known_students:
                ks_tokens = set(tokenize(ks)) - IBN_STOPWORDS
                if student_tokens and student_tokens.issubset(ks_tokens):
                    score += 1.0
                    reasons.append(f"Partial student match: {student}")
                    break

    # ── Book match ───────────────────────────────────────────────────────────
    if book:
        book_norm = normalize_ar(book)
        for b in candidate['books']:
            if normalize_ar(b) in book_norm or book_norm in normalize_ar(b):
                score += 1.0
                reasons.append(f"Appears in book: {b}")
                break

    # ── Confidence bonus ─────────────────────────────────────────────────────
    ic = candidate.get('identity_confidence', 'unique')
    if ic == 'certain':   score += 0.1
    elif ic == 'uncertain': score -= 0.2

    # ── Famousness bonus (based on hadith count) ───────────────────────────
    count = candidate.get('hadith_count')
    if count:
        try:
            # Add a logarithmic bonus for hadith count
            # Increased to 0.2 multiplier to make it more impactful
            import math
            score += math.log10(float(count) + 1) * 0.2
            reasons.append(f"Famousness bonus (count={count})")
        except (ValueError, TypeError):
            pass

    # ── Reliability bias (favor thiqah in ambiguous ties) ──────────────────
    status = candidate.get('status')
    if status == 'thiqah':
        score += 2.0  # Dominant bonus for reliable narrators
    elif status == 'daif':
        score -= 2.0  # Dominant penalty for weak ones

    # ── Tabaqah fitness score ──────────────────────────────────────────────────
    if expected_tabaqah_range is not None:
        t_low, t_high = expected_tabaqah_range
        t_cand = candidate.get('tabaqah')
        if t_cand is not None:
            import math
            # Use float range check (T6.9 is genuinely below T7.1 even after rounding)
            # Distance is ceiling of float gap so T6.9 vs T7.1 = ceil(0.2) = 1 generation
            if t_low <= t_cand <= t_high:
                dist = 0
            elif t_cand < t_low:
                dist = math.ceil(t_low - t_cand)
            else:
                dist = math.ceil(t_cand - t_high)

            if dist == 0:
                tab_bonus = 1.5
            elif dist == 1:
                tab_bonus = 0.0
            elif dist == 2:
                tab_bonus = -1.0
            else:
                tab_bonus = -2.0

            score += tab_bonus
            t_low_i, t_high_i, t_cand_i = round(t_low), round(t_high), round(t_cand)
            reasons.append(
                f"Tabaqah T{t_cand_i} vs expected [T{t_low_i}-T{t_high_i}]: "
                f"{'in range' if dist == 0 else f'dist={dist}'} ({tab_bonus:+.1f})"
            )

    return score, reasons


# ─── Resolution ───────────────────────────────────────────────────────────────

def resolve(resolver_index: dict, name: str,
            narrates_from: Optional[list[str]] = None,
            narrated_by: Optional[list[str]] = None,
            book: Optional[str] = None,
            top_k: int = 5,
            expected_tabaqah_range: Optional[tuple[int, int]] = None) -> dict:
    """
    Resolve a partial name to narrator identities.

    Args:
        name:          Arabic name as it appears in the chain (partial ok)
        narrates_from: List of narrators/Imams this person narrates FROM
                       (person after them in the chain direction)
        narrated_by:   List of narrators who narrate FROM this person
                       (person before them in the chain direction)
        book:          Book this hadith appears in (optional)
        top_k:         How many results to return

    Returns:
        dict with top_match, other_candidates, total_candidates
    """
    narrates_from = narrates_from or []
    narrated_by   = narrated_by   or []

    norm_query = normalize_ar(name)
    query_tokens = set(tokenize(norm_query)) - IBN_STOPWORDS

    # Collect candidate pool
    # canonical_key → candidate  (one representative per canonical cluster)
    candidates_pool: dict[str, dict] = {}

    def _pool_add(c: dict):
        """Add c to pool, upgrading if it has a better status than the current entry."""
        existing = candidates_pool.get(c['canonical_key'])
        if existing is None:
            candidates_pool[c['canonical_key']] = c
        elif (_STATUS_RANK.get(c['status'], 99) <
              _STATUS_RANK.get(existing['status'], 99)):
            candidates_pool[c['canonical_key']] = c   # better status wins

    # 1. Exact form match (highest priority)
    if norm_query in resolver_index:
        for c in resolver_index[norm_query]:
            _pool_add(c)

    # 1b. Definite-article (ال) toggle on first word:
    #     "النضر بن سويد" ↔ "نضر بن سويد" — done at query time to avoid index bloat
    first_word = norm_query.split()[0] if norm_query.split() else ''
    if first_word.startswith('ال') and len(first_word) > 2:
        alt_query = norm_query[2:]   # strip leading ال
    elif first_word and not first_word.startswith('ال'):
        alt_query = 'ال' + norm_query  # add leading ال
    else:
        alt_query = ''
    if alt_query and alt_query in resolver_index:
        for c in resolver_index[alt_query]:
            _pool_add(c)

    # 1c. Al/non-al toggle on the word after "بن":
    #     "هارون بن الخارجة" ↔ "هارون بن خارجة" — same person, db uses bare form
    norm_words = norm_query.split()
    for i, w in enumerate(norm_words):
        if w in ('بن', 'ابن') and i + 1 < len(norm_words):
            next_w = norm_words[i + 1]
            if next_w.startswith('ال') and len(next_w) > 2:
                alt_words = norm_words[:]; alt_words[i + 1] = next_w[2:]
            else:
                alt_words = norm_words[:]; alt_words[i + 1] = 'ال' + next_w
            alt_q = ' '.join(alt_words)
            if alt_q in resolver_index:
                for c in resolver_index[alt_q]:
                    _pool_add(c)

    # 1d. Strip kunyah prefix "ابي/ابو/ابا [laqab]" from start:
    #     "ابي هاشم داود بن القاسم" → try "داود بن القاسم"
    if norm_words and norm_words[0] in ('ابي', 'ابو', 'ابا') and len(norm_words) >= 3:
        alt_q = ' '.join(norm_words[2:])
        if alt_q in resolver_index:
            for c in resolver_index[alt_q]:
                _pool_add(c)

    # 2. Partial match: index forms whose non-stop tokens ⊇ query tokens
    if len(candidates_pool) < 20:
        for form, cands in resolver_index.items():
            form_tokens = set(tokenize(form)) - IBN_STOPWORDS
            if query_tokens and query_tokens.issubset(form_tokens):
                for c in cands:
                    _pool_add(c)

    # 1e. Drop trailing nisba/laqab (last word):
    #     "اسماعيل بن عبد الرحمن الجعفي" → try "اسماعيل بن عبد الرحمن"
    #     Safer than full reverse-subset match — only drops ONE trailing token.
    if len(norm_words) >= 3:
        alt_q = ' '.join(norm_words[:-1])
        if alt_q in resolver_index:
            for c in resolver_index[alt_q]:
                _pool_add(c)

    if not candidates_pool:
        return {
            "query": name,
            "top_match": None,
            "other_candidates": [],
            "total_candidates": 0,
            "message": "No candidates found for this name.",
        }

    # Score all candidates
    scored = []
    for canonical_key, candidate in candidates_pool.items():
        sc, reasons = score_candidate(
            candidate, norm_query, narrates_from, narrated_by, book,
            expected_tabaqah_range=expected_tabaqah_range
        )
        scored.append((sc, candidate, reasons))

    scored.sort(key=lambda x: -x[0])

    def format_result(score, candidate, reasons):
        return {
            "entry_key":           candidate['entry_key'],
            "canonical_key":       candidate['canonical_key'],
            "n3_display":          candidate.get('n3_display', candidate['entry_key']),
            "name_ar":             candidate['name_ar'],
            "name_en":             candidate['name_en'],
            "status":              candidate['status'],
            "confidence_score":    round(score, 3),
            "narrates_from_imams": candidate['narrates_from_imams'],
            "books":               candidate['books'],
            "period_hint":         candidate['period_hint'],
            "has_book":            candidate['has_book'],
            "match_reasons":       reasons,
            "disambiguation_notes": candidate.get('disambiguation_notes'),
        }

    results = [format_result(sc, c, r) for sc, c, r in scored[:top_k]]

    return {
        "query":            name,
        "narrates_from":    narrates_from,
        "narrated_by":      narrated_by,
        "book":             book,
        "top_match":        results[0] if results else None,
        "other_candidates": results[1:],
        "total_candidates": len(candidates_pool),
    }


# ─── Display ──────────────────────────────────────────────────────────────────

def print_result(result: dict):
    print(f"\n{'─'*60}")
    print(f"  Query: {result['query']}")
    if result.get('narrates_from'):
        print(f"  Narrates from: {', '.join(result['narrates_from'])}")
    if result.get('narrated_by'):
        print(f"  Narrated by:   {', '.join(result['narrated_by'])}")
    if result.get('book'):
        print(f"  Book:          {result['book']}")
    print(f"  Total candidates found: {result['total_candidates']}")
    print()

    if not result['top_match']:
        print("  ⚠  No match found.")
        return

    tm = result['top_match']
    stars = '★★★' if tm['confidence_score'] >= 3 else '★★' if tm['confidence_score'] >= 1.5 else '★'
    status_label = {
        'thiqah':     '✓ THIQAH (trustworthy)',
        'daif':       '✗ DA\'ĪF (weak)',
        'majhul':     '? MAJHŪL (unknown)',
        'mamduh':     '~ MAMDŪH (praised)',
        'hasan':      '~ ḤASAN (good)',
        'muwaththaq': '~ MUWATHTHAQ (reliable)',
        'unspecified':'  UNSPECIFIED',
    }.get(tm['status'], tm['status'])

    n3_display = tm.get('n3_display', tm['canonical_key'])
    print(f"  {stars} TOP MATCH [n3={n3_display}, idx={tm['canonical_key']}]")
    print(f"  Arabic:  {tm['name_ar']}")
    print(f"  English: {tm['name_en']}")
    print(f"  Status:  {status_label}")
    print(f"  Score:   {tm['confidence_score']}")
    if tm['narrates_from_imams']:
        print(f"  Imams:   {', '.join(tm['narrates_from_imams'])}")
    if tm['period_hint']:
        print(f"  Era:     {tm['period_hint']}")
    if tm['books']:
        print(f"  Books:   {', '.join(tm['books'])}")
    print(f"  Reasons: {'; '.join(tm['match_reasons']) or 'name form match'}")
    if tm['disambiguation_notes']:
        print(f"  Notes:   {tm['disambiguation_notes']}")

    if result['other_candidates']:
        print(f"\n  Other candidates:")
        for c in result['other_candidates']:
            n3d = c.get('n3_display', c['canonical_key'])
            print(f"    [n3={n3d}, idx={c['canonical_key']}] {c['name_ar']}  "
                  f"({c['status']})  score={c['confidence_score']}")

    print(f"{'─'*60}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

def load_files():
    if not DATABASE_FILE.exists():
        print("ERROR: rijal_database.json not found. Run rijal_builder.py first.")
        sys.exit(1)
    if not IDENTITIES_FILE.exists():
        print("ERROR: rijal_identities.json not found. Run rijal_disambiguate.py first.")
        sys.exit(1)
    
    # Load identities file (needed for building resolver index)
    with open(IDENTITIES_FILE, 'r', encoding='utf-8') as f:
        identities = json.load(f)
    
    # Use DatabaseLoader for lazy loading of the database
    # This avoids loading the entire database into memory
    if get_loader:
        loader = get_loader()
        # For building the index, we need to iterate through all entries
        # The loader provides efficient iteration with caching
        db = {}
        for entry in loader.iter_entries():
            # Get the entry key from the entry itself or use a counter
            entry_key = entry.get('_entry_idx', str(len(db)))
            db[entry_key] = entry
    else:
        # Fallback to old method if loader is not available
        with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
            db = json.load(f)
    
    return db, identities



def main():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    parser = argparse.ArgumentParser(
        description="Rijāl Name Resolver v2 — Hadith Authentication Engine"
    )
    parser.add_argument("--build",   action="store_true",
                        help="Build/rebuild the resolver index from current database")
    parser.add_argument("--resolve", action="store_true",
                        help="Interactive name resolution session")
    parser.add_argument("--name",    type=str,
                        help="Arabic name to resolve")
    parser.add_argument("--from",    dest="narrates_from", type=str, default="",
                        help="Who this narrator narrates FROM (comma-separated)")
    parser.add_argument("--by",      dest="narrated_by",   type=str, default="",
                        help="Who narrates FROM this narrator (comma-separated)")
    parser.add_argument("--book",    type=str, default="",
                        help="Book this chain appears in")
    parser.add_argument("--top-k",  type=int, default=5,
                        help="Number of results to return (default: 5)")
    parser.add_argument("--stats",   action="store_true",
                        help="Show index statistics")
    args = parser.parse_args()

    # ── Build index ──────────────────────────────────────────────────────────
    if args.build or not RESOLVER_FILE.exists():
        print("Building resolver index ...")
        db, identities = load_files()
        idx = build_resolver_index(db, identities)
        with open(RESOLVER_FILE, 'w', encoding='utf-8') as f:
            json.dump(idx, f, ensure_ascii=False, indent=2)
        print(f"✓ Resolver index built: {len(idx):,} name forms → {RESOLVER_FILE.name}")
        if not args.resolve and not args.name and not args.stats:
            return

    if not RESOLVER_FILE.exists():
        print("No resolver index. Run with --build first.")
        sys.exit(1)

    with open(RESOLVER_FILE, 'r', encoding='utf-8') as f:
        resolver_index = json.load(f)

    # ── Stats ────────────────────────────────────────────────────────────────
    if args.stats:
        total_candidates = sum(len(v) for v in resolver_index.values())
        print(f"\nResolver Index Statistics:")
        print(f"  Name forms indexed:  {len(resolver_index):,}")
        print(f"  Total candidate refs:{total_candidates:,}")
        print(f"  Avg candidates/form: {total_candidates/len(resolver_index):.1f}")
        # Show most ambiguous names
        print(f"\n  Most ambiguous names (most candidates):")
        sorted_forms = sorted(resolver_index.items(), key=lambda x: -len(x[1]))
        for form, cands in sorted_forms[:15]:
            print(f"    {form:40s} → {len(cands)} candidates")
        return

    # ── Direct lookup ────────────────────────────────────────────────────────
    if args.name:
        nf = [x.strip() for x in args.narrates_from.split(',') if x.strip()] if args.narrates_from else []
        nb = [x.strip() for x in args.narrated_by.split(',')   if x.strip()] if args.narrated_by   else []
        result = resolve(resolver_index, args.name, nf, nb,
                         args.book or None, args.top_k)
        print_result(result)
        return

    # ── Interactive mode ─────────────────────────────────────────────────────
    if args.resolve:
        print(f"\nRijāl Name Resolver v2 — Interactive Mode")
        print(f"Loaded {len(resolver_index):,} name forms.")
        print("Type 'quit' to exit.\n")

        while True:
            print("─" * 60)
            name = input("Arabic name to resolve: ").strip()
            if name.lower() in ('quit', 'exit', 'q'):
                break
            if not name:
                continue

            nf_raw = input("Narrates FROM (optional, comma-sep): ").strip()
            nb_raw = input("Narrated BY   (optional, comma-sep): ").strip()
            bk_raw = input("Book          (optional): ").strip()

            nf = [x.strip() for x in nf_raw.split(',') if x.strip()]
            nb = [x.strip() for x in nb_raw.split(',') if x.strip()]

            result = resolve(resolver_index, name, nf, nb, bk_raw or None, args.top_k)
            print_result(result)
        return

    parser.print_help()


if __name__ == "__main__":
    main()

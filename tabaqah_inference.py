#!/usr/bin/env python3
"""
Tabaqah Inference Engine — canonical implementation
===================================================

Assigns a generational layer (tabaqah, 1–12) to every narrator in the rijal
database. Replaces the older trio of `tabaqah_inference.py` (slow, O(N^2M)),
`tabaqah_inferrer.py` (CLI), and `infer_tabaqah.py` (Dirichlet relaxation that
produced fractional tabaqat).

Tiers, run in order; each tier locks its assignments against override by later
tiers (alf_rajul is canonical, never overridden by inference):

  Tier 0  ─  alf_rajul seeding         tabaqah_source = "alf_rajul"
            Authoritative tabaqat from Mu'jam al-Alf Rajul (Sayyid Ghayth Shubar,
            anchored in al-Burujirdi's 12-generation system). Locks 988 entries.

  Tier 1  ─  Imam-direct propagation   tabaqah_source = "inferred_imam"
            Narrator X who appears in `narrates_from_imams` for Imam Y of
            tabaqah T is assigned tabaqah T (contemporary of the Imam).

  Tier 2  ─  Network propagation       tabaqah_source = "inferred_network"
            Bidirectional iterative propagation: median of {teacher+1, student-1}
            votes. Variance check rejects assignments where teachers span >3
            tabaqat (flagged as `tabaqah_conflict`).

  Tier 3  ─  LLM (DeepSeek/Claude)     tabaqah_source = "inferred_llm"
            For stragglers with no graph signal but a non-empty `_raw_full` text,
            ask the model. Stores the model's evidence quote.

Public API (preserved for backward compatibility):
  IMAM_TABAQAH, IMAM_TABAQAH_MAP        canonical-key → tabaqah dicts
  BURUJIRDI_AH_RANGES                   tabaqah → (start_AH, end_AH) tuple
  TabaqahInferenceEngine(db)            class with .infer_all() / .apply_to_db()

CLI:
  python tabaqah_inference.py --stats          # show coverage, no writes
  python tabaqah_inference.py --tier 1         # imam-direct only
  python tabaqah_inference.py --tier 2         # imam + network (default)
  python tabaqah_inference.py --tier 3         # all tiers (LLM included)
  python tabaqah_inference.py --apply          # write changes to DB
  python tabaqah_inference.py --rebuild        # clear all inferred (keep alf_rajul) and re-run
  python tabaqah_inference.py --workers 5      # LLM concurrency for tier 3
"""

import json
import os
import re
import sys
import time
import threading
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from rijal_resolver import DATABASE_FILE

# ─── File paths ──────────────────────────────────────────────────────────────

SCRIPT_DIR        = Path(__file__).resolve().parent
ALF_RAJUL_DB      = SCRIPT_DIR / "alf_rajul_database.json"
ALF_DISAMB_FILE   = SCRIPT_DIR / "alf_rajul_disambiguation_llm.json"
LLM_PROGRESS      = SCRIPT_DIR / "tabaqah_infer_progress.json"
OVERRIDES_FILE    = SCRIPT_DIR / "tabaqah_overrides.json"

# ─── Burujirdi 12-generation system ──────────────────────────────────────────

# Canonical key (used by isnad_analyzer.VIRTUAL_NARRATORS) → tabaqah.
IMAM_TABAQAH: Dict[str, int] = {
    'IMAM_PROPHET': 1,
    'IMAM_ALI':     2,
    'IMAM_HASAN':   2,
    'IMAM_HUSAYN':  2,
    'IMAM_SAJJAD':  3,
    'IMAM_BAQIR':   4,
    'IMAM_SADIQ':   5,
    'IMAM_KADHIM':  6,
    'IMAM_REZA':    7,
    'IMAM_JAWAD':   7,
    'IMAM_HADI':    8,
    'IMAM_ASKARI':  8,
    'IMAM_MAHDI':   9,
}

# Alias used by isnad_analyzer.py (which also defines its own copy locally).
IMAM_TABAQAH_MAP = IMAM_TABAQAH

# Burujirdi/Shubar AH ranges per tabaqah. Used by Phase B (lifetime extractor)
# to derive birth/death windows for narrators with explicit tabaqah but no
# explicit dates. Values are inclusive.
BURUJIRDI_AH_RANGES: Dict[int, Tuple[int, int]] = {
    1:  ( 10,  40),   # Companions of the Prophet
    2:  ( 40,  70),   # Companions of Imam Ali
    3:  ( 70, 100),   # Era of Imam al-Sajjad
    4:  (100, 130),   # Companions of Imam al-Baqir
    5:  (130, 170),   # Companions of Imam al-Sadiq
    6:  (170, 200),   # Companions of Imam al-Kadhim
    7:  (200, 240),   # Companions of Imams al-Reza and al-Jawad
    8:  (240, 270),   # Companions of Imams al-Hadi and al-'Askari
    9:  (270, 300),   # Early minor occultation
    10: (300, 330),   # Late minor occultation
    11: (330, 360),   # Early major occultation
    12: (360, 400),   # Era of classical compilation
}

# Arabic Imam title → tabaqah. Used to scan free-text fields like
# `narrates_from_imams` and `companions_of`. Sorted longest-first at use site
# so that "الحسن بن علي العسكري" matches T8 before "الحسن بن علي" (T2).
ARABIC_IMAM_TABAQAH: Dict[str, int] = {
    # T1
    'النبي': 1, 'رسول الله': 1, 'محمد بن عبد الله': 1,
    # T2
    'أمير المؤمنين': 2, 'علي بن أبي طالب': 2,
    'الحسن بن علي بن أبي طالب': 2, 'الحسين بن علي بن أبي طالب': 2,
    'الحسن المجتبى': 2, 'الحسين الشهيد': 2,
    'سيد الشهداء': 2,
    # T3
    'علي بن الحسين': 3, 'السجاد': 3, 'زين العابدين': 3,
    # T4
    'محمد بن علي الباقر': 4, 'الباقر': 4, 'أبي جعفر الباقر': 4,
    # T5
    'جعفر بن محمد الصادق': 5, 'الصادق': 5, 'أبي عبد الله الصادق': 5,
    'أبي عبد الله': 5,  # default to al-Sadiq when ambiguous; chain context can override
    # T6
    'موسى بن جعفر': 6, 'الكاظم': 6, 'أبي الحسن الأول': 6, 'أبي إبراهيم': 6,
    # T7
    'علي بن موسى الرضا': 7, 'الرضا': 7, 'أبي الحسن الثاني': 7,
    'محمد بن علي الجواد': 7, 'الجواد': 7, 'أبي جعفر الثاني': 7,
    # T8
    'علي بن محمد الهادي': 8, 'الهادي': 8, 'أبي الحسن الثالث': 8,
    'الحسن بن علي العسكري': 8, 'العسكري': 8, 'أبي محمد العسكري': 8,
    # T9
    'المهدي': 9, 'القائم': 9, 'صاحب الأمر': 9, 'الحجة': 9, 'صاحب الزمان': 9,
}

# ─── Arabic normalization helpers ────────────────────────────────────────────

_DIAC = re.compile(r'[ً-ٰٟ]')
_ALEF = re.compile(r'[إأآا]')
_HONORIFIC_PARENS = re.compile(
    r'\s*[\(\[]?\s*(?:عليه|عليها|عليهما|عليهم)\s+(?:السلام|السلم)\s*[\)\]]?'
)
_HONORIFIC_LETTERS = re.compile(r'\s*[\(\[]?\s*[عصج]{1,3}\s*[\)\]]?')


def _norm(t: str) -> str:
    """Light Arabic normalization for tabaqah inference name matching.

    More aggressive than `rijal_resolver.normalize_ar` because we want to match
    across spelling variants — but consistent enough that the same name in
    different places will land on the same key.
    """
    if not t:
        return ''
    t = _DIAC.sub('', t)
    t = _ALEF.sub('ا', t)
    t = t.replace('ک', 'ك')   # Persian Keheh → Arabic Kaf
    t = t.replace('ی', 'ي')   # Farsi Yeh → Arabic Yeh
    t = t.replace('ۍ', 'ي')
    t = t.replace('ى', 'ي')   # Alef Maksura → Yeh
    t = t.replace('ہ', 'ه')   # Heh Goal → Heh
    t = t.replace('ة', 'ه')
    t = t.replace('ى', 'ي')
    t = t.replace('ـ', '')
    t = re.sub(r'[^؀-ۿ\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _strip_imam_honorifics(name: str) -> str:
    """Drop (ع), (عليه السلام), etc. from an Imam reference."""
    n = _norm(name)
    n = _HONORIFIC_PARENS.sub('', n)
    n = _HONORIFIC_LETTERS.sub('', n)
    return n.strip()


def imam_tabaqah_from_name(name: str) -> Optional[int]:
    """Find the tabaqah for an Imam reference like 'أبي عبد الله (ع)'.

    Returns None if no Imam title matches.
    """
    n = _strip_imam_honorifics(name)
    if not n:
        return None
    # Sort longest title first so 'الحسن بن علي العسكري' beats 'الحسن بن علي'
    for title, t in sorted(ARABIC_IMAM_TABAQAH.items(), key=lambda x: -len(x[0])):
        if _norm(title) in n:
            return t
    return None


# ─── Source tag helpers ──────────────────────────────────────────────────────

MANUAL_OVERRIDE_SOURCE  = "manual_override"   # highest priority, hand-curated
ALF_RAJUL_SOURCE       = "alf_rajul"          # canonical, never overridden by inference
INFERRED_IMAM_SOURCE   = "inferred_imam"
INFERRED_NETWORK_SOURCE = "inferred_network"
INFERRED_LLM_SOURCE    = "inferred_llm"
PROPAGATED_ALIAS_SOURCE = "propagated_alias"  # copied from canonical entry to aliases

# Sources that are LOCKED against override by lower tiers
LOCKED_SOURCES: Set[str] = {
    MANUAL_OVERRIDE_SOURCE,
    ALF_RAJUL_SOURCE,
    "Alf Rajul",   # legacy spelling
}

# Legacy source labels seen in older databases. We treat any of these as
# "previously inferred" and overwritable in --rebuild mode.
LEGACY_INFERRED_SOURCES: Set[str] = {
    "Inferred", "Alf Rajul",         # from old infer_tabaqah.py (note the space)
    "inferred_high", "inferred_medium", "inferred_low",  # from old tabaqah_inference.py
}


def _is_seed(entry: dict) -> bool:
    """Locked sources (manual_override, alf_rajul) — never overridden."""
    src = entry.get('tabaqah_source')
    return src in LOCKED_SOURCES


def _is_alias(entry: dict) -> bool:
    """True if this entry is an alias of another canonical entry.

    Alias entries should be skipped during inference and have their tabaqah
    propagated from the canonical at the end of the pipeline.
    """
    canon = entry.get('canonical_entry')
    return bool(canon) and canon != entry.get('_entry_idx')


def _has_tabaqah(entry: dict) -> bool:
    return entry.get('tabaqah') is not None


# ─── Tier −1: Manual overrides (highest priority) ────────────────────────────

def apply_manual_overrides(
    db: Dict[str, dict],
    overrides: Dict[str, dict],
    dry_run: bool = False,
) -> int:
    """Apply hand-curated tabaqah values from `tabaqah_overrides.json`.

    Override format (entry_idx → spec):
        {
            "6331": {
                "tabaqah": 7,
                "tabaqah_sub": null,
                "evidence": "Narrates from Abu Khadijah (T5), teacher of Sahl b. Ziyad (T8)."
            }
        }

    Overrides take precedence over alf_rajul seeds (a scholar who wrote an
    override has presumably looked at this case specifically). Source tag is
    `manual_override` and confidence is `certain`.

    Returns count of entries updated.
    """
    if not overrides:
        return 0

    updated = 0
    for key, spec in overrides.items():
        # Skip JSON-comment keys (anything starting with `_`)
        if key.startswith('_'):
            continue
        # Skip non-dict values (e.g. plain string comments)
        if not isinstance(spec, dict):
            continue
        # Try direct key, then string conversion of _entry_idx
        target_k = key if key in db else None
        if target_k is None:
            for k, e in db.items():
                if str(e.get('_entry_idx')) == str(key):
                    target_k = k
                    break
        if target_k is None:
            print(f"  [override] entry {key} not found in DB — skipping", flush=True)
            continue

        # Follow canonical pointer so the override applies at the canonical
        # node; aliases will be filled in by propagate_to_aliases().
        target_k = _canonicalize(target_k, db)
        entry = db.get(target_k)
        if entry is None:
            continue

        t = spec.get('tabaqah')
        if t is None or not isinstance(t, int) or not (1 <= t <= 12):
            print(f"  [override] entry {key} has invalid tabaqah: {t} — skipping", flush=True)
            continue

        if dry_run:
            updated += 1
            continue

        entry['tabaqah'] = t
        entry['tabaqah_detail'] = spec.get('tabaqah_detail') or f'الطبقة {t} (إدخال يدوي)'
        entry['tabaqah_sub'] = spec.get('tabaqah_sub')
        entry['tabaqah_source'] = MANUAL_OVERRIDE_SOURCE
        entry['tabaqah_confidence'] = 'certain'
        entry['tabaqah_evidence'] = spec.get('evidence') or 'تعديل يدوي'
        # Clear any prior conflict marker
        entry.pop('tabaqah_conflict', None)
        updated += 1

    return updated


# ─── Tier 0: Alf Rajul seeding ───────────────────────────────────────────────

def _build_db_name_index(db: Dict[str, dict]) -> Dict[str, List[str]]:
    """Map normalized full-name → list of db keys. Used for alf_rajul matching."""
    idx: Dict[str, List[str]] = defaultdict(list)
    for k, e in db.items():
        for name in [e.get('name_ar', '')] + (e.get('aliases') or []):
            n = _norm(name)
            if n:
                idx[n].append(k)
    return idx


def _canonicalize(key: str, db: Dict[str, dict]) -> str:
    """Follow `canonical_entry` redirects (used in merged DB clusters)."""
    seen = set()
    cur = key
    while cur in db and cur not in seen:
        seen.add(cur)
        canon = db[cur].get('canonical_entry')
        if not canon or canon == cur:
            return cur
        cur = canon
    return key


def seed_from_alf_rajul(
    db: Dict[str, dict],
    alf_db: Dict[str, dict],
    disamb: Dict[str, str] = None,
    dry_run: bool = False,
) -> Tuple[int, List[Tuple[str, str]]]:
    """Apply alf_rajul tabaqat to the rijal DB as canonical seeds.

    Returns (matched_count, unmatched_list) where unmatched_list is
    [(alf_key, name_ar), ...] for entries that could not be located in the DB.
    """
    disamb = disamb or {}
    name_idx = _build_db_name_index(db)
    matched = 0
    unmatched: List[Tuple[str, str]] = []

    for alf_key, alf in alf_db.items():
        t = alf.get('tabaqah')
        if t is None:
            continue
        try:
            t = int(t)
        except (TypeError, ValueError):
            continue
        if not (1 <= t <= 12):
            continue

        target_k = None

        # 1) Manual disambiguation override (from alf_rajul_disambiguation_llm.json)
        override = disamb.get(alf_key)
        if override and str(override) in db:
            target_k = str(override)

        # 2) Direct name match (try main name then alt names)
        if not target_k:
            candidates_to_try = [alf.get('name_ar', '')] + (alf.get('alt_names') or [])
            for candidate in candidates_to_try:
                hits = name_idx.get(_norm(candidate), [])
                if len(hits) == 1:
                    target_k = hits[0]
                    break

        if not target_k:
            unmatched.append((alf_key, alf.get('name_ar', '?')))
            continue

        target_k = _canonicalize(target_k, db)
        entry = db.get(target_k)
        if entry is None:
            unmatched.append((alf_key, alf.get('name_ar', '?')))
            continue

        if dry_run:
            matched += 1
            continue

        entry['tabaqah'] = t
        entry['tabaqah_detail'] = alf.get('tabaqah_detail') or f'الطبقة {t}'
        entry['tabaqah_sub'] = alf.get('tabaqah_sub')
        entry['tabaqah_source'] = ALF_RAJUL_SOURCE
        entry['tabaqah_confidence'] = 'certain'
        entry['tabaqah_evidence'] = f'مُعجم الألف رجل (إدخال {alf_key})'

        # Merge alt_names from alf_rajul into rijal aliases (preserve uniqueness)
        if alf.get('alt_names'):
            existing = entry.get('aliases') or []
            for alt in alf['alt_names']:
                if alt and alt != entry.get('name_ar') and alt not in existing:
                    existing.append(alt)
            entry['aliases'] = existing

        matched += 1

    return matched, unmatched


# ─── Tier 1: Imam-direct propagation ─────────────────────────────────────────

def tier1_imam(db: Dict[str, dict], dry_run: bool = False) -> int:
    """A narrator who narrates FROM Imam Y is in Imam Y's tabaqah.

    Returns count of new assignments. Skips alf_rajul seeds and entries already
    assigned in this run (so re-running is idempotent). Skips alias entries —
    propagate_to_aliases() handles those at the end of the pipeline.

    Bug-fix #4: same Imam appearing in BOTH `narrates_from_imams` and
    `companions_of` is counted once, not twice. Previously, ["الصادق"] in
    both fields produced votes [5, 5] and locked in T5 even when other
    evidence (e.g., a pure narrates_from_imams entry) suggested otherwise.
    """
    assigned = 0
    unmatched_imams_count = 0

    for entry in db.values():
        if _is_seed(entry):
            continue
        if _is_alias(entry):
            continue
        if _has_tabaqah(entry):
            continue

        # Dedupe: collect (normalized_imam_name → tabaqah) so the same Imam
        # mentioned in both fields contributes one vote.
        imam_votes: Dict[str, int] = {}
        unrecognized: List[str] = []

        for imam_name in (entry.get('narrates_from_imams') or []):
            t = imam_tabaqah_from_name(imam_name)
            if t is not None:
                imam_votes[_strip_imam_honorifics(imam_name)] = t
            elif imam_name.strip():
                unrecognized.append(imam_name)

        companions = entry.get('companions_of') or []
        if isinstance(companions, str):
            companions = [companions] if companions else []
        for comp_name in companions:
            t = imam_tabaqah_from_name(comp_name)
            if t is not None:
                # Use companion key in same namespace so duplicates collapse
                imam_votes[_strip_imam_honorifics(comp_name)] = t

        votes = list(imam_votes.values())

        if not votes:
            # Bug-fix #5 awareness: surface the unrecognized Imam title
            # rather than silently dropping. A scholar reading the DB can
            # then add the title to ARABIC_IMAM_TABAQAH if it's a real Imam.
            if unrecognized and not dry_run:
                entry['tabaqah_imam_unmatched'] = unrecognized[:3]
            unmatched_imams_count += 1 if unrecognized else 0
            continue

        # Median is robust against one outlier (e.g. mistakenly listed as
        # narrating from Prophet AND from al-Sadiq).
        votes.sort()
        inferred = votes[len(votes) // 2]
        confidence = 'high' if len(votes) >= 2 else 'medium'

        if not dry_run:
            entry['tabaqah'] = inferred
            entry['tabaqah_detail'] = f'الطبقة {inferred} (مستنتج من رواية عن الإمام)'
            entry['tabaqah_sub'] = None
            entry['tabaqah_source'] = INFERRED_IMAM_SOURCE
            entry['tabaqah_confidence'] = confidence
            entry['tabaqah_evidence'] = f'الإمام: {", ".join(list(imam_votes.keys())[:3])}'

        assigned += 1

    if unmatched_imams_count:
        print(f"  [tier1] {unmatched_imams_count:,} entries had Imam refs that could not be matched", flush=True)
    return assigned


# ─── Tier 2: Bidirectional network propagation ───────────────────────────────

def _build_word_index(db: Dict[str, dict], n_words: int) -> Dict[str, List[str]]:
    """Map first-N-word key → list of db keys (canonical only, alias entries
    excluded since they share names with their canonical and would inflate
    counts).

    Returns the FULL list (not just unambiguous) — caller decides what to do
    with multi-candidate keys.
    """
    idx: Dict[str, List[str]] = defaultdict(list)
    for db_key, entry in db.items():
        if _is_alias(entry):
            continue
        for name in [entry.get('name_ar', '')] + (entry.get('aliases') or []):
            words = _norm(name).split()
            if len(words) >= n_words:
                key = ' '.join(words[:n_words])
                if db_key not in idx[key]:
                    idx[key].append(db_key)
    return dict(idx)


def _resolve_name_in_index(
    name: str,
    idx_3word: Dict[str, List[str]],
    idx_2word: Dict[str, List[str]],
) -> Optional[str]:
    """Resolve a name to a single canonical db_key.

    Bug-fix #2: prefer 3-word matches (much less prone to collisions like
    'إبراهيم بن X' matching a T2 figure). Fall back to 2-word ONLY if it
    yields a unique match. If ambiguous at both depths, return None — better
    to skip the vote than to inject noise.
    """
    words = _norm(name).split()
    if len(words) < 2:
        return None

    # 3-word match (preferred): require unique
    if len(words) >= 3:
        k3 = ' '.join(words[:3])
        hits3 = idx_3word.get(k3, [])
        if len(hits3) == 1:
            return hits3[0]
        if len(hits3) > 1:
            # Ambiguous at 3-word — bail rather than fall back to coarser
            # 2-word matching (would just be more ambiguous).
            return None

    # 2-word fallback: only when 3-word yielded nothing
    k2 = ' '.join(words[:2])
    hits2 = idx_2word.get(k2, [])
    if len(hits2) == 1:
        return hits2[0]
    return None


def tier2_network(
    db: Dict[str, dict],
    max_rounds: int = 10,
    dry_run: bool = False,
) -> int:
    """Bidirectional iterative propagation through the teacher/student graph.

    Per round, for each unassigned narrator:
      - Collect 'teacher+1' votes from documented teachers with known tabaqah
      - Collect 'student-1' votes from documented students with known tabaqah
      - If votes span more than 3 tabaqat, flag `tabaqah_conflict` and skip
      - Otherwise assign the median, clamped to [1, 12]

    Stops early on convergence (zero new assignments in a round).

    Bug-fix #1: alias entries are skipped (their tabaqah is propagated from
    the canonical entry at the end of the pipeline).
    Bug-fix #2: name resolution uses 3-word matching with 2-word fallback,
    eliminating most spurious votes from name collisions.
    """
    idx_3word = _build_word_index(db, 3)
    idx_2word = _build_word_index(db, 2)
    # 2-word index: only keep unambiguous keys
    idx_2word_unambig = {k: v for k, v in idx_2word.items() if len(v) == 1}

    total = 0

    for round_num in range(1, max_rounds + 1):
        round_assigned = 0

        for entry in db.values():
            if _is_seed(entry):
                continue
            if _is_alias(entry):
                continue
            if _has_tabaqah(entry):
                continue

            teacher_votes: List[int] = []
            student_votes: List[int] = []

            for tname in (entry.get('narrates_from_narrators') or []):
                tk = _resolve_name_in_index(tname, idx_3word, idx_2word_unambig)
                if tk:
                    tt = db.get(tk, {}).get('tabaqah')
                    if tt is not None:
                        teacher_votes.append(int(tt))

            for sname in (entry.get('narrated_from_by') or []):
                sk = _resolve_name_in_index(sname, idx_3word, idx_2word_unambig)
                if sk:
                    st = db.get(sk, {}).get('tabaqah')
                    if st is not None:
                        student_votes.append(int(st))

            votes: List[int] = [t + 1 for t in teacher_votes] + [s - 1 for s in student_votes]
            if not votes:
                continue

            # Variance check: a narrator whose teachers span >3 tabaqat is
            # almost certainly a name collision in `narrates_from_narrators`,
            # not a real connection. Don't assign — flag for review.
            spread = max(votes) - min(votes)
            if spread > 3:
                if not dry_run:
                    entry['tabaqah_conflict'] = {
                        'votes': votes,
                        'spread': spread,
                        'teachers': (entry.get('narrates_from_narrators') or [])[:5],
                        'students': (entry.get('narrated_from_by') or [])[:5],
                    }
                continue

            votes.sort()
            inferred = max(1, min(12, votes[len(votes) // 2]))

            num_paths = len(votes)
            if num_paths >= 3 and spread <= 1:
                confidence = 'high'
            elif num_paths >= 2:
                confidence = 'medium'
            else:
                confidence = 'low'

            if not dry_run:
                entry['tabaqah'] = inferred
                entry['tabaqah_detail'] = f'الطبقة {inferred} (مستنتج من الشبكة)'
                entry['tabaqah_sub'] = None
                entry['tabaqah_source'] = INFERRED_NETWORK_SOURCE
                entry['tabaqah_confidence'] = confidence
                evidence_parts = []
                if teacher_votes:
                    evidence_parts.append(f'شيوخ معروفو الطبقة: {len(teacher_votes)}')
                if student_votes:
                    evidence_parts.append(f'تلامذة معروفو الطبقة: {len(student_votes)}')
                entry['tabaqah_evidence'] = ' | '.join(evidence_parts)

            round_assigned += 1

        total += round_assigned
        print(f"  Round {round_num}: assigned {round_assigned:,} new tabaqat", flush=True)
        if round_assigned == 0:
            break

    return total


# ─── Final pass: propagate tabaqah from canonical to alias entries ───────────

def propagate_to_aliases(db: Dict[str, dict], dry_run: bool = False) -> int:
    """Copy tabaqah fields from each canonical entry to all its aliases.

    Bug-fix #1: alias entries (those with `canonical_entry` pointing elsewhere)
    are skipped during inference. After all tiers complete, this function
    copies the canonical entry's tabaqah to its aliases so chains that resolve
    to an alias entry still get a tabaqah value.

    Returns count of alias entries updated.
    """
    updated = 0
    fields_to_copy = (
        'tabaqah', 'tabaqah_detail', 'tabaqah_sub',
        'tabaqah_confidence', 'tabaqah_evidence',
    )

    for k, entry in db.items():
        if not _is_alias(entry):
            continue
        canon_key = _canonicalize(k, db)
        if canon_key == k:
            continue
        canon = db.get(canon_key)
        if canon is None or not _has_tabaqah(canon):
            continue

        if dry_run:
            updated += 1
            continue

        for f in fields_to_copy:
            if f in canon:
                entry[f] = canon[f]
        # Tag the source so it's clear this is propagated, not directly inferred.
        # Preserve the original source as a sub-field for provenance.
        entry['tabaqah_source'] = PROPAGATED_ALIAS_SOURCE
        entry['tabaqah_canonical_source'] = canon.get('tabaqah_source')
        entry['tabaqah_canonical_entry'] = canon_key
        # Clear any stale conflict record on the alias
        entry.pop('tabaqah_conflict', None)
        updated += 1

    return updated


# ─── Tier 3: LLM inference ───────────────────────────────────────────────────

TIER3_SYSTEM_PROMPT = """\
أنت خبير في علم طبقات رجال الحديث الشيعي. ستُعطى ترجمة راوٍ كاملة من معجم رجال الحديث.

مهمتك: تحديد الطبقة الزمنية للراوي وفق النظام الاثني عشري (نظام السيد البروجردي):
  1 = أصحاب النبي ﷺ                         (~١٠–٤٠ هـ)
  2 = أصحاب الإمام علي (ع)                  (~٤٠–٧٠ هـ)
  3 = عصر الإمام السجاد (ع)                 (~٧٠–١٠٠ هـ)
  4 = أصحاب الإمام الباقر (ع)               (~١٠٠–١٣٠ هـ)
  5 = أصحاب الإمام الصادق (ع)               (~١٣٠–١٧٠ هـ)
  6 = أصحاب الإمام الكاظم (ع)               (~١٧٠–٢٠٠ هـ)
  7 = أصحاب الإمامين الرضا والجواد (ع)      (~٢٠٠–٢٤٠ هـ)
  8 = أصحاب الإمامين الهادي والعسكري (ع)    (~٢٤٠–٢٧٠ هـ)
  9 = الغيبة الصغرى الأولى                  (~٢٧٠–٣٠٠ هـ)
  10 = الغيبة الصغرى الأخيرة               (~٣٠٠–٣٣٠ هـ)
  11 = أوائل الغيبة الكبرى                  (~٣٣٠–٣٦٠ هـ)
  12 = عصر التصنيف الكلاسيكي               (~٣٦٠–٤٠٠ هـ)

أعِد JSON فقط بلا أي شرح:
{
  "tabaqah": <رقم 1–12 أو null>,
  "evidence": "<اقتباس مباشر من النص يدعم التحديد>",
  "confidence": "<high|medium|low>"
}
"""


def _call_deepseek_tabaqah(prompt: str, api_key: str, timeout: int = 60) -> Optional[dict]:
    """One-shot DeepSeek call with retries on rate-limit. Returns parsed JSON or None."""
    try:
        import requests
    except ImportError:
        raise RuntimeError("`requests` package required for tier 3 LLM inference")

    payload = {
        "model": "deepseek-v4-flash",
        "messages": [
            {"role": "system", "content": TIER3_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 256,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.deepseek.com/chat/completions",
                json=payload, headers=headers, timeout=timeout
            )
        except Exception as e:
            print(f"  [LLM exception attempt {attempt+1}: {e}]", flush=True)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            time.sleep(20)
            continue
        if r.status_code != 200:
            print(f"  [LLM HTTP {r.status_code}: {r.text[:120]}]", flush=True)
            return None
        text = r.json()["choices"][0]["message"]["content"]
        text = re.sub(r'<think>[\s\S]*?</think>', '', text)
        text = re.sub(r'```(?:json)?\s*', '', text).strip()
        m = re.search(r'\{[\s\S]*\}', text)
        if not m:
            return None
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            return None
    return None


def tier3_llm(
    db: Dict[str, dict],
    workers: int = 5,
    dry_run: bool = False,
    max_entries: Optional[int] = None,
) -> int:
    """LLM inference for unassigned entries that have a `_raw_full` biography.

    Uses a progress file (`tabaqah_infer_progress.json`) so re-runs skip
    already-processed entries. DEEPSEEK_API_KEY must be set in the environment.
    """
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("  [tier 3 skipped: DEEPSEEK_API_KEY not set]", flush=True)
        return 0

    candidates = [
        (k, e) for k, e in db.items()
        if not _is_seed(e)
        and not _is_alias(e)
        and not _has_tabaqah(e)
        and (e.get('_raw_full') or e.get('_raw'))
    ]

    done: Set[str] = set()
    if LLM_PROGRESS.exists():
        try:
            done = set(json.loads(LLM_PROGRESS.read_text(encoding='utf-8')).get('done', []))
        except Exception:
            done = set()

    candidates = [(k, e) for k, e in candidates if k not in done]
    if max_entries:
        candidates = candidates[:max_entries]

    print(f"  LLM queue: {len(candidates):,} entries (skipping {len(done):,} already processed)", flush=True)
    if not candidates:
        return 0

    assigned = 0
    lock = threading.Lock()
    counter = [0]

    def process(item):
        k, e = item
        name = e.get('name_ar', '?')
        raw = (e.get('_raw_full') or e.get('_raw') or '')[:3000]
        prompt = f"الراوي: {name}\n\nالنص:\n{raw}"
        try:
            return k, _call_deepseek_tabaqah(prompt, api_key), None
        except Exception as exc:
            return k, None, str(exc)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(process, item): item for item in candidates}
        for fut in as_completed(futs):
            k, result, err = fut.result()
            with lock:
                counter[0] += 1
                done.add(k)

                if err:
                    print(f"  [ERR] {k}: {err}", flush=True)
                elif result and isinstance(result.get('tabaqah'), int):
                    t = result['tabaqah']
                    if 1 <= t <= 12 and not dry_run:
                        entry = db[k]
                        entry['tabaqah'] = t
                        entry['tabaqah_detail'] = f'الطبقة {t} (مستنتج بالذكاء الاصطناعي)'
                        entry['tabaqah_source'] = INFERRED_LLM_SOURCE
                        entry['tabaqah_confidence'] = result.get('confidence') or 'medium'
                        entry['tabaqah_evidence'] = result.get('evidence') or ''
                        assigned += 1

                if counter[0] % 100 == 0:
                    print(f"  [{counter[0]:,}/{len(candidates):,}]  assigned: {assigned:,}", flush=True)
                    if not dry_run:
                        LLM_PROGRESS.write_text(json.dumps({'done': sorted(done)}), encoding='utf-8')

    if not dry_run:
        LLM_PROGRESS.write_text(json.dumps({'done': sorted(done)}), encoding='utf-8')

    return assigned


# ─── Engine class (backward-compatible API) ──────────────────────────────────

class TabaqahInferenceEngine:
    """Backward-compatible class wrapping the tier functions.

    Older callers (and `test_isnad.py`) expect:
        engine = TabaqahInferenceEngine(db)
        result = engine.infer_all()
        # result[entry_key] == {'tabaqah': int, 'tabaqah_source': str,
        #                       'tabaqah_confidence': str, 'inference_sources': [...]}
    """

    def __init__(self, db: Dict[str, dict]):
        self.db = db
        self._inferred: Dict[str, dict] = {}

    def infer_all(self) -> Dict[str, dict]:
        """Run Tier 1 + Tier 2 in dry-run mode and return inferred values.

        Note: this method is non-mutating for backward compatibility with
        the older test suite. Use `apply_to_db()` to actually write results.
        """
        # Snapshot pre-existing tabaqat so we can detect what's new.
        pre = {k: e.get('tabaqah') for k, e in self.db.items()}

        tier1_imam(self.db, dry_run=False)
        tier2_network(self.db, dry_run=False)

        result: Dict[str, dict] = {}
        for k, e in self.db.items():
            if pre.get(k) is None and e.get('tabaqah') is not None:
                result[k] = {
                    'tabaqah': int(e['tabaqah']),
                    'tabaqah_source': e.get('tabaqah_source'),
                    'tabaqah_confidence': e.get('tabaqah_confidence'),
                    'inference_sources': [e.get('tabaqah_evidence', '')],
                }

        # Restore non-mutating contract: roll back tabaqat we just set so
        # callers who only want the dict don't accidentally mutate the DB.
        # (The class's stored `db` reference IS the caller's dict, so we
        #  must undo our writes here.)
        for k, original in pre.items():
            if original is None and self.db[k].get('tabaqah') is not None:
                # Caller did not have tabaqah before — clear our additions
                for field in ('tabaqah', 'tabaqah_detail', 'tabaqah_sub',
                              'tabaqah_source', 'tabaqah_confidence', 'tabaqah_evidence',
                              'tabaqah_conflict'):
                    self.db[k].pop(field, None)

        self._inferred = result
        return result

    def apply_to_db(self, dry_run: bool = True) -> Tuple[int, int]:
        """Run Tier 1 + Tier 2 and actually write to the DB."""
        n1 = tier1_imam(self.db, dry_run=dry_run)
        n2 = tier2_network(self.db, dry_run=dry_run)
        return (n1 + n2, n1 + n2)

    def infer_tabaqah_for_narrator(self, entry_key: str) -> Optional[int]:
        """Single-narrator inference. Used by isnad_analyzer for chain analysis."""
        entry = self.db.get(entry_key)
        if not entry:
            return None
        if entry.get('tabaqah') is not None:
            return int(entry['tabaqah'])

        # Try Imam-direct first
        votes: List[int] = []
        for imam in (entry.get('narrates_from_imams') or []):
            t = imam_tabaqah_from_name(imam)
            if t is not None:
                votes.append(t)
        if votes:
            votes.sort()
            return votes[len(votes) // 2]

        return None


# ─── Coverage reporting ──────────────────────────────────────────────────────

def print_coverage(db: Dict[str, dict], label: str = "Coverage"):
    total = len(db)
    with_tab = sum(1 for e in db.values() if _has_tabaqah(e))
    by_src: Dict[str, int] = defaultdict(int)
    for e in db.values():
        if _has_tabaqah(e):
            by_src[e.get('tabaqah_source') or 'unknown'] += 1

    conflicts = sum(1 for e in db.values() if e.get('tabaqah_conflict'))

    print(f"\n{label}")
    print(f"  Total entries:    {total:,}")
    pct = (with_tab / total * 100) if total else 0
    print(f"  With tabaqah:     {with_tab:,}  ({pct:.1f}%)")
    for src, cnt in sorted(by_src.items(), key=lambda x: -x[1]):
        print(f"    {src:<25}: {cnt:,}")
    if conflicts:
        print(f"  Variance conflicts (skipped): {conflicts:,}")


# ─── Rebuild helper ──────────────────────────────────────────────────────────

def clear_inferred(db: Dict[str, dict]) -> int:
    """Clear all non-canonical tabaqah assignments (preserves manual_override
    and alf_rajul seeds).

    Used by --rebuild. Returns count cleared.
    """
    cleared = 0
    fields_to_clear = (
        'tabaqah', 'tabaqah_detail', 'tabaqah_sub',
        'tabaqah_source', 'tabaqah_confidence', 'tabaqah_evidence',
        'tabaqah_conflict', 'tabaqah_imam_unmatched',
        'tabaqah_canonical_source', 'tabaqah_canonical_entry',
    )
    for e in db.values():
        if _is_seed(e):
            continue
        if _has_tabaqah(e) or e.get('tabaqah_source') in LEGACY_INFERRED_SOURCES:
            for f in fields_to_clear:
                e.pop(f, None)
            cleared += 1
    return cleared


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Tabaqah inference for the rijal database")
    p.add_argument('--tier', type=int, default=2, choices=[0, 1, 2, 3],
                   help='Highest tier to run (0=alf_rajul only, 1=+imam, 2=+network, 3=+LLM)')
    p.add_argument('--apply', action='store_true', help='Write changes to DB (default: dry-run)')
    p.add_argument('--stats', action='store_true', help='Show coverage and exit')
    p.add_argument('--rebuild', action='store_true',
                   help='Clear all inferred values (keeping alf_rajul seeds) before running')
    p.add_argument('--workers', type=int, default=5, help='LLM concurrency (tier 3)')
    p.add_argument('--max-llm', type=int, default=None,
                   help='Cap tier-3 candidates (for testing)')
    p.add_argument('--db', type=str, default=str(DATABASE_FILE),
                   help='Rijal database path (default: rijal_database_merged.json)')
    p.add_argument('--alf-db', type=str, default=str(ALF_RAJUL_DB),
                   help='Alf rajul database path')
    p.add_argument('--overrides', type=str, default=str(OVERRIDES_FILE),
                   help='Manual tabaqah overrides JSON (default: tabaqah_overrides.json)')
    p.add_argument('--report-unmatched', type=str, default='alf_rajul_match_audit.json',
                   help='Where to write list of unmatched alf_rajul entries')
    p.add_argument('--no-propagate-aliases', action='store_true',
                   help='Skip the final canonical-to-alias propagation pass')
    args = p.parse_args()

    db_path = Path(args.db)
    print(f"Loading {db_path} …", flush=True)
    with open(db_path, 'r', encoding='utf-8') as f:
        db = json.load(f)
    print(f"  {len(db):,} entries loaded")

    print_coverage(db, "BEFORE")

    if args.stats:
        return

    dry = not args.apply

    if args.rebuild:
        if dry:
            print("\n[--rebuild requested but in dry-run; not clearing]")
        else:
            n = clear_inferred(db)
            print(f"\nCleared {n:,} previously-inferred entries (alf_rajul seeds preserved)")

    # Tier 0: alf_rajul seeding (always runs, idempotent — overwrites stale seeds)
    alf_db: Dict[str, dict] = {}
    if Path(args.alf_db).exists():
        with open(args.alf_db, 'r', encoding='utf-8') as f:
            alf_db = json.load(f)
        disamb: Dict[str, str] = {}
        if ALF_DISAMB_FILE.exists():
            try:
                disamb = {str(k): str(v) for k, v in
                          json.loads(ALF_DISAMB_FILE.read_text(encoding='utf-8')).items()}
            except Exception:
                disamb = {}
        print(f"\nTier 0: alf_rajul seeding ({len(alf_db):,} alf entries)")
        n_seed, unmatched = seed_from_alf_rajul(db, alf_db, disamb=disamb, dry_run=dry)
        print(f"  Matched: {n_seed:,}")
        print(f"  Unmatched: {len(unmatched):,}")
        if unmatched:
            audit_path = Path(args.report_unmatched)
            audit_path.write_text(
                json.dumps([{'alf_key': k, 'name_ar': n} for k, n in unmatched],
                           ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            print(f"  Unmatched audit → {audit_path}")
    else:
        print(f"\n[no alf_rajul DB at {args.alf_db}; skipping Tier 0]")

    # Manual overrides (applied AFTER alf_rajul so a hand-curated value wins
    # over an alf_rajul seed when both exist for the same entry).
    overrides_path = Path(args.overrides)
    if overrides_path.exists():
        try:
            overrides = json.loads(overrides_path.read_text(encoding='utf-8'))
        except Exception as e:
            print(f"\n[overrides] failed to load {overrides_path}: {e}", flush=True)
            overrides = {}
        if overrides:
            print(f"\nManual overrides ({len(overrides):,} entries from {overrides_path.name})")
            n_ov = apply_manual_overrides(db, overrides, dry_run=dry)
            print(f"  Applied: {n_ov:,}")
    else:
        print(f"\n[no overrides file at {overrides_path}; skipping]")

    if args.tier >= 1:
        print("\nTier 1: Imam-direct propagation")
        n = tier1_imam(db, dry_run=dry)
        print(f"  Assigned: {n:,}")

    if args.tier >= 2:
        print("\nTier 2: Bidirectional network propagation")
        n = tier2_network(db, dry_run=dry)
        print(f"  Total assigned: {n:,}")

    if args.tier >= 3:
        print("\nTier 3: LLM (DeepSeek)")
        n = tier3_llm(db, workers=args.workers, dry_run=dry, max_entries=args.max_llm)
        print(f"  Assigned: {n:,}")

    # Final pass: copy canonical entry's tabaqah to all its aliases.
    if not args.no_propagate_aliases:
        print("\nFinal pass: propagating tabaqah from canonical entries to aliases")
        n_alias = propagate_to_aliases(db, dry_run=dry)
        print(f"  Aliases updated: {n_alias:,}")

    print_coverage(db, "AFTER")

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

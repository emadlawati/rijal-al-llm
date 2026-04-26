#!/usr/bin/env python3
"""
Kunya Disambiguator — Phase 4
==============================

Resolves kunya-only chain references like "أبو بصير", "أبو عبد الله", "أبو علي"
to a specific narrator using the surrounding chain context.

The rijal DB has a `kunyah` field on most entries, but a single kunya can map to
many narrators (the four famous Abū Baṣīr's, ~30 Abū ʿAbd Allāh's, etc.). This
module ranks the candidates by:

  +5  Adjacent-narrator teacher–student match
        The narrator immediately before or after the kunya in the chain
        appears in the candidate's `narrates_from_narrators` or
        `narrated_from_by` fields. This is the strongest signal — established
        student/teacher pairings are well-documented.

  +3  Imam attribution match
        The chain attributes to Imam X, and the candidate is documented as a
        companion of Imam X (`narrates_from_imams`/`companions_of`).

  +2 / -∞  Tabaqah constraint
        Candidate's tabaqah must be within ±1 of the chain's expected tabaqah
        range (computed from neighbors). Incompatible candidates are
        eliminated, not just penalized.

  +2 / -∞  Lifetime overlap (Phase 3 integration)
        Candidate's lifetime must be `possible` or better with adjacent
        narrators. `impossible` eliminates the candidate.

  +1  Book/compiler bias
        Some kunyas are conventionally one specific person in a given book
        (e.g., "أبو عبد الله" in al-Kāfī almost always means Imam al-Sadiq
        when he's the chain target). Encoded as a small overrides table.

Output:
    Resolution:
        kunya:                "أبو بصير"
        top_candidate:        rijal entry dict (or None if all eliminated)
        confidence:           "high" | "medium" | "low" | "ambiguous"
        score:                final composite score
        runner_up:            second-best candidate (only when ambiguous)
        evidence:             list of strings showing how each rule contributed
        eliminated:           list of (entry, reason) for candidates ruled out
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from tabaqah_inference import _norm  # shared Arabic normalizer

# Try to use lifetime validator; degrade gracefully if it's missing.
try:
    from transmission_validator import can_transmit, VERDICT_IMPOSSIBLE, VERDICT_TIGHT
except ImportError:
    can_transmit = None
    VERDICT_IMPOSSIBLE = 'impossible'
    VERDICT_TIGHT = 'tight'


# ─── Scoring weights (tunable from a future grading_policy.py) ───────────────

W_TEACHER_STUDENT_MATCH = 5
W_IMAM_ATTRIBUTION      = 3
W_TABAQAH_FIT           = 2
W_LIFETIME_FIT          = 2
W_BOOK_CONVENTION       = 1

# Confidence thresholds
SCORE_HIGH       = 7   # at minimum: a documented student match + tabaqah fit
SCORE_MEDIUM     = 4
AMBIGUOUS_GAP    = 2   # if (top_score - runner_up_score) < this, flag ambiguous


# ─── Book conventions ────────────────────────────────────────────────────────

# In a given book, certain kunyas have a heavy default. Override sparingly —
# this is meant to encode the strongest conventions, not micro-bias every case.
# Keys are (book_id_substring, normalized_kunya). Values bias the canonical_key
# of the candidate.
BOOK_KUNYA_CONVENTIONS: Dict[Tuple[str, str], List[str]] = {
    # In al-Kāfī, "أبو عبد الله" overwhelmingly = al-Sadiq (when used as chain
    # target, not as transmitter). Detection of "as chain target" is a
    # caller responsibility; we just bias the Imam canonical_key here.
    ('Al-Kafi', 'ابو عبد الله'): ['IMAM_SADIQ'],
}


# ─── Inputs ──────────────────────────────────────────────────────────────────

@dataclass
class ChainContext:
    """Information about the chain surrounding the kunya we want to resolve."""
    # Narrator immediately before this kunya in the chain (the student who
    # heard from this kunya — they are this kunya's STUDENT). May be None
    # if the kunya is at the start of the chain.
    student_neighbor: Optional[dict] = None

    # Narrator immediately after this kunya (the kunya's TEACHER). May be
    # None if the kunya is at the end of the chain.
    teacher_neighbor: Optional[dict] = None

    # If the chain is attributed to an Imam (e.g., "قال أبو عبد الله"), this
    # is the canonical Imam key (IMAM_SADIQ etc).
    imam_target: Optional[str] = None

    # The book this hadith comes from (e.g., 'Al-Kafi-Volume-1-Kulayni').
    book_id: Optional[str] = None

    # Expected tabaqah range from chain analysis: (low, high) inclusive.
    expected_tabaqah_range: Optional[Tuple[int, int]] = None


@dataclass
class CandidateScore:
    entry:        dict
    score:        int
    eliminated:   bool = False
    elimination_reason: Optional[str] = None
    rule_hits:    List[str] = field(default_factory=list)


@dataclass
class Resolution:
    kunya:         str
    top_candidate: Optional[dict]
    confidence:    str            # "high" | "medium" | "low" | "ambiguous" | "none"
    score:         int
    runner_up:     Optional[dict] = None
    evidence:      List[str] = field(default_factory=list)
    eliminated:    List[Tuple[str, str]] = field(default_factory=list)
    all_scores:    List[CandidateScore] = field(default_factory=list)


# ─── Kunya normalization ─────────────────────────────────────────────────────

# Strip the "عن " / "حدثني " / etc. prefix; we want the bare kunya.
_KUNYA_PREFIX = re.compile(r'^(?:عن|حدثنا|حدثني|أخبرنا|أخبرني|قال|سمعت)\s+')


def normalize_kunya(name: str) -> str:
    """Return a normalized form of a kunya for matching against the DB."""
    n = _norm(name or '')
    n = _KUNYA_PREFIX.sub('', n).strip()
    return n


def _entry_kunya_norm(entry: dict) -> str:
    return _norm(entry.get('kunyah') or '')


# ─── Candidate gathering ─────────────────────────────────────────────────────

def find_kunya_candidates(kunya: str, db: Dict[str, dict]) -> List[dict]:
    """All entries in the rijal DB whose kunyah matches the given kunya."""
    target = normalize_kunya(kunya)
    if not target:
        return []
    out: List[dict] = []
    for entry in db.values():
        if _entry_kunya_norm(entry) == target:
            out.append(entry)
            continue
        # Also accept aliases that start with this kunya (e.g., "أبو بصير الأسدي"
        # has kunya "أبو بصير" but might be listed under the longer alias).
        for alias in (entry.get('aliases') or []):
            if _norm(alias).startswith(target + ' '):
                out.append(entry)
                break
    return out


# ─── Rule helpers ────────────────────────────────────────────────────────────

def _name_in_list(name_to_find: str, name_list: List[str]) -> bool:
    """True if name_to_find appears (as a substring of significant tokens) in
    any of the names in name_list."""
    target_norm = _norm(name_to_find)
    if not target_norm:
        return False
    target_tokens = [t for t in target_norm.split() if len(t) >= 3]
    if not target_tokens:
        return False

    for n in name_list:
        n_norm = _norm(n)
        # Whole-string match
        if n_norm == target_norm:
            return True
        # All significant tokens of target appear in n
        if all(tok in n_norm for tok in target_tokens):
            return True
    return False


def _check_teacher_student_match(candidate: dict, ctx: ChainContext) -> Tuple[int, List[str]]:
    """Score teacher/student edges between the candidate and its neighbors.

    Returns (score, evidence_strings).
    """
    score = 0
    evidence: List[str] = []

    # ctx.student_neighbor heard FROM this candidate, so they are this
    # candidate's STUDENT. Check that:
    #   - student_neighbor.name is in candidate.narrated_from_by, OR
    #   - candidate.name is in student_neighbor.narrates_from_narrators
    if ctx.student_neighbor:
        student_name = ctx.student_neighbor.get('name_ar', '')
        candidate_name = candidate.get('name_ar', '')
        if (_name_in_list(student_name, candidate.get('narrated_from_by') or []) or
            _name_in_list(candidate_name, ctx.student_neighbor.get('narrates_from_narrators') or [])):
            score += W_TEACHER_STUDENT_MATCH
            evidence.append(f'علاقة شيخ-تلميذ مع "{student_name}"')

    # ctx.teacher_neighbor is THIS candidate's teacher.
    if ctx.teacher_neighbor:
        teacher_name = ctx.teacher_neighbor.get('name_ar', '')
        candidate_name = candidate.get('name_ar', '')
        if (_name_in_list(teacher_name, candidate.get('narrates_from_narrators') or []) or
            _name_in_list(candidate_name, ctx.teacher_neighbor.get('narrated_from_by') or [])):
            score += W_TEACHER_STUDENT_MATCH
            evidence.append(f'علاقة تلميذ-شيخ مع "{teacher_name}"')

    return score, evidence


def _check_imam_attribution(candidate: dict, ctx: ChainContext) -> Tuple[int, List[str]]:
    """Score Imam-attribution match."""
    if not ctx.imam_target:
        return 0, []
    imams = candidate.get('narrates_from_imams') or []
    companions = candidate.get('companions_of') or []
    if isinstance(companions, str):
        companions = [companions] if companions else []

    target_words = ctx.imam_target.replace('IMAM_', '').lower()
    for imam_ref in list(imams) + list(companions):
        if any(tok in _norm(imam_ref).lower() for tok in [
            'الصادق' if 'sadiq' in target_words else None,
            'الباقر' if 'baqir' in target_words else None,
            'الكاظم' if 'kadhim' in target_words else None,
            'الرضا'  if 'reza'   in target_words else None,
            'الجواد' if 'jawad'  in target_words else None,
            'الهادي' if 'hadi'   in target_words else None,
            'العسكري' if 'askari' in target_words else None,
            'السجاد' if 'sajjad' in target_words else None,
            'علي بن أبي طالب' if 'ali' in target_words else None,
        ] if tok):
            return W_IMAM_ATTRIBUTION, [f'صحبة الإمام مطابقة ({ctx.imam_target})']
    return 0, []


def _check_tabaqah_fit(candidate: dict, ctx: ChainContext) -> Tuple[int, List[str], bool]:
    """Returns (score, evidence, eliminate).

    eliminate=True means the candidate is incompatible (e.g., expected T5,
    candidate is T9). Caller should mark eliminated.
    """
    if ctx.expected_tabaqah_range is None:
        return 0, [], False
    t = candidate.get('tabaqah')
    if t is None:
        return 0, [], False
    try:
        t = int(t)
    except (TypeError, ValueError):
        return 0, [], False

    low, high = ctx.expected_tabaqah_range
    if low - 1 <= t <= high + 1:
        return W_TABAQAH_FIT, [f'الطبقة {t} مطابقة للنطاق المتوقّع [{low},{high}]'], False
    return 0, [f'الطبقة {t} خارج النطاق المتوقّع [{low},{high}]'], True


def _check_lifetime_fit(candidate: dict, ctx: ChainContext) -> Tuple[int, List[str], bool]:
    """Use the transmission validator to test feasibility against neighbors."""
    if can_transmit is None:
        return 0, [], False

    score = 0
    evidence: List[str] = []
    eliminate = False

    # Candidate must be able to transmit TO student_neighbor
    if ctx.student_neighbor:
        v = can_transmit(candidate, ctx.student_neighbor)
        if v.verdict == VERDICT_IMPOSSIBLE:
            evidence.append(f'يستحيل التلاقي مع "{ctx.student_neighbor.get("name_ar","?")}" زمنياً')
            eliminate = True
        elif v.verdict == VERDICT_TIGHT:
            evidence.append(f'تداخل ضيّق مع "{ctx.student_neighbor.get("name_ar","?")}"')
            # No score penalty for tight (it's a warning, not a disqualifier)
        else:
            score += W_LIFETIME_FIT
            evidence.append(f'العمر يسمح بالتلاقي مع "{ctx.student_neighbor.get("name_ar","?")}"')

    # Candidate must have been able to learn FROM teacher_neighbor
    if ctx.teacher_neighbor:
        v = can_transmit(ctx.teacher_neighbor, candidate)
        if v.verdict == VERDICT_IMPOSSIBLE:
            evidence.append(f'يستحيل السماع من "{ctx.teacher_neighbor.get("name_ar","?")}" زمنياً')
            eliminate = True
        elif v.verdict == VERDICT_TIGHT:
            evidence.append(f'تداخل ضيّق مع "{ctx.teacher_neighbor.get("name_ar","?")}"')
        else:
            score += W_LIFETIME_FIT
            evidence.append(f'العمر يسمح بالسماع من "{ctx.teacher_neighbor.get("name_ar","?")}"')

    return score, evidence, eliminate


def _check_book_convention(candidate: dict, kunya: str, ctx: ChainContext) -> Tuple[int, List[str]]:
    """Apply the small book/kunya overrides table."""
    if not ctx.book_id:
        return 0, []
    target = normalize_kunya(kunya)
    for (book_sub, kunya_norm), preferred_keys in BOOK_KUNYA_CONVENTIONS.items():
        if book_sub.lower() not in ctx.book_id.lower():
            continue
        if kunya_norm != target:
            continue
        cand_key = candidate.get('canonical_key') or candidate.get('_entry_idx') or ''
        if str(cand_key) in preferred_keys:
            return W_BOOK_CONVENTION, [f'تفضيل عُرفي في كتاب {ctx.book_id}']
    return 0, []


# ─── Main entry point ────────────────────────────────────────────────────────

def disambiguate(
    kunya: str,
    db: Dict[str, dict],
    context: ChainContext,
) -> Resolution:
    """Resolve `kunya` against the rijal DB given chain context.

    Returns a Resolution. If no candidates match the kunya at all, returns
    confidence='none' with top_candidate=None.
    """
    candidates = find_kunya_candidates(kunya, db)
    if not candidates:
        return Resolution(
            kunya=kunya, top_candidate=None, confidence='none', score=0,
            evidence=['لا يوجد راوٍ بهذه الكنية في قاعدة البيانات'],
        )

    scored: List[CandidateScore] = []
    eliminated_pairs: List[Tuple[str, str]] = []

    for cand in candidates:
        cs = CandidateScore(entry=cand, score=0)

        s1, e1 = _check_teacher_student_match(cand, context)
        cs.score += s1
        cs.rule_hits.extend(e1)

        s2, e2 = _check_imam_attribution(cand, context)
        cs.score += s2
        cs.rule_hits.extend(e2)

        s3, e3, kill_t = _check_tabaqah_fit(cand, context)
        cs.score += s3
        cs.rule_hits.extend(e3)
        if kill_t:
            cs.eliminated = True
            cs.elimination_reason = 'tabaqah_mismatch'

        s4, e4, kill_l = _check_lifetime_fit(cand, context)
        cs.score += s4
        cs.rule_hits.extend(e4)
        if kill_l:
            cs.eliminated = True
            cs.elimination_reason = (
                'lifetime_impossible' if cs.elimination_reason is None
                else cs.elimination_reason
            )

        s5, e5 = _check_book_convention(cand, kunya, context)
        cs.score += s5
        cs.rule_hits.extend(e5)

        scored.append(cs)

        if cs.eliminated:
            eliminated_pairs.append((cand.get('name_ar', '?'), cs.elimination_reason or ''))

    # Among non-eliminated candidates, pick the highest-scoring
    surviving = [c for c in scored if not c.eliminated]
    surviving.sort(key=lambda c: c.score, reverse=True)

    if not surviving:
        return Resolution(
            kunya=kunya, top_candidate=None, confidence='none', score=0,
            evidence=['جميع المرشحين مستبعَدون'],
            eliminated=eliminated_pairs,
            all_scores=scored,
        )

    top = surviving[0]
    runner_up = surviving[1] if len(surviving) > 1 else None

    # Determine confidence
    if top.score >= SCORE_HIGH:
        confidence = 'high'
    elif top.score >= SCORE_MEDIUM:
        confidence = 'medium'
    else:
        confidence = 'low'

    # If runner-up is too close, downgrade to ambiguous
    if runner_up and (top.score - runner_up.score) < AMBIGUOUS_GAP:
        confidence = 'ambiguous'

    return Resolution(
        kunya=kunya,
        top_candidate=top.entry,
        confidence=confidence,
        score=top.score,
        runner_up=runner_up.entry if runner_up else None,
        evidence=top.rule_hits,
        eliminated=eliminated_pairs,
        all_scores=scored,
    )


# ─── Demo ────────────────────────────────────────────────────────────────────

def _demo():
    """Synthetic check that the wiring is correct."""
    db = {
        'abu_basir_layth': {
            'canonical_key': 'abu_basir_layth',
            'name_ar': 'ليث بن البختري المرادي',
            'kunyah': 'أبو بصير',
            'tabaqah': 5,
            'narrates_from_imams': ['الصادق'],
            'narrates_from_narrators': [],
            'narrated_from_by': ['عاصم بن حُميد'],
            'birth_year_hijri': 100,
            'death_year_hijri': 148,
        },
        'abu_basir_yahya': {
            'canonical_key': 'abu_basir_yahya',
            'name_ar': 'يحيى بن أبي القاسم الأسدي',
            'kunyah': 'أبو بصير',
            'tabaqah': 5,
            'narrates_from_imams': ['الباقر', 'الصادق'],
            'narrates_from_narrators': [],
            'narrated_from_by': ['عاصم بن حُميد', 'شعيب العقرقوفي'],
            'birth_year_hijri': 80,
            'death_year_hijri': 150,
        },
        'asim_humayd': {
            'canonical_key': 'asim_humayd',
            'name_ar': 'عاصم بن حُميد',
            'kunyah': None,
            'tabaqah': 5,
            'narrates_from_narrators': ['ليث بن البختري المرادي', 'يحيى بن أبي القاسم الأسدي'],
        },
    }
    ctx = ChainContext(
        student_neighbor=db['asim_humayd'],
        imam_target='IMAM_SADIQ',
        expected_tabaqah_range=(5, 5),
        book_id='Al-Kafi-Volume-1-Kulayni',
    )
    r = disambiguate('أبو بصير', db, ctx)
    print(f"\nDemo: disambiguate('أبو بصير') with student=ʿĀṣim b. Ḥumayd")
    print(f"  top: {r.top_candidate.get('name_ar') if r.top_candidate else None}")
    print(f"  confidence: {r.confidence}, score: {r.score}")
    if r.runner_up:
        print(f"  runner-up: {r.runner_up.get('name_ar')}")
    for e in r.evidence:
        print(f"    • {e}")


if __name__ == '__main__':
    _demo()

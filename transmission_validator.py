#!/usr/bin/env python3
"""
Transmission Validator — Phase 3
=================================

Answers: "Could narrator A have actually transmitted hadith to narrator B?"

Uses lifetime data written by `rijal_lifetime_extractor.py`. If lifetime data
is missing, falls back to ṭabaqah-window inheritance via `BURUJIRDI_AH_RANGES`.

Verdicts (ordered most → least confident):
    certain      teacher's lifetime cleanly contains a window where student
                 was at least 10 years old (old enough for samāʿ)
    possible     lifetimes overlap by at least 1 year, but tightly
    tight        windows touch within ±5 years; suspicious, surface for review
    impossible   teacher died before student was born, or student born after
                 teacher's known death year
    unknown      either narrator lacks lifetime data and lacks a tabaqah window

A `tadlīs` flag is raised when:
  - Narrator A claims direct transmission from B (e.g., حدثني), but
  - can_transmit(B → A) is `tight` or `impossible`, OR
  - A has documented intermediary teachers (in narrates_from_narrators) that
    A skipped over to claim B directly.

Public API:
    can_transmit(teacher, student) -> Verdict
    detect_tadlis_in_chain(chain_entries) -> List[TadlisFlag]
    annotate_chain(chain_entries) -> List[dict]  # adds .transmission per link
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from tabaqah_inference import BURUJIRDI_AH_RANGES

# Minimum age at which a student can plausibly receive (samāʿ) hadith.
# Classical view: ~5 for huḍūr/qirāʾah, ~10 for samāʿ in formal sense, but
# scholars accept hearing as young as ~5–7. We use 10 as the "comfortable"
# threshold and treat 5–10 as tight.
MIN_AGE_FOR_SAMA       = 10
MIN_AGE_FOR_HUDUR      = 5

# How many years of overlap qualifies a transmission as "certain" rather than
# merely "possible". Less than this and we mark it `possible` to flag the
# brief contact window for scholar review.
COMFORTABLE_OVERLAP    = 10

# Tightness window: lifetimes that touch within ±5 years are "tight" — a
# scholar should look at this case explicitly.
TIGHT_GAP_YEARS        = 5


@dataclass
class Lifetime:
    """Resolved lifetime for one narrator. All years are AH (hijri)."""
    birth_low:  Optional[int]   # earliest plausible birth year
    birth_high: Optional[int]   # latest   plausible birth year
    death_low:  Optional[int]   # earliest plausible death year
    death_high: Optional[int]   # latest   plausible death year
    source:     str             # "explicit" | "explicit+window" | "tabaqah_window" | "unknown"


def lifetime_from_entry(entry: dict) -> Lifetime:
    """Pull a Lifetime from the schema written by rijal_lifetime_extractor."""
    if entry is None:
        return Lifetime(None, None, None, None, 'unknown')

    birth = entry.get('birth_year_hijri')
    death = entry.get('death_year_hijri')
    bw = entry.get('birth_year_window')   # [low, high] | None
    dw = entry.get('death_year_window')

    has_explicit_birth = birth is not None
    has_explicit_death = death is not None
    has_window_birth   = bool(bw)
    has_window_death   = bool(dw)

    if has_explicit_birth:
        b_low = b_high = int(birth)
    elif has_window_birth:
        b_low, b_high = int(bw[0]), int(bw[1])
    else:
        b_low = b_high = None

    if has_explicit_death:
        d_low = d_high = int(death)
    elif has_window_death:
        d_low, d_high = int(dw[0]), int(dw[1])
    else:
        d_low = d_high = None

    if has_explicit_birth and has_explicit_death:
        source = 'explicit'
    elif has_explicit_birth or has_explicit_death:
        source = 'explicit+window'
    elif has_window_birth or has_window_death:
        source = 'tabaqah_window'
    else:
        source = 'unknown'

    return Lifetime(b_low, b_high, d_low, d_high, source)


# ─── Verdict types ────────────────────────────────────────────────────────────

VERDICT_CERTAIN    = 'certain'
VERDICT_POSSIBLE   = 'possible'
VERDICT_TIGHT      = 'tight'
VERDICT_IMPOSSIBLE = 'impossible'
VERDICT_UNKNOWN    = 'unknown'


@dataclass
class TransmissionVerdict:
    verdict:    str        # one of the VERDICT_* constants
    reason:     str        # human-readable Arabic+English explanation
    teacher_lt: Lifetime
    student_lt: Lifetime
    overlap_years: Optional[int] = None   # how many years they could have overlapped


def can_transmit(teacher: dict, student: dict) -> TransmissionVerdict:
    """Could `teacher` plausibly have transmitted to `student`?

    Both args are rijal DB entries (the dicts you'd get from DatabaseLoader).
    """
    t = lifetime_from_entry(teacher)
    s = lifetime_from_entry(student)

    if t.source == 'unknown' or s.source == 'unknown':
        return TransmissionVerdict(
            verdict=VERDICT_UNKNOWN,
            reason='لا توجد بيانات حياة كافية لأحد الراويين',
            teacher_lt=t, student_lt=s,
        )

    # Use the most pessimistic interpretation of windows for "impossible":
    # teacher died at the EARLIEST plausible date, student born at the LATEST.
    # If even then the student is too young, transmission is genuinely tight.
    teacher_died_earliest = t.death_low
    student_born_latest   = s.birth_high

    # And the most optimistic interpretation: teacher died at the LATEST date,
    # student born at the EARLIEST. This gives the maximum possible overlap.
    teacher_died_latest = t.death_high
    student_born_earliest = s.birth_low

    if teacher_died_latest is None or student_born_earliest is None:
        return TransmissionVerdict(
            verdict=VERDICT_UNKNOWN,
            reason='تواريخ الميلاد/الوفاة المتاحة غير كافية للحكم',
            teacher_lt=t, student_lt=s,
        )

    # The student was alive (and old enough) during what fraction of the teacher's life?
    # Student's earliest "ready for samāʿ" year:
    student_ready_earliest = student_born_earliest + MIN_AGE_FOR_SAMA

    # Optimistic overlap: window between (teacher_died_latest) and
    # (student_ready_earliest).
    optimistic_overlap = teacher_died_latest - student_ready_earliest

    # Pessimistic gap: how late did the student become ready vs how early did
    # the teacher die?
    if teacher_died_earliest is not None and student_born_latest is not None:
        pessimistic_gap = (student_born_latest + MIN_AGE_FOR_HUDUR) - teacher_died_earliest
    else:
        pessimistic_gap = None

    # Hard impossibility: even at the most generous interpretation, the
    # student's earliest possible birth is AFTER the teacher's latest possible
    # death.
    if student_born_earliest > teacher_died_latest:
        return TransmissionVerdict(
            verdict=VERDICT_IMPOSSIBLE,
            reason=(
                f'الطالب وُلد سنة ≥ {student_born_earliest} هـ، '
                f'والشيخ توفّي سنة ≤ {teacher_died_latest} هـ — '
                f'لا تلاقي ممكن.'
            ),
            teacher_lt=t, student_lt=s,
            overlap_years=optimistic_overlap,
        )

    # Tight: even the optimistic overlap is shorter than samāʿ readiness margin
    if optimistic_overlap < MIN_AGE_FOR_HUDUR:
        return TransmissionVerdict(
            verdict=VERDICT_IMPOSSIBLE,
            reason=(
                f'تداخل العمر الأقصى = {optimistic_overlap} سنة فقط — '
                f'الطالب لم يبلغ السنّ الذي يصحّ معه السماع.'
            ),
            teacher_lt=t, student_lt=s,
            overlap_years=optimistic_overlap,
        )

    # Tight: optimistic overlap is between MIN_AGE_FOR_HUDUR and TIGHT_GAP_YEARS
    if optimistic_overlap < TIGHT_GAP_YEARS:
        return TransmissionVerdict(
            verdict=VERDICT_TIGHT,
            reason=(
                f'تداخل قصير ({optimistic_overlap} سنة) — احتمال السماع ضعيف؛ '
                f'يحتاج إلى تحقيق.'
            ),
            teacher_lt=t, student_lt=s,
            overlap_years=optimistic_overlap,
        )

    # Possible: meaningful overlap but not generous
    if optimistic_overlap < COMFORTABLE_OVERLAP:
        return TransmissionVerdict(
            verdict=VERDICT_POSSIBLE,
            reason=(
                f'تداخل ممكن ({optimistic_overlap} سنة) — السماع ممكن لكن '
                f'النافذة الزمنية محدودة.'
            ),
            teacher_lt=t, student_lt=s,
            overlap_years=optimistic_overlap,
        )

    # Certain
    return TransmissionVerdict(
        verdict=VERDICT_CERTAIN,
        reason=(
            f'تداخل واسع (≥ {optimistic_overlap} سنة) — التلاقي والسماع مؤكدان زمنياً.'
        ),
        teacher_lt=t, student_lt=s,
        overlap_years=optimistic_overlap,
    )


# ─── Tadlīs detection ─────────────────────────────────────────────────────────

@dataclass
class TadlisFlag:
    position: int                 # index in the chain where suspicion arises
    teacher_name: str
    student_name: str
    type: str                     # "lifetime_gap" | "skipped_intermediary"
    severity: str                 # "high" | "medium" | "low"
    evidence: str                 # human-readable explanation
    extra: dict = field(default_factory=dict)


def detect_tadlis_in_chain(chain_entries: List[dict]) -> List[TadlisFlag]:
    """Scan a resolved chain for tadlīs signals.

    `chain_entries` is the list produced by the resolver, in transmission order
    (i.e., `chain_entries[0]` heard from `chain_entries[1]`, etc.). Each entry
    is the rijal DB dict for that narrator, or None if unresolved.
    """
    flags: List[TadlisFlag] = []
    if not chain_entries or len(chain_entries) < 2:
        return flags

    for i in range(len(chain_entries) - 1):
        student = chain_entries[i]
        teacher = chain_entries[i + 1]
        if student is None or teacher is None:
            continue

        s_name = student.get('name_ar', '?')
        t_name = teacher.get('name_ar', '?')

        # 1) Lifetime-gap tadlīs: claimed direct transmission, but they barely
        #    overlapped or didn't.
        verdict = can_transmit(teacher, student)
        if verdict.verdict == VERDICT_TIGHT:
            flags.append(TadlisFlag(
                position=i,
                teacher_name=t_name,
                student_name=s_name,
                type='lifetime_gap',
                severity='medium',
                evidence=verdict.reason,
                extra={'overlap_years': verdict.overlap_years},
            ))
        elif verdict.verdict == VERDICT_IMPOSSIBLE:
            flags.append(TadlisFlag(
                position=i,
                teacher_name=t_name,
                student_name=s_name,
                type='lifetime_gap',
                severity='high',
                evidence=verdict.reason,
                extra={'overlap_years': verdict.overlap_years},
            ))

        # 2) Skipped-intermediary tadlīs: the student documents one or more
        #    teachers (narrates_from_narrators) and the chain claims direct
        #    transmission from someone NOT in that list. This is weak signal
        #    on its own — only flagged if combined with a tight lifetime.
        documented_teachers = student.get('narrates_from_narrators') or []
        if documented_teachers and verdict.verdict in (VERDICT_TIGHT, VERDICT_POSSIBLE):
            # Quick name-substring check: is t_name (or its key tokens) in the
            # documented list? If not, the chain may be skipping a known
            # intermediary.
            t_tokens = set(t_name.split())
            documented = False
            for dt in documented_teachers:
                if any(tok in dt for tok in t_tokens if len(tok) >= 3):
                    documented = True
                    break
            if not documented:
                flags.append(TadlisFlag(
                    position=i,
                    teacher_name=t_name,
                    student_name=s_name,
                    type='skipped_intermediary',
                    severity='low',
                    evidence=(
                        f'الراوي "{s_name}" يوثَّق له شيوخ معروفون '
                        f'(عددهم {len(documented_teachers)})، '
                        f'وليس "{t_name}" منهم — احتمال إسقاط واسطة.'
                    ),
                    extra={'documented_teachers': documented_teachers[:5]},
                ))

    return flags


# ─── Chain-level annotation helper ────────────────────────────────────────────

def annotate_chain(chain_entries: List[Optional[dict]]) -> List[dict]:
    """Walk a chain and annotate each transmission link with its verdict.

    Returns a list of dicts: one per link (i.e., len(chain) - 1 dicts), each
    with: {position, student_name, teacher_name, verdict, reason, overlap_years}.

    Designed to be embedded in IsnadAnalyzer's output between resolution and
    grading. Unresolved narrators yield a `verdict='unresolved'` entry instead
    of a TransmissionVerdict.
    """
    out: List[dict] = []
    if not chain_entries or len(chain_entries) < 2:
        return out

    for i in range(len(chain_entries) - 1):
        student = chain_entries[i]
        teacher = chain_entries[i + 1]

        if student is None or teacher is None:
            out.append({
                'position':     i,
                'student_name': (student or {}).get('name_ar') or '<unresolved>',
                'teacher_name': (teacher or {}).get('name_ar') or '<unresolved>',
                'verdict':      'unresolved',
                'reason':       'لا يمكن التحقق: أحد الراويين غير محدّد',
                'overlap_years': None,
            })
            continue

        v = can_transmit(teacher, student)
        out.append({
            'position':     i,
            'student_name': student.get('name_ar', '?'),
            'teacher_name': teacher.get('name_ar', '?'),
            'verdict':      v.verdict,
            'reason':       v.reason,
            'overlap_years': v.overlap_years,
            'teacher_lifetime_source': v.teacher_lt.source,
            'student_lifetime_source': v.student_lt.source,
        })

    return out


# ─── CLI smoke test ──────────────────────────────────────────────────────────

def _demo():
    """Quick visual check using two synthetic narrators."""
    teacher = {
        'name_ar': 'الإمام جعفر الصادق',
        'birth_year_hijri': 83,
        'death_year_hijri': 148,
    }
    student_old = {
        'name_ar': 'زرارة بن أعين',
        'birth_year_hijri': 80,
        'death_year_hijri': 150,
    }
    student_too_young = {
        'name_ar': 'مولود تجريبي',
        'birth_year_hijri': 200,
        'death_year_hijri': 280,
    }

    print("Demo: can_transmit(al-Sadiq → Zurara)")
    v = can_transmit(teacher, student_old)
    print(f"  verdict: {v.verdict}\n  reason:  {v.reason}\n  overlap: {v.overlap_years}")

    print("\nDemo: can_transmit(al-Sadiq → impossible-student)")
    v = can_transmit(teacher, student_too_young)
    print(f"  verdict: {v.verdict}\n  reason:  {v.reason}")

    print("\nDemo: chain annotation")
    chain = [student_old, teacher]
    for link in annotate_chain(chain):
        print(f"  [{link['position']}] {link['student_name']} ← {link['teacher_name']}: "
              f"{link['verdict']}")


if __name__ == '__main__':
    _demo()

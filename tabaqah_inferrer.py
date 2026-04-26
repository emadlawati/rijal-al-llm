#!/usr/bin/env python3
"""
tabaqah_inferrer.py
===================
Infers tabaqah (generational layer) for narrators NOT covered by
alf_rajul_extractor.py, using three tiers in order of confidence:

  Tier 1 — Imam connection (HIGH confidence)
    If a narrator directly narrates from/to a known Imam whose tabaqah
    is fixed, their tabaqah = imam_tabaqah ± 1.

  Tier 2 — Network neighbours (MEDIUM confidence)
    Propagate tabaqah through the narration graph: if most of a
    narrator's teachers are tabaqah X, they are likely tabaqah X+1.
    Runs iteratively until no more assignments can be made.

  Tier 3 — LLM inference (LOW-MEDIUM confidence, optional)
    Send _raw_full to DeepSeek and ask it to determine the tabaqah.
    Only runs for entries that Tiers 1 & 2 couldn't resolve.

Run:
  python tabaqah_inferrer.py --stats          # show coverage before/after
  python tabaqah_inferrer.py --tier 1         # only Imam-based
  python tabaqah_inferrer.py --tier 2         # Imam + network
  python tabaqah_inferrer.py --tier 3         # all tiers (includes LLM)
  python tabaqah_inferrer.py --dry-run        # compute but don't save
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

RIJAL_DB  = Path("rijal_database.json")
PROGRESS  = Path("tabaqah_infer_progress.json")

DRY_RUN   = "--dry-run" in sys.argv
STATS     = "--stats"   in sys.argv
MAX_TIER  = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--tier"), 3))
BACKEND   = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--backend"), "deepseek")
WORKERS   = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--workers"), 5))

# ─── Known Imam tabaqat ───────────────────────────────────────────────────────
# Each Imam has a known tabaqah. Narrators who directly narrate FROM an Imam
# are in that Imam's tabaqah (same generation or one later).
IMAM_TABAQAH: dict[str, int] = {
    # Tabaqah 1
    'النبي': 1, 'رسول الله': 1, 'محمد بن عبد الله': 1,
    # Tabaqah 2
    'أمير المؤمنين': 2, 'علي بن أبي طالب': 2, 'الحسن بن علي': 2, 'الحسين بن علي': 2,
    # Tabaqah 3
    'السجاد': 3, 'زين العابدين': 3, 'علي بن الحسين': 3,
    # Tabaqah 4
    'الباقر': 4, 'أبي جعفر': 4, 'محمد بن علي الباقر': 4,
    # Tabaqah 5
    'الصادق': 5, 'أبي عبد الله': 5, 'جعفر بن محمد': 5,
    # Tabaqah 6
    'الكاظم': 6, 'أبي الحسن الأول': 6, 'موسى بن جعفر': 6,
    # Tabaqah 7
    'الرضا': 7, 'أبي الحسن الثاني': 7, 'علي بن موسى': 7,
    'الجواد': 7, 'أبي جعفر الثاني': 7, 'محمد بن علي الجواد': 7,
    # Tabaqah 8
    'الهادي': 8, 'أبي الحسن الثالث': 8, 'علي بن محمد': 8,
    'العسكري': 8, 'أبي محمد': 8, 'الحسن بن علي العسكري': 8,
    # Tabaqah 9
    'المهدي': 9, 'القائم': 9, 'صاحب الأمر': 9, 'الحجة': 9,
}

_DIAC = re.compile(r'[\u064b-\u065f\u0670]')
_ALEF = re.compile(r'[إأآا]')

def norm(t: str) -> str:
    t = _DIAC.sub('', t)
    t = _ALEF.sub('ا', t)
    return re.sub(r'\s+', ' ', t.replace('ـ', '')).strip()

def imam_tabaqah_from_name(name: str) -> int | None:
    n = norm(name)
    # Sort longest title first — prevents "الحسن بن علي" (T2) from matching
    # as a substring inside "الحسن بن علي العسكري" (T8)
    for title, t in sorted(IMAM_TABAQAH.items(), key=lambda x: -len(x[0])):
        if norm(title) in n:
            return t
    return None

# ─── Tier 1: Imam-based inference ────────────────────────────────────────────

def tier1_imam(db: dict) -> int:
    """
    Assign tabaqah to narrators who directly narrate from a known Imam.
    A narrator who narrates FROM Imam of tabaqah T is assigned tabaqah T.
    (They are contemporaries — the narrator's tabaqah = the Imam's tabaqah.)
    Returns count of new assignments.
    """
    assigned = 0
    for entry in db.values():
        if entry.get('tabaqah') is not None:
            continue

        imam_tabs: list[int] = []

        # Check narrates_from_imams field (structured — most reliable)
        for imam_name in (entry.get('narrates_from_imams') or []):
            t = imam_tabaqah_from_name(imam_name)
            if t:
                imam_tabs.append(t)

        # Check companions_of field (structured — equally reliable)
        # Iterate each name individually so all votes feed the median
        companions_raw = entry.get('companions_of') or []
        if isinstance(companions_raw, str):
            companions_raw = [companions_raw] if companions_raw else []
        for comp_name in companions_raw:
            t = imam_tabaqah_from_name(comp_name)
            if t:
                imam_tabs.append(t)

        # NOTE: raw text scan deliberately removed — it causes false positives
        # when a narrator's own name contains an imam's name as a substring
        # (e.g. "علي بن الحسن بن علي بن فضال" wrongly matches "الحسن بن علي" = T2).
        # The structured fields above are sufficient and accurate.

        if not imam_tabs:
            continue

        # Use the median to be robust against one-off outliers
        imam_tabs.sort()
        inferred = imam_tabs[len(imam_tabs) // 2]

        if not DRY_RUN:
            entry['tabaqah']        = inferred
            entry['tabaqah_detail'] = f'الطبقة {inferred} (مستنتج من رواية عن الإمام)'
            entry['tabaqah_sub']    = None
            entry['tabaqah_source'] = 'inferred_imam'
            entry['tabaqah_confidence'] = 'high'
        assigned += 1

    return assigned

# ─── Tier 2: Network propagation ─────────────────────────────────────────────

def build_name_to_entry(db: dict) -> dict[str, str]:
    """Map normalised 2-word name key → db_key."""
    idx: dict[str, list[str]] = defaultdict(list)
    for db_key, entry in db.items():
        n = entry.get('name_ar', '')
        if n:
            k = ' '.join(norm(n).split()[:2])
            idx[k].append(db_key)
    # Only unambiguous mappings
    return {k: v[0] for k, v in idx.items() if len(v) == 1}

def tier2_network(db: dict, max_rounds: int = 10) -> int:
    """
    Propagate tabaqah through the narration graph iteratively.

    Logic:
      - If narrator A has known tabaqah T, and A is in someone's
        narrates_from_narrators, that someone is likely tabaqah T+1.
      - If narrator A has known tabaqah T, and A is in someone's
        narrated_from_by, that someone is likely tabaqah T-1.

    We run multiple rounds until convergence.
    Returns total count of new assignments.
    """
    name_idx = build_name_to_entry(db)
    total_assigned = 0

    for round_num in range(1, max_rounds + 1):
        assigned_this_round = 0

        for db_key, entry in db.items():
            if entry.get('tabaqah') is not None:
                continue

            teacher_tabs: list[int] = []   # people this narrator learned FROM
            student_tabs: list[int] = []   # people who learned FROM this narrator

            # Teachers → this narrator is one tabaqah AFTER them
            for teacher_name in (entry.get('narrates_from_narrators') or []):
                k = ' '.join(norm(teacher_name).split()[:2])
                teacher_key = name_idx.get(k)
                if teacher_key and db.get(teacher_key, {}).get('tabaqah'):
                    teacher_tabs.append(db[teacher_key]['tabaqah'])

            # Students → this narrator is one tabaqah BEFORE them
            for student_name in (entry.get('narrated_from_by') or []):
                k = ' '.join(norm(student_name).split()[:2])
                student_key = name_idx.get(k)
                if student_key and db.get(student_key, {}).get('tabaqah'):
                    student_tabs.append(db[student_key]['tabaqah'])

            votes: list[int] = []
            if teacher_tabs:
                # Narrator is one generation after their teachers
                votes.extend(t + 1 for t in teacher_tabs)
            if student_tabs:
                # Narrator is one generation before their students
                votes.extend(t - 1 for t in student_tabs)

            if not votes:
                continue

            # Infer as median, clamp to [1, 12]
            votes.sort()
            inferred = max(1, min(12, votes[len(votes) // 2]))

            # Require at least 2 supporting votes for confidence
            confidence = 'medium' if len(votes) >= 2 else 'low'

            if not DRY_RUN:
                entry['tabaqah']        = inferred
                entry['tabaqah_detail'] = f'الطبقة {inferred} (مستنتج من الشبكة)'
                entry['tabaqah_sub']    = None
                entry['tabaqah_source'] = 'inferred_network'
                entry['tabaqah_confidence'] = confidence
            assigned_this_round += 1

        total_assigned += assigned_this_round
        print(f"  Round {round_num}: assigned {assigned_this_round:,} new tabaqat", flush=True)
        if assigned_this_round == 0:
            break   # converged

    return total_assigned

# ─── Tier 3: LLM inference ───────────────────────────────────────────────────

TABAQAH_SYSTEM_PROMPT = """\
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

أعِد JSON فقط بلا أي كلام:
{
  "tabaqah": <رقم ١–١٢ أو null إن لم تتمكن من التحديد>,
  "tabaqah_detail": "<وصف موجز كـ 'من أصحاب الإمام الصادق (ع)' أو null>",
  "reasoning": "<جملة واحدة تشرح أساس التحديد>"
}
"""

def call_deepseek_tabaqah(prompt: str) -> dict | None:
    import requests
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not set")
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": TABAQAH_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 256,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    while True:
        r = requests.post("https://api.deepseek.com/chat/completions",
                          json=payload, headers=headers, timeout=60)
        if r.status_code == 429:
            print("  [rate limit — sleeping 20s]", flush=True)
            time.sleep(20)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"API error {r.status_code}: {r.text}")
        break
    text = r.json()["choices"][0]["message"]["content"]
    # Strip reasoning tags
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'<reasoning>.*?</reasoning>', '', text, flags=re.DOTALL)
    text = re.sub(r'```(?:json)?\s*', '', text).strip()
    m = re.search(r'\{[\s\S]*\}', text)
    if not m:
        return None
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return None


def tier3_llm(db: dict) -> int:
    """
    LLM inference for remaining unassigned entries that have _raw_full.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    candidates = [
        (db_key, entry) for db_key, entry in db.items()
        if entry.get('tabaqah') is None and entry.get('_raw_full')
    ]
    print(f"  LLM queue: {len(candidates):,} entries", flush=True)

    done_set: set[str] = set()
    if PROGRESS.exists():
        prog = json.loads(PROGRESS.read_text(encoding='utf-8'))
        done_set = set(prog.get('done', []))
        print(f"  Already done: {len(done_set):,}", flush=True)

    candidates = [(k, e) for k, e in candidates if k not in done_set]

    assigned   = 0
    lock       = threading.Lock()
    count      = [0]

    def process(item):
        db_key, entry = item
        name  = entry.get('name_ar', '?')
        raw   = entry.get('_raw_full', '')[:3000]
        prompt = f"الراوي: {name}\n\nالنص:\n{raw}"
        try:
            result = call_deepseek_tabaqah(prompt)
        except Exception as e:
            return db_key, None, str(e)
        return db_key, result, None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process, item): item for item in candidates}
        for future in as_completed(futures):
            db_key, result, error = future.result()
            entry = db[db_key]

            with lock:
                count[0] += 1
                n = count[0]
                done_set.add(db_key)

                if error:
                    print(f"  [ERROR] {db_key}: {error}", flush=True)
                elif result and result.get('tabaqah'):
                    t = result['tabaqah']
                    if isinstance(t, int) and 1 <= t <= 12:
                        if not DRY_RUN:
                            entry['tabaqah']         = t
                            entry['tabaqah_detail']  = result.get('tabaqah_detail', f'الطبقة {t}')
                            entry['tabaqah_source']  = 'inferred_llm'
                            entry['tabaqah_confidence'] = 'medium'
                        assigned += 1

                if n % 100 == 0:
                    print(f"  [{n:,}/{len(candidates):,}]  assigned so far: {assigned:,}", flush=True)
                    # Save progress
                    if not DRY_RUN:
                        PROGRESS.write_text(
                            json.dumps({'done': sorted(done_set)}),
                            encoding='utf-8'
                        )

    if not DRY_RUN:
        PROGRESS.write_text(
            json.dumps({'done': sorted(done_set)}),
            encoding='utf-8'
        )

    return assigned

# ─── Main ─────────────────────────────────────────────────────────────────────

def print_coverage(db: dict, label: str):
    total      = len(db)
    with_tab   = sum(1 for e in db.values() if e.get('tabaqah') is not None)
    by_source: dict[str, int] = defaultdict(int)
    for e in db.values():
        src = e.get('tabaqah_source', 'none')
        if e.get('tabaqah') is not None:
            by_source[src] += 1
    print(f"\n{label}")
    print(f"  Total entries:    {total:,}")
    print(f"  With tabaqah:     {with_tab:,}  ({with_tab/total*100:.1f}%)")
    for src, cnt in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {src:<25}: {cnt:,}")


def main():
    print(f"Loading {RIJAL_DB} …")
    db: dict = json.loads(RIJAL_DB.read_text(encoding='utf-8'))

    print_coverage(db, "BEFORE inference")

    if STATS:
        return

    total_added = 0

    if MAX_TIER >= 1:
        print("\nTier 1: Imam-based inference …")
        n = tier1_imam(db)
        print(f"  Assigned: {n:,}")
        total_added += n

    if MAX_TIER >= 2:
        print("\nTier 2: Network propagation …")
        n = tier2_network(db)
        print(f"  Assigned (total): {n:,}")
        total_added += n

    if MAX_TIER >= 3:
        print("\nTier 3: LLM inference …")
        n = tier3_llm(db)
        print(f"  Assigned: {n:,}")
        total_added += n

    print_coverage(db, "AFTER inference")
    print(f"\nTotal newly assigned: {total_added:,}")

    if DRY_RUN:
        print("\n[--dry-run: not saved]")
        return

    print(f"\nSaving {RIJAL_DB} …")
    RIJAL_DB.write_text(
        json.dumps(db, ensure_ascii=False, indent=2),
        encoding='utf-8'
    )
    print(f"Done.  {RIJAL_DB.stat().st_size / 1e6:.1f} MB")


if __name__ == '__main__':
    main()

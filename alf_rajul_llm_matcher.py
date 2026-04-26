#!/usr/bin/env python3
"""
Alf-Rajul → Rijal LLM Disambiguator
====================================

Matches unmatched alf_rajul entries (those listed in
`alf_rajul_match_audit.json`) to entries in `rijal_database_merged.json` using
an LLM, then writes the results into `alf_rajul_disambiguation_llm.json` —
which `tabaqah_inference.seed_from_alf_rajul` already reads as
manual-disambiguation overrides.

Pipeline:
  1. For each unmatched alf_rajul entry, find rijal candidates by loose
     token-overlap matching, filtered to a tabaqah window (alf_t ± 2).
  2. If 0 candidates → record as null (this narrator doesn't appear in the
     rijal DB at all).
  3. If 1 candidate → take it directly (no LLM call needed).
  4. If 2–12 candidates → ask the LLM to pick one. The prompt includes the
     alf_rajul biographical text plus a structured summary of each candidate.
  5. After all picks, the next `python tabaqah_inference.py --apply --rebuild`
     run automatically picks them up via Tier 0 disambiguation.

CLI:
    python alf_rajul_llm_matcher.py --stats           # candidate distribution
    python alf_rajul_llm_matcher.py --dry-run         # show prompts, don't call LLM
    python alf_rajul_llm_matcher.py --max 5           # process 5 entries (test)
    python alf_rajul_llm_matcher.py                   # full run
    python alf_rajul_llm_matcher.py --backend claude  # use Claude instead of DeepSeek

Environment variables:
    DEEPSEEK_API_KEY   for --backend deepseek (default)
    ANTHROPIC_API_KEY  for --backend claude
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from tabaqah_inference import (
    _norm,
    ARABIC_IMAM_TABAQAH,
    imam_tabaqah_from_name,
)

# Reuse the systematic OCR corrections from alf_rajul_extractor: the alf_rajul
# OCR routinely renders م as ه in certain words (إسماعيل → إسهاعيل, سماعة →
# سهاعة, سليمان → سليهان, اليماني → اليهاني, etc.). Without applying these,
# many famous narrators fail to match because the rijal DB has the corrected
# spelling but the alf bio has the OCR'd form.
try:
    from alf_rajul_extractor import WORD_CORRECTIONS, ocr_variants
    HAS_OCR_CORRECTIONS = True
except ImportError:
    WORD_CORRECTIONS = {}
    HAS_OCR_CORRECTIONS = False
    def ocr_variants(name: str) -> List[str]:
        return []

# ─── Paths ───────────────────────────────────────────────────────────────────

SCRIPT_DIR        = Path(__file__).resolve().parent
RIJAL_DB          = SCRIPT_DIR / "rijal_database_merged.json"
ALF_RAJUL_DB      = SCRIPT_DIR / "alf_rajul_database.json"
AUDIT_FILE        = SCRIPT_DIR / "alf_rajul_match_audit.json"
DISAMB_FILE       = SCRIPT_DIR / "alf_rajul_disambiguation_llm.json"
PROGRESS_FILE     = SCRIPT_DIR / "alf_rajul_llm_matcher_progress.json"

# ─── Tunables ────────────────────────────────────────────────────────────────

# How wide a tabaqah window around the alf_rajul tabaqah to admit candidates.
# alf says T5 → admit rijal entries with tabaqah in [T2, T8] (and entries
# without tabaqah, since they might still be the right person). Wider than
# the previous ±2 because the rijal entry's tabaqah may itself be inferred
# (and possibly off by a few generations).
TABAQAH_WINDOW = 3

# Cap candidates per entry. Increased from 12 to 20 so the LLM has more
# room when there are many same-named figures (إبراهيم, أحمد, محمد).
MAX_CANDIDATES = 20

# Minimum token-overlap score to consider a candidate. Lowered from 2 to 1
# because a single rare token (e.g., a kunya like "السكوني" or a nisba like
# "البلدي") is often enough — and pairing with the OCR-corrected variant
# tends to recover most missed matches.
MIN_OVERLAP_SCORE = 1

# Common particles that should NOT count as significant tokens.
# Note: we keep `عبد` IN the significant set because it's a discriminating
# token in compound names like `عبد الله`, `عبد الرحمن`, `عبد الصمد`.
INSIGNIFICANT_TOKENS = {
    'بن', 'ابن', 'ابو', 'ابي', 'ام', 'الى', 'عن',
    'ال', 'في', 'من', 'بنت',
}


# ─── Token utilities ─────────────────────────────────────────────────────────

def _apply_word_corrections(name: str) -> str:
    """Replace known OCR errors (e.g. إسهاعيل → إسماعيل) within a name."""
    if not WORD_CORRECTIONS:
        return name
    out = name
    for wrong, right in WORD_CORRECTIONS.items():
        out = out.replace(wrong, right)
    return out


def _significant_tokens(name: str) -> Set[str]:
    """Tokens of length ≥ 3 that aren't common particles. Applies OCR
    corrections so alf names and rijal names land on the same tokens."""
    corrected = _apply_word_corrections(name)
    tokens = _norm(corrected).split()
    return {
        t for t in tokens
        if len(t) >= 3 and t not in INSIGNIFICANT_TOKENS
    }


def _overlap_score(alf_name: str, rijal_name: str) -> int:
    """Count of significant tokens shared between two names.

    Both names are OCR-corrected before tokenization, so إسهاعيل in the alf
    bio and إسماعيل in the rijal entry land on the same token.
    """
    a = _significant_tokens(alf_name)
    b = _significant_tokens(rijal_name)
    return len(a & b)


# ─── Candidate finding ───────────────────────────────────────────────────────

def find_candidates(
    alf_entry: dict,
    rijal_db: Dict[str, dict],
    rijal_index: Optional[Dict[str, List[str]]] = None,
) -> List[Tuple[str, dict, int]]:
    """For one alf_rajul entry, return ranked rijal candidates.

    Each item: (rijal_db_key, rijal_entry, overlap_score). Sorted by score
    descending. Filtered by tabaqah window. Capped at MAX_CANDIDATES.
    """
    alf_name = alf_entry.get('name_ar', '')
    alf_alts = alf_entry.get('alt_names') or []
    alf_t = alf_entry.get('tabaqah')

    # Build a search corpus: alf main name + alt names
    search_corpus = [alf_name] + alf_alts

    scored: List[Tuple[str, dict, int]] = []

    for k, e in rijal_db.items():
        # Skip alias entries — match against canonical entries only. The
        # `seed_from_alf_rajul` flow already canonicalizes the chosen target,
        # so we save work and noise by not surfacing aliases as candidates.
        canon = e.get('canonical_entry')
        if canon and canon != e.get('_entry_idx'):
            continue

        # Tabaqah-window filter
        rijal_t = e.get('tabaqah')
        if alf_t is not None and rijal_t is not None:
            try:
                if abs(int(rijal_t) - int(alf_t)) > TABAQAH_WINDOW:
                    continue
            except (TypeError, ValueError):
                pass

        # Compute overlap against the best of (main name, aliases)
        rijal_names = [e.get('name_ar', '')] + (e.get('aliases') or [])
        best_score = 0
        for an in search_corpus:
            for rn in rijal_names:
                s = _overlap_score(an, rn)
                if s > best_score:
                    best_score = s

        if best_score >= MIN_OVERLAP_SCORE:
            scored.append((k, e, best_score))

    scored.sort(key=lambda x: -x[2])
    return scored[:MAX_CANDIDATES]


# ─── LLM prompt construction ─────────────────────────────────────────────────

SYSTEM_PROMPT = """\
أنت خبير في علم رجال الحديث الشيعي. ستُعطى ترجمة راوٍ من معجم الألف رجل (للسيد غيث الشبر) ومجموعة من المرشحين من قاعدة بيانات رجالية مبنية على معجم رجال الحديث (للسيد الخوئي). مهمتك تحديد أي المرشحين هو نفس الراوي المذكور في معجم الألف رجل، أو الإقرار بعدم وجود تطابق إن لم يكن في المرشحين من يطابق.

قواعد الموازنة:
  - الكنية واللقب والنسبة والاسم الثلاثي (الجد) دلائل قوية.
  - الأئمة الذين يروي عنهم الراوي تحدد طبقته الزمنية بدقة.
  - الطبقة في معجم الألف رجل قطعية: لا تختر مرشحاً تختلف طبقته عن طبقة الألف رجل بأكثر من ٢ إلا لسبب واضح.
  - تحريفات الأسماء (مثل: إسهاعيل ↔ إسماعيل، الخوزي ↔ الخوري) شائعة جداً، فلا ترفض المرشح بمجرد اختلاف حرف.
  - إن لم يطابق أحد المرشحين بدقة، اختر null ولا تخمّن.

أعد JSON فقط بهذا الشكل (لا شرح، لا نص قبل أو بعد):
{
  "match_index": <رقم 0-based في قائمة المرشحين، أو null>,
  "confidence": "high" | "medium" | "low",
  "reasoning": "<جملة واحدة باللغة العربية تشرح أساس الاختيار>"
}
"""


def _candidate_summary(rijal_entry: dict, max_raw_chars: int = 300) -> str:
    """Compact one-line-ish description of a rijal candidate for the prompt."""
    parts = []
    name = rijal_entry.get('name_ar', '?')
    parts.append(f"الاسم: {name}")

    kunya = rijal_entry.get('kunyah')
    if kunya:
        parts.append(f"الكنية: {kunya}")
    laqab = rijal_entry.get('laqab')
    if laqab:
        parts.append(f"اللقب: {laqab}")
    nisba = rijal_entry.get('nisba')
    if nisba:
        parts.append(f"النسبة: {nisba}")

    imams = rijal_entry.get('narrates_from_imams') or []
    if imams:
        parts.append(f"يروي عن: {' • '.join(imams[:3])}")
    companions = rijal_entry.get('companions_of') or []
    if isinstance(companions, str):
        companions = [companions] if companions else []
    if companions:
        parts.append(f"أصحاب: {' • '.join(companions[:2])}")

    t = rijal_entry.get('tabaqah')
    if t is not None:
        parts.append(f"الطبقة: {t}")

    status = rijal_entry.get('status_detail') or rijal_entry.get('status')
    if status and status != 'unspecified':
        parts.append(f"الحال: {status}")

    raw = rijal_entry.get('_raw') or ''
    if raw:
        snippet = raw[:max_raw_chars].replace('\n', ' ').strip()
        parts.append(f"النص: «{snippet}…»")

    return ' | '.join(parts)


def build_prompt(alf_entry: dict, candidates: List[Tuple[str, dict, int]]) -> str:
    """Build the user prompt for one disambiguation query."""
    alf_name = alf_entry.get('name_ar', '?')
    alf_alts = alf_entry.get('alt_names') or []
    alf_t = alf_entry.get('tabaqah')
    alf_t_detail = alf_entry.get('tabaqah_detail') or ''
    alf_raw = (alf_entry.get('_raw') or '')[:1500]

    lines: List[str] = []
    lines.append("═══ من معجم الألف رجل ═══")
    lines.append(f"الاسم: {alf_name}")
    if alf_alts:
        lines.append(f"تردد بعنوان: {' • '.join(alf_alts[:5])}")
    if alf_t is not None:
        lines.append(f"الطبقة: {alf_t} ({alf_t_detail})")
    if alf_raw:
        lines.append(f"\nالترجمة:\n{alf_raw}")

    lines.append("\n═══ المرشحون من قاعدة الرجال ═══")
    for i, (_k, entry, _score) in enumerate(candidates):
        lines.append(f"\n[{i}] " + _candidate_summary(entry))

    lines.append("\nأي المرشحين هو نفس الراوي؟ أعد JSON فقط.")
    return '\n'.join(lines)


# ─── LLM backends ────────────────────────────────────────────────────────────

def _parse_llm_response(text: str) -> Optional[dict]:
    """Extract the JSON object from an LLM response."""
    # Strip <think> blocks (DeepSeek-R1 style)
    text = re.sub(r'<think>[\s\S]*?</think>', '', text)
    # Strip code fences
    text = re.sub(r'```(?:json)?\s*', '', text).strip()
    m = re.search(r'\{[\s\S]*\}', text)
    if not m:
        return None
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return None


def call_deepseek(prompt: str, api_key: str, timeout: int = 60) -> Optional[dict]:
    import requests
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 512,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.deepseek.com/chat/completions",
                json=payload, headers=headers, timeout=timeout,
            )
        except Exception as e:
            print(f"  [deepseek] exception (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            time.sleep(20)
            continue
        if r.status_code != 200:
            print(f"  [deepseek HTTP {r.status_code}]: {r.text[:200]}", flush=True)
            return None
        return _parse_llm_response(r.json()["choices"][0]["message"]["content"])
    return None


def call_claude(prompt: str, api_key: str, timeout: int = 60) -> Optional[dict]:
    import requests
    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 512,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                json=payload, headers=headers, timeout=timeout,
            )
        except Exception as e:
            print(f"  [claude] exception (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            time.sleep(20)
            continue
        if r.status_code != 200:
            print(f"  [claude HTTP {r.status_code}]: {r.text[:200]}", flush=True)
            return None
        # Claude returns content blocks
        body = r.json()
        text_blocks = [b.get('text', '') for b in body.get('content', []) if b.get('type') == 'text']
        return _parse_llm_response('\n'.join(text_blocks))
    return None


# ─── Driver ──────────────────────────────────────────────────────────────────

def load_inputs() -> Tuple[Dict, Dict, List[Dict], Dict, Set[str]]:
    """Load all the JSON files. Returns (rijal_db, alf_db, audit, existing_disamb, done_set)."""
    print(f"Loading {RIJAL_DB.name}…", flush=True)
    rijal_db = json.loads(RIJAL_DB.read_text(encoding='utf-8'))
    print(f"  {len(rijal_db):,} rijal entries", flush=True)

    print(f"Loading {ALF_RAJUL_DB.name}…", flush=True)
    alf_db = json.loads(ALF_RAJUL_DB.read_text(encoding='utf-8'))
    print(f"  {len(alf_db):,} alf entries", flush=True)

    print(f"Loading {AUDIT_FILE.name}…", flush=True)
    audit = json.loads(AUDIT_FILE.read_text(encoding='utf-8'))
    print(f"  {len(audit):,} unmatched entries to disambiguate", flush=True)

    existing: Dict[str, str] = {}
    if DISAMB_FILE.exists():
        try:
            existing = {
                str(k): str(v) for k, v in
                json.loads(DISAMB_FILE.read_text(encoding='utf-8')).items()
            }
            print(f"  {len(existing):,} existing disambiguations in {DISAMB_FILE.name}", flush=True)
        except Exception as e:
            print(f"  [warning] could not read {DISAMB_FILE.name}: {e}", flush=True)
            existing = {}

    done_set: Set[str] = set()
    if PROGRESS_FILE.exists():
        try:
            done_set = set(
                json.loads(PROGRESS_FILE.read_text(encoding='utf-8')).get('done', [])
            )
            print(f"  {len(done_set):,} entries already processed (resuming)", flush=True)
        except Exception:
            done_set = set()

    return rijal_db, alf_db, audit, existing, done_set


def save_disambiguation(disamb: Dict[str, str]):
    """Write the disambiguation file atomically."""
    tmp = DISAMB_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(disamb, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(DISAMB_FILE)


def save_progress(done_set: Set[str]):
    PROGRESS_FILE.write_text(
        json.dumps({'done': sorted(done_set)}, ensure_ascii=False),
        encoding='utf-8',
    )


def main():
    p = argparse.ArgumentParser(description="LLM-disambiguate unmatched alf_rajul entries against rijal_database_merged.json")
    p.add_argument('--backend', choices=['deepseek', 'claude'], default='deepseek')
    p.add_argument('--workers', type=int, default=5, help='Concurrent LLM requests')
    p.add_argument('--max', type=int, default=None, help='Process at most N entries (testing)')
    p.add_argument('--dry-run', action='store_true', help='Show candidates and prompts; do not call LLM')
    p.add_argument('--stats', action='store_true', help='Print candidate-count distribution and exit')
    p.add_argument('--rebuild', action='store_true',
                   help='Ignore existing progress file; re-process everything in the audit')
    args = p.parse_args()

    rijal_db, alf_db, audit, existing_disamb, done_set = load_inputs()

    if args.rebuild:
        done_set = set()

    # Diagnose existing disambiguation entries for the audit cases. If an
    # alf_key is in the audit file, its existing disambiguation value MUST
    # be either empty or pointing to a non-existent rijal key (that's why it
    # ended up in the audit). We re-process all audit entries, but also
    # print a breakdown of the staleness.
    audit_keys = [str(item.get('alf_key', '')) for item in audit if item.get('alf_key')]
    stale_empty = 0
    stale_invalid_key = 0
    no_existing_entry = 0
    for k in audit_keys:
        v = existing_disamb.get(k)
        if v is None:
            no_existing_entry += 1
        elif not v:
            stale_empty += 1
        elif str(v) not in rijal_db:
            stale_invalid_key += 1

    print(f"\nAudit-entry diagnosis (of {len(audit_keys)} audit entries):")
    print(f"  No existing disambig record:        {no_existing_entry:,}")
    print(f"  Existing record is empty string:    {stale_empty:,}")
    print(f"  Existing record → missing rijal key: {stale_invalid_key:,}")

    # Now build plans. Audit entries are always reprocessed (the existing
    # disambig values for them are stale/broken — that's why they're in the
    # audit). Non-audit entries are skipped if already in done_set.
    print("\nGathering candidates…", flush=True)
    plans: List[Tuple[str, dict, List[Tuple[str, dict, int]]]] = []
    no_candidates = 0
    single_candidate = 0
    multi_candidate = 0

    for item in audit:
        alf_key = str(item.get('alf_key', ''))
        if not alf_key:
            continue
        if alf_key in done_set:
            # Already processed in this matcher session
            continue

        alf_entry = alf_db.get(alf_key)
        if alf_entry is None:
            continue

        cands = find_candidates(alf_entry, rijal_db)
        plans.append((alf_key, alf_entry, cands))

        if not cands:
            no_candidates += 1
        elif len(cands) == 1:
            single_candidate += 1
        else:
            multi_candidate += 1

    print(f"  Plans built: {len(plans):,}")
    print(f"    0 candidates (truly absent):     {no_candidates:,}")
    print(f"    1 candidate (auto-resolve):      {single_candidate:,}")
    print(f"    2+ candidates (need LLM):        {multi_candidate:,}")

    if args.stats:
        # Candidate count histogram
        from collections import Counter
        counts = Counter(len(c) for _, _, c in plans)
        print("\n  Candidate-count distribution:")
        for n in sorted(counts):
            print(f"    {n:>2} candidates: {counts[n]:,}")
        return

    if args.max:
        plans = plans[:args.max]
        print(f"  (capped to first {args.max} entries for this run)")

    # Trivially resolve 0-cand and 1-cand cases without an LLM call
    auto_resolved = 0
    truly_absent = 0
    needs_llm: List[Tuple[str, dict, List[Tuple[str, dict, int]]]] = []

    for alf_key, alf_entry, cands in plans:
        if not cands:
            existing_disamb[alf_key] = ""    # explicit "no match in rijal DB"
            done_set.add(alf_key)
            truly_absent += 1
        elif len(cands) == 1:
            existing_disamb[alf_key] = cands[0][0]
            done_set.add(alf_key)
            auto_resolved += 1
        else:
            needs_llm.append((alf_key, alf_entry, cands))

    print(f"\n  Auto-resolved (1 candidate): {auto_resolved:,}")
    print(f"  Marked truly absent:         {truly_absent:,}")
    print(f"  Will query LLM:              {len(needs_llm):,}")

    if (auto_resolved or truly_absent) and not args.dry_run:
        save_disambiguation(existing_disamb)
        save_progress(done_set)

    if not needs_llm:
        print("\nNothing left for LLM. Done.")
        return

    if args.dry_run:
        print("\n[dry-run] Sample prompt for first entry:")
        sample_key, sample_alf, sample_cands = needs_llm[0]
        print(f"  alf_key={sample_key}")
        print(f"  candidates: {[(k, e.get('name_ar', '?')) for k,e,_ in sample_cands]}")
        print("─" * 60)
        print(build_prompt(sample_alf, sample_cands)[:2000])
        print("─" * 60)
        return

    # API key
    if args.backend == 'deepseek':
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        env_var = "DEEPSEEK_API_KEY"
        call_fn = call_deepseek
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        env_var = "ANTHROPIC_API_KEY"
        call_fn = call_claude

    if not api_key:
        print(f"\n[error] {env_var} not set in environment.")
        print(f"  Set it with: export {env_var}=...")
        sys.exit(1)

    # Process via thread pool
    print(f"\nQuerying {args.backend}: {len(needs_llm):,} entries, {args.workers} workers…", flush=True)
    lock = threading.Lock()
    counter = [0]
    matched = [0]
    no_match = [0]

    def process(item):
        alf_key, alf_entry, cands = item
        prompt = build_prompt(alf_entry, cands)
        try:
            result = call_fn(prompt, api_key)
        except Exception as e:
            return alf_key, None, None, str(e)
        if not result:
            return alf_key, None, cands, "no_parsed_response"
        return alf_key, result, cands, None

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(process, item): item for item in needs_llm}
        for fut in as_completed(futs):
            alf_key, result, cands, err = fut.result()
            with lock:
                counter[0] += 1
                done_set.add(alf_key)

                if err:
                    print(f"  [{counter[0]:>3}/{len(needs_llm)}] {alf_key}: ERROR {err}", flush=True)
                elif result is None:
                    print(f"  [{counter[0]:>3}/{len(needs_llm)}] {alf_key}: no result", flush=True)
                else:
                    idx = result.get('match_index')
                    conf = result.get('confidence', '?')
                    reason = result.get('reasoning', '')[:80]
                    if idx is None:
                        existing_disamb[alf_key] = ""
                        no_match[0] += 1
                        print(f"  [{counter[0]:>3}/{len(needs_llm)}] {alf_key}: no match ({conf}) — {reason}", flush=True)
                    elif isinstance(idx, int) and 0 <= idx < len(cands):
                        rijal_key = cands[idx][0]
                        existing_disamb[alf_key] = rijal_key
                        matched[0] += 1
                        rijal_name = cands[idx][1].get('name_ar', '?')[:40]
                        print(f"  [{counter[0]:>3}/{len(needs_llm)}] {alf_key} → {rijal_key} ({rijal_name}) [{conf}]", flush=True)
                    else:
                        print(f"  [{counter[0]:>3}/{len(needs_llm)}] {alf_key}: invalid index {idx}", flush=True)

                # Save every 20 entries
                if counter[0] % 20 == 0:
                    save_disambiguation(existing_disamb)
                    save_progress(done_set)

    save_disambiguation(existing_disamb)
    save_progress(done_set)

    print(f"\n══ Summary ══")
    print(f"  Total entries planned:        {len(plans):,}")
    print(f"  Auto-resolved (1 candidate):  {auto_resolved:,}")
    print(f"  Marked truly absent:          {truly_absent:,}")
    print(f"  LLM-matched:                  {matched[0]:,}")
    print(f"  LLM said no match:            {no_match[0]:,}")
    print(f"  Disambiguation file:          {DISAMB_FILE.name} ({len(existing_disamb):,} total entries)")
    print()
    print("Next: re-run `python tabaqah_inference.py --apply --rebuild` to apply these.")


if __name__ == '__main__':
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
    main()

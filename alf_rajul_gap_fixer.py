#!/usr/bin/env python3
"""
alf_rajul_gap_fixer.py
======================
Recovers the ~15 Alf Rajul entries whose OCR entry numbers were corrupted
(e.g. ۱۶۱ read as ۱۲۱, ۲۴۳ read as ۷۴۳).

Strategy:
  1. Parse the OCR text with the fixed regex → find 988 entries
  2. Find all text gaps between consecutive found entries that contain a
     "missing" expected entry number (based on sequential order)
  3. Send each gap to DeepSeek: "this text contains entry #N, extract it"
  4. Merge recovered entries into alf_rajul_database.json

Run:
  python alf_rajul_gap_fixer.py --dry-run    # show gaps without calling LLM
  python alf_rajul_gap_fixer.py              # full run + save
"""

import json
import os
import re
import sys
import time
import requests
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

TXT_PATH = Path("books_md/output_surya_bbox.txt")
ALF_DB   = Path("alf_rajul_database.json")

DRY_RUN  = "--dry-run" in sys.argv

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_URL     = "https://api.deepseek.com/chat/completions"

# ── Digit maps (Arabic-Indic + Persian Extended) ────────────────────────────
_AR_DIGIT = {chr(0x660 + i): str(i) for i in range(10)}
_AR_DIGIT.update({chr(0x6f0 + i): str(i) for i in range(10)})

def ar2int(s: str) -> int:
    return int(''.join(_AR_DIGIT.get(c, c) for c in s))

# ── Normalisation ────────────────────────────────────────────────────────────
_DIAC = re.compile(r'[\u064b-\u065f\u0670]')
_ALEF = re.compile(r'[إأآا]')

def norm(t: str) -> str:
    t = _DIAC.sub('', t)
    t = _ALEF.sub('ا', t)
    return re.sub(r'\s+', ' ', t.replace('ـ', '')).strip()

# ── Tabaqah map ──────────────────────────────────────────────────────────────
TABAQAH_MAP = {
    'الأولى': 1, 'الاولى': 1,
    'الثانية': 2,
    'الثالثة': 3,
    'الرابعة': 4,
    'الخامسة': 5,
    'السادسة': 6,
    'السابعة': 7,
    'الثامنة': 8,
    'التاسعة': 9,
    'العاشرة': 10,
    'الحادية عشرة': 11,
    'الثانية عشرة': 12,
}

TABAQAH_LABELS = {
    1: "T1 — صحابة النبي",
    2: "T2 — صحابة الإمام علي",
    3: "T3 — عصر السجاد",
    4: "T4 — عصر الباقر",
    5: "T5 — عصر الصادق",
    6: "T6 — عصر الكاظم",
    7: "T7 — عصر الرضا والجواد",
    8: "T8 — عصر الهادي والعسكري",
    9: "T9 — الغيبة الصغرى (مبكرة)",
    10: "T10 — الغيبة الصغرى (متأخرة)",
    11: "T11 — الغيبة الكبرى (مبكرة)",
    12: "T12 — عصر التدوين",
}

ENTRY_HEADER_RE = re.compile(
    r'^([0-9\u0660-\u0669\u06f0-\u06f9]+)\s*[.\-]\s*([\u0600-\u06ff][^\n]{3,100}?)$',
    re.MULTILINE
)
BOLD_RE = re.compile(r'<b>([^<]+)</b>')

def text_to_tabaqah(text: str):
    for word, num in sorted(TABAQAH_MAP.items(), key=lambda x: -len(x[0])):
        if word in text:
            return num, word
    return None, None

ALT_NAMES_RE = re.compile(
    r'يرد بعنوان\s*[:：]\s*([\s\S]+?)(?=\n\n|\Z)',
    re.MULTILINE
)

# ── Parse to find existing entries + gaps ───────────────────────────────────

def parse_existing(text: str):
    traajim_pos = text.find('التراجم')
    if traajim_pos == -1:
        traajim_pos = 0

    headers = []
    for m in ENTRY_HEADER_RE.finditer(text, traajim_pos):
        try:
            num = ar2int(m.group(1))
        except ValueError:
            continue
        if not (1 <= num <= 1100):
            continue
        headers.append((m.start(), m.end(), num, m.group(2).strip()))

    # Deduplicate
    seen = set()
    unique = []
    for h in headers:
        if h[2] not in seen:
            seen.add(h[2])
            unique.append(h)
    return unique


def find_gaps(headers: list, total: int = 1003):
    """Return list of (expected_num, text_start, text_end) for missing entries."""
    found_nums = {h[2] for h in headers}
    missing = [i for i in range(1, total + 1) if i not in found_nums]

    # For each missing number, find the text gap between its neighbours
    # Build a mapping: num → (body_start, body_end)
    bodies = {}
    for i, (start, end, num, name) in enumerate(headers):
        body_end = headers[i + 1][0] if i + 1 < len(headers) else None
        bodies[num] = (end, body_end)   # body starts after header line end

    gaps = []
    for m in missing:
        # Find the closest found entries before and after
        before = max((n for n in found_nums if n < m), default=None)
        after  = min((n for n in found_nums if n > m), default=None)
        if before is None or after is None:
            continue

        # The gap text = from start-of-before-body to start-of-after-header
        before_body_start = bodies[before][0]
        after_header_start = next(
            (h[0] for h in headers if h[2] == after), None
        )
        if after_header_start is None:
            continue

        gaps.append({
            'expected_num': m,
            'neighbour_before': before,
            'neighbour_after': after,
            'text_start': before_body_start,
            'text_end': after_header_start,
        })

    # Merge overlapping gaps (consecutive missing numbers share the same text block)
    merged = []
    for g in gaps:
        if merged and g['text_start'] == merged[-1]['text_start']:
            merged[-1]['expected_nums'] = merged[-1].get(
                'expected_nums', [merged[-1]['expected_num']]
            ) + [g['expected_num']]
            merged[-1]['neighbour_after'] = g['neighbour_after']
            merged[-1]['text_end'] = g['text_end']
        else:
            merged.append(dict(g, expected_nums=[g['expected_num']]))

    return merged


# ── DeepSeek call ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """أنت متخصص في استخراج بيانات الرجال من كتب الحديث الشيعية.
المطلوب منك استخراج بيانات مداخل كتاب "معجم الألف رجل" للسيد غيث شبر.

لكل مدخل:
- رقم المدخل (الرقم الصحيح الذي ينبغي أن يكون، مع مراعاة أن الأرقام في النص قد تكون مشوهة بسبب OCR)
- اسم الراوي بالعربية
- رقم الطبقة (من 1 إلى 12) — الطبقة النهائية التي خلص إليها المؤلف (غالبا تكون مُغلَّظة في النص)
- نص الطبقة (العبارة الدالة على الطبقة مثل: "من الخامسة" أو "كبار الرابعة")
- الأسماء البديلة إن وجدت (من عبارة "يرد بعنوان")

الطبقات:
1=صحابة النبي, 2=صحابة الإمام علي, 3=عصر السجاد, 4=عصر الباقر,
5=عصر الصادق, 6=عصر الكاظم, 7=عصر الرضا والجواد, 8=عصر الهادي والعسكري,
9=الغيبة الصغرى (مبكرة), 10=الغيبة الصغرى (متأخرة),
11=الغيبة الكبرى (مبكرة), 12=عصر التدوين

أجب بـ JSON فقط — مصفوفة من الكائنات، كل كائن:
{"num": int, "name_ar": str, "tabaqah": int_or_null, "tabaqah_detail": str_or_null, "alt_names": [str]}
"""

def call_deepseek(user_msg: str, expected_nums: list) -> list | None:
    """Call DeepSeek to extract entries from a text gap."""
    prompt = (
        f"النص التالي يحتوي على المداخل المتوقعة رقم/أرقام: {expected_nums}.\n"
        f"أرقام المداخل في النص قد تكون مشوهة بسبب OCR — استخدم السياق للتصحيح.\n\n"
        f"النص:\n{user_msg[:8000]}"
    )

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": "deepseek-reasoner",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "max_tokens": 1024,
        "temperature": 0,
    }

    for attempt in range(3):
        try:
            r = requests.post(DEEPSEEK_URL, headers=headers, json=body, timeout=120)
            if r.status_code == 429:
                print("  [rate limit] sleeping 20s…")
                time.sleep(20)
                continue
            r.raise_for_status()
            raw = r.json()['choices'][0]['message']['content']
            # Strip markdown fences
            raw = re.sub(r'```(?:json)?\s*', '', raw).strip().rstrip('`').strip()
            # Strip <think> sections
            raw = re.sub(r'<think>[\s\S]*?</think>', '', raw).strip()
            return json.loads(raw)
        except Exception as e:
            print(f"  [attempt {attempt+1}] error: {e}")
            time.sleep(5)

    return None


# ── Regex fallback (no LLM) ──────────────────────────────────────────────────

# Very permissive: any digits (including Western) at start of line + Arabic name
_ANY_HEADER_RE = re.compile(
    r'^([0-9\u0660-\u0669\u06f0-\u06f9]{1,4})\s*[.\-]\s*([\u0600-\u06ff][^\n]{3,80}?)$',
    re.MULTILINE
)

def regex_extract_from_gap(gap_text: str, expected_nums: list) -> list | None:
    """
    Try to extract entry data from gap text using regex alone.
    Looks for any digit-like header line in the gap, takes the best match,
    then extracts tabaqah from the surrounding body.
    """
    # Find all potential entry headers in the gap
    candidates = []
    for m in _ANY_HEADER_RE.finditer(gap_text):
        raw = m.group(1)
        try:
            num = int(''.join(_AR_DIGIT.get(c, c) for c in raw))
        except ValueError:
            continue
        if 1 <= num <= 1100:
            candidates.append((m.start(), m.end(), num, m.group(2).strip()))

    if not candidates:
        return None

    results = []
    remaining_expected = list(expected_nums)  # pool of unassigned expected numbers

    for i, (start, end, cand_num, name) in enumerate(candidates):
        if not remaining_expected:
            break  # all expected entries already assigned

        # Take body between this header and the next candidate (or end of gap)
        body_end = candidates[i + 1][0] if i + 1 < len(candidates) else len(gap_text)
        body = gap_text[end:body_end]

        # Extract tabaqah from bold tags first, then plain text
        bolds = BOLD_RE.findall(body)
        tab, tab_detail = None, None
        for b in reversed(bolds):
            t, d = text_to_tabaqah(b.strip())
            if t:
                tab, tab_detail = t, d
                break
        if tab is None:
            # Plain text fallback
            _PLAIN_RE = re.compile(
                r'(?:من|وهو|فهو|فإنه|يكون|عدّه|عده|فيكون|والصحيح)\s+'
                r'(?:من\s+)?(?:صغار\s+|كبار\s+|أوائل\s+|أواخر\s+)?'
                r'(الأولى|الاولى|الثانية|الثالثة|الرابعة|الخامسة|'
                r'السادسة|السابعة|الثامنة|التاسعة|العاشرة|الحادية عشرة|الثانية عشرة)'
            )
            m2 = _PLAIN_RE.search(body[-600:])
            if m2:
                tab_detail = m2.group(1)
                tab = TABAQAH_MAP.get(tab_detail)

        # Assign expected numbers in order (first found → first expected)
        # so consecutive missing entries get correctly separated
        assign_num = remaining_expected.pop(0)

        # Extract alt names
        alt_names = []
        alt_match = ALT_NAMES_RE.search(body)
        if alt_match:
            raw_alts = re.sub(r'<[^>]+>', '', alt_match.group(1))
            for part in re.split(r'[،,]', raw_alts):
                cleaned = part.strip().rstrip('.').strip()
                if len(cleaned) >= 3 and re.search(r'[\u0600-\u06ff]', cleaned):
                    alt_names.append(cleaned)

        results.append({
            'num':            assign_num,
            'name_ar':        name,
            'tabaqah':        tab,
            'tabaqah_detail': tab_detail,
            'alt_names':      alt_names,
        })
        print(f"  [regex] Found entry header: num={cand_num} -> assign={assign_num}  name={name[:40]}  T={tab}")

    return results if results else None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"Reading {TXT_PATH}…")
    text = TXT_PATH.read_text(encoding='utf-8')

    print("Parsing existing entries…")
    headers = parse_existing(text)
    found_nums = {h[2] for h in headers}
    missing = [i for i in range(1, 1004) if i not in found_nums]
    print(f"  Found: {len(headers)}  Missing: {len(missing)}: {missing}")

    if not missing:
        print("No missing entries — nothing to do.")
        return

    gaps = find_gaps(headers)
    print(f"  Identified {len(gaps)} text gap(s) to process\n")

    # Load existing alf_rajul_database
    print(f"Loading {ALF_DB}…")
    alf_db: dict = json.loads(ALF_DB.read_text(encoding='utf-8'))
    print(f"  {len(alf_db)} existing entries")

    recovered = 0
    for gap in gaps:
        expected = gap.get('expected_nums', [gap['expected_num']])
        before   = gap['neighbour_before']
        after    = gap['neighbour_after']
        gap_text = text[gap['text_start']:gap['text_end']]

        print(f"\n[Gap] Expected entry/entries {expected}  (between {before} and {after})")
        print(f"  Gap text length: {len(gap_text)} chars")
        print(f"  Preview: {gap_text[:200].strip()!r}")

        if DRY_RUN:
            print("  [dry-run] skipping LLM call")
            continue

        if not DEEPSEEK_API_KEY:
            print("  [no API key] trying regex fallback…")
            results = regex_extract_from_gap(gap_text, expected)
        else:
            print("  Calling DeepSeek…")
            results = call_deepseek(gap_text, expected)
            if not results:
                print("  [LLM fail] trying regex fallback…")
                results = regex_extract_from_gap(gap_text, expected)
        if not results:
            print("  [FAIL] could not extract entry")
            continue

        print(f"  LLM returned {len(results)} entry/entries")
        for entry in results:
            num = entry.get('num')
            if num is None:
                print(f"  [SKIP] missing num in {entry}")
                continue
            key = str(num)
            if key in alf_db:
                print(f"  [SKIP] entry {num} already in database")
                continue

            # Validate tabaqah
            tab = entry.get('tabaqah')
            tab_detail = entry.get('tabaqah_detail', '')
            if tab and not (1 <= tab <= 12):
                tab = None

            alf_db[key] = {
                'num':            num,
                'name_ar':        entry.get('name_ar', ''),
                'tabaqah':        tab,
                'tabaqah_detail': tab_detail,
                'tabaqah_sub':    None,
                'alt_names':      entry.get('alt_names', []),
                '_raw':           gap_text[:3000],
                '_source':        'llm_gap_fixer',
            }
            print(f"  -> Recovered entry {num}: {entry.get('name_ar', '')[:50]} | T{tab}")
            recovered += 1

    print(f"\n=== Done ===")
    print(f"  Recovered: {recovered} entries")
    print(f"  Total alf_rajul entries: {len(alf_db)}")

    if not DRY_RUN and recovered > 0:
        ALF_DB.write_text(
            json.dumps(alf_db, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
        print(f"  Saved {ALF_DB}  ({ALF_DB.stat().st_size/1e6:.1f} MB)")


if __name__ == '__main__':
    main()

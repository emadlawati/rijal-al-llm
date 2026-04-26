#!/usr/bin/env python3
"""
rijal_full_builder.py  —  Path 2: LLM re-processing from _raw_full
====================================================================
Re-processes rijal_database.json entries using their full Mu'jam Rijal
text (_raw_full) as source — far richer than the condensed _raw text
used by rijal_builder.py.

Priority order for processing:
  1. Entries where `_raw_full` contains روى عن but narrates_from_narrators is []
  2. Entries where len(_raw_full) > 3 × len(_raw)  (lots of new content)
  3. All remaining entries with _raw_full (status == "unspecified")

Merge strategy (NEVER overwrites):
  • Lists: union-merge (new items appended if not already present)
  • Scalars (status, tariq_status, …): only written if currently null/"unspecified"
  • Protected fields: name_ar, name_en, _raw, _raw_full, _entry_idx, _num_*

Backends:
  --backend ollama     (default)  qwen3:14b via localhost:11434
  --backend claude               claude-sonnet-4-6
  --backend gemini               gemini-2.5-pro-preview (via google-generativeai)
  --backend deepseek             deepseek-chat via API

Usage:
  python rijal_full_builder.py --dry-run           # show queue, don't process
  python rijal_full_builder.py --backend claude     # run with Claude
  python rijal_full_builder.py --backend ollama     # run with local Ollama
  python rijal_full_builder.py --start 500          # resume from entry 500
  python rijal_full_builder.py --limit 100          # process only 100 entries
"""

import json
import os
import re
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────
DB_PATH       = Path("rijal_database.json")
PROGRESS_PATH = Path("rijal_full_progress.json")

DRY_RUN  = "--dry-run"  in sys.argv
BACKEND  = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--backend"), "deepseek")
START_AT = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--start"), 0))
LIMIT    = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--limit"), 0))
WORKERS  = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--workers"), 5))

# ─── System prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
أنت مساعد متخصص في علم الرجال الشيعي. ستُعطى النص الكامل لترجمة راوٍ من كتاب معجم رجال الحديث للسيد الخوئي.
استخرج المعلومات التالية بدقة عالية وأعِدها بتنسيق JSON فقط، بدون أي شرح إضافي.

الحقول المطلوبة:
{
  "status": "thiqah|daif|hasan|majhul|mujmal|mursal|unspecified",
  "gender": "male|female",
  "era": "صحابي|تابعي|قرن ثانٍ|قرن ثالث|قرن رابع|...",
  "tribe": "اسم القبيلة أو العائلة إن وُجدت",
  "kunya": "كنيته مثل أبو عبد الله",
  "laqab": "لقبه إن وُجد",
  "nisbah": "نسبته مثل الكوفي أو البصري",
  "death_year": "سنة الوفاة بالهجري كرقم، أو null",
  "birth_year": "سنة الميلاد بالهجري كرقم، أو null",
  "has_book": true|false,
  "book_title": "عنوان كتابه إن وُجد",
  "tariq_status": "sahih|daif|null",
  "tariq_details": "نص وصف الطريق إن وُجد",
  "narrates_from_narrators": ["قائمة الرواة الذين روى عنهم — أشخاص فقط، لا كتب"],
  "narrates_from_imams": ["الأئمة الذين روى عنهم"],
  "narrated_from_by": ["الرواة الذين رووا عنه"],
  "other_names": ["أسماء أخرى أو تصحيفات مذكورة"],
  "notes": "أي ملاحظة مهمة لا تندرج تحت الحقول السابقة، أو null"
}

قواعد:
- إذا لم تجد المعلومة فاكتب null أو [] أو false حسب نوع الحقل.
- للرواة: اكتب الاسم كما ورد في النص بدون إضافة أو حذف.
- لا تستنتج ما لم يُذكر صراحةً في النص.
- أعِد JSON فقط، بلا مقدمة ولا تعليق.
"""

def build_user_prompt(entry: dict) -> str:
    name = entry.get("name_ar", "؟")
    idx  = entry.get("_entry_idx", "؟")
    raw  = entry.get("_raw_full") or entry.get("_raw", "")
    return f"الراوي: {name}  (رقم: {idx})\n\nالنص الكامل:\n{raw}"

# ─── Arabic normalisation ─────────────────────────────────────────────────────
_DIAC = re.compile(r'[\u064b-\u065f\u0670]')

def norm(t: str) -> str:
    return _DIAC.sub('', t).replace('ـ', '').strip()

# ─── Merge helpers ─────────────────────────────────────────────────────────────

PROTECTED = {
    'name_ar', 'name_en', '_raw', '_raw_full', '_entry_idx',
    '_num_beirut', '_num_najaf', '_num_tehran',
}

# Fields that are list type
LIST_FIELDS = {
    'narrates_from_narrators', 'narrates_from_imams', 'narrated_from_by',
    'other_names', 'books',
}
# Scalar fields: only update if currently falsy/None/"unspecified"
SCALAR_FIELDS = {
    'status', 'gender', 'era', 'tribe', 'kunya', 'laqab', 'nisbah',
    'death_year', 'birth_year', 'has_book', 'book_title',
    'tariq_status', 'tariq_details', 'notes',
}

def merge_into(entry: dict, extracted: dict) -> int:
    """
    Merge extracted dict into entry using union-merge for lists,
    update-if-null for scalars.
    Returns number of fields changed.
    """
    changed = 0

    for field, new_val in extracted.items():
        if field in PROTECTED:
            continue

        if field in LIST_FIELDS:
            existing = entry.get(field) or []
            if not isinstance(new_val, list):
                new_val = [new_val] if new_val else []
            existing_norm = {norm(str(x)) for x in existing}
            added = 0
            result = list(existing)
            for item in new_val:
                if item and norm(str(item)) not in existing_norm:
                    result.append(item)
                    existing_norm.add(norm(str(item)))
                    added += 1
            if added:
                entry[field] = result
                changed += added

        elif field in SCALAR_FIELDS:
            existing = entry.get(field)
            if existing in (None, '', False, 'unspecified', [], 0):
                if new_val not in (None, '', 'null', [], 0):
                    # Coerce death_year / birth_year to int
                    if field in ('death_year', 'birth_year') and new_val:
                        try:
                            new_val = int(str(new_val).replace('هـ', '').strip())
                        except ValueError:
                            new_val = None
                    if new_val is not None:
                        entry[field] = new_val
                        changed += 1

    return changed

# ─── Backend callers ──────────────────────────────────────────────────────────

def call_ollama(prompt: str) -> str:
    import urllib.request
    payload = json.dumps({
        "model": "qwen3:14b",
        "prompt": SYSTEM_PROMPT + "\n\n" + prompt,
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 1024},
    }).encode()
    req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
    return data.get("response", "")


def call_claude(prompt: str) -> str:
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return msg.content[0].text


def call_gemini(prompt: str) -> str:
    import google.generativeai as genai
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    model = genai.GenerativeModel(
        "gemini-2.5-pro-preview-05-06",
        system_instruction=SYSTEM_PROMPT,
    )
    resp = model.generate_content(prompt)
    return resp.text


def call_deepseek(prompt: str) -> str:
    import requests as req
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY environment variable not set.")
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 2048,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    while True:
        resp = req.post("https://api.deepseek.com/chat/completions",
                        json=payload, headers=headers, timeout=120)
        if resp.status_code == 429:
            print("  [DeepSeek rate limit — sleeping 20 s …]", flush=True)
            time.sleep(20)
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"DeepSeek API error {resp.status_code}: {resp.text}")
        break
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected DeepSeek response: {data}")


BACKENDS = {
    "ollama":   call_ollama,
    "claude":   call_claude,
    "gemini":   call_gemini,
    "deepseek": call_deepseek,
}

def call_llm(prompt: str) -> str:
    fn = BACKENDS.get(BACKEND)
    if fn is None:
        raise ValueError(f"Unknown backend: {BACKEND}. Choose: {list(BACKENDS)}")
    return fn(prompt)


def parse_llm_response(text: str) -> dict | None:
    """Extract and parse the JSON block from LLM output."""
    # Strip thinking/reasoning tags (Ollama qwen3 uses <think>, DeepSeek uses <reasoning>)
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'<reasoning>.*?</reasoning>', '', text, flags=re.DOTALL)
    # Strip markdown code fences
    text = re.sub(r'```(?:json)?\s*', '', text).strip()
    # Find JSON block
    m = re.search(r'\{[\s\S]*\}', text)
    if not m:
        return None
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return None


# ─── Queue builder ────────────────────────────────────────────────────────────

def build_queue(db: dict) -> list[dict]:
    """
    Priority:
      P1 — روى عن in _raw_full but narrates_from_narrators is []
      P2 — len(_raw_full) > 3 * len(_raw) (rich full text)
      P3 — has _raw_full and status == "unspecified"
    """
    p1, p2, p3 = [], [], []

    for key, entry in db.items():
        raw_full = entry.get('_raw_full', '')
        if not raw_full:
            continue

        has_narrators = bool(entry.get('narrates_from_narrators'))
        has_rawa_an   = 'روى عن' in raw_full or 'روى عنه' in raw_full

        raw_short = entry.get('_raw', '')
        is_rich   = len(raw_full) > 3 * max(len(raw_short), 1)
        is_unspec = entry.get('status') in (None, '', 'unspecified')

        if has_rawa_an and not has_narrators:
            p1.append(entry)
        elif is_rich:
            p2.append(entry)
        elif is_unspec:
            p3.append(entry)

    # Sort each tier by _entry_idx
    key_fn = lambda e: e.get('_entry_idx', 0)
    return sorted(p1, key=key_fn) + sorted(p2, key=key_fn) + sorted(p3, key=key_fn)


# ─── Progress helpers ─────────────────────────────────────────────────────────

def load_progress() -> set[int]:
    if PROGRESS_PATH.exists():
        data = json.loads(PROGRESS_PATH.read_text(encoding='utf-8'))
        return set(data.get('done', []))
    return set()


def save_progress(done: set[int]):
    tmp = PROGRESS_PATH.with_suffix('.tmp')
    tmp.write_text(
        json.dumps({'done': sorted(done)}, ensure_ascii=False),
        encoding='utf-8'
    )
    os.replace(tmp, PROGRESS_PATH)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if BACKEND not in BACKENDS:
        print(f"Unknown backend '{BACKEND}'. Choose: {list(BACKENDS)}")
        sys.exit(1)

    print(f"Loading {DB_PATH} …")
    with DB_PATH.open(encoding='utf-8') as f:
        db = json.load(f)
    print(f"  {len(db):,} entries")

    queue = build_queue(db)
    print(f"  Queue: {len(queue):,} entries to process")
    print(f"         (backend={BACKEND}, start={START_AT}, limit={LIMIT or 'all'})")

    if DRY_RUN:
        # Show priority breakdown
        p1 = p2 = p3 = 0
        for e in queue:
            raw_full = e.get('_raw_full', '')
            raw_short = e.get('_raw', '')
            has_narrators = bool(e.get('narrates_from_narrators'))
            has_rawa      = 'روى عن' in raw_full or 'روى عنه' in raw_full
            is_rich       = len(raw_full) > 3 * max(len(raw_short), 1)
            if has_rawa and not has_narrators:
                p1 += 1
            elif is_rich:
                p2 += 1
            else:
                p3 += 1
        print(f"\n  P1 (missing narrators): {p1:,}")
        print(f"  P2 (rich full text):    {p2:,}")
        print(f"  P3 (status=unspec):     {p3:,}")
        print("\n[dry-run — nothing processed]")
        return

    done_idxs = load_progress()
    print(f"  Already done: {len(done_idxs):,}")
    print(f"  Workers: {WORKERS}")

    # Apply --start offset and --limit
    queue_slice = queue[START_AT:]
    if LIMIT:
        queue_slice = queue_slice[:LIMIT]

    # Filter out already-done entries
    queue_slice = [e for e in queue_slice if e.get('_entry_idx', -1) not in done_idxs]
    print(f"  Remaining to process: {len(queue_slice):,}")

    # Thread-safety locks
    progress_lock   = threading.Lock()
    print_lock      = threading.Lock()
    counter_lock    = threading.Lock()
    checkpoint_lock = threading.Lock()   # prevents overlapping checkpoint writes

    stats = defaultdict(int)
    completed_count = [0]
    save_every = 50

    def process_entry(entry: dict) -> tuple[int, int, str]:
        """Called in worker thread. Returns (idx, fields_changed, status_str)."""
        idx  = entry.get('_entry_idx', -1)
        prompt = build_user_prompt(entry)

        try:
            raw_response = call_llm(prompt)
        except Exception as exc:
            return idx, -1, f"ERROR: {exc}"

        parsed = parse_llm_response(raw_response)
        if parsed is None:
            return idx, 0, f"PARSE FAIL: {raw_response[:80]!r}"

        changed = merge_into(entry, parsed)
        return idx, changed, "ok"

    def _do_checkpoint():
        """Runs in a background thread — never blocks the main loop."""
        if not checkpoint_lock.acquire(blocking=False):
            return   # previous checkpoint still writing, skip this one
        try:
            with DB_PATH.open('w', encoding='utf-8') as f:
                json.dump(db, f, ensure_ascii=False, indent=2)
            with progress_lock:
                save_progress(done_idxs)
            size_mb = DB_PATH.stat().st_size / 1_000_000
            with print_lock:
                print(f"  -- checkpoint saved ({size_mb:.1f} MB, "
                      f"done={len(done_idxs):,}) --", flush=True)
        finally:
            checkpoint_lock.release()

    def checkpoint(wait: bool = False):
        t = threading.Thread(target=_do_checkpoint, daemon=True)
        t.start()
        if wait:
            t.join()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process_entry, e): e for e in queue_slice}

        for future in as_completed(futures):
            idx, changed, status = future.result()
            name = futures[future].get('name_ar', '?')

            with progress_lock:
                done_idxs.add(idx)

            with counter_lock:
                completed_count[0] += 1
                n = completed_count[0]

            if status == "ok":
                with counter_lock:
                    stats['entries_processed'] += 1
                    stats['fields_added'] += changed
                if changed:
                    with print_lock:
                        pct = n / len(queue_slice) * 100
                        print(f"  [{n:5,}/{len(queue_slice):,}  {pct:.0f}%]  "
                              f"idx={idx}  +{changed} fields  "
                              f"({name[:25].encode('ascii','replace').decode()})",
                              flush=True)
            elif status.startswith("ERROR"):
                with counter_lock:
                    stats['errors'] += 1
                with print_lock:
                    print(f"  [ERROR] idx={idx}: {status}", flush=True)
            else:
                with counter_lock:
                    stats['parse_fail'] += 1
                with print_lock:
                    print(f"  [PARSE FAIL] idx={idx}: {status}", flush=True)

            # Periodic checkpoint (every save_every completions)
            if n % save_every == 0:
                checkpoint()

    # Final save — wait=True so we don't exit before the file is written
    checkpoint(wait=True)

    print()
    print("=== Path 2 Results ===")
    print(f"  Entries processed:  {stats['entries_processed']:,}")
    print(f"  Fields added:       {stats['fields_added']:,}")
    print(f"  Errors:             {stats['errors']:,}")
    print(f"  Parse failures:     {stats['parse_fail']:,}")
    size_mb = DB_PATH.stat().st_size / 1_000_000
    print(f"  Database size:      {size_mb:.1f} MB")


if __name__ == '__main__':
    main()

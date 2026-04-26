#!/usr/bin/env python3
"""
Rijāl Database Builder v2 — idx-Keyed Rewrite
==============================================
Processes entries from al-Mufīd min Muʿjam Rijāl al-Ḥadīth through
a local Ollama model (qwen3:14b recommended), DeepSeek, Gemini, or
Claude API to extract structured narrator data.

v2 KEY CHANGE: Uses `idx` (0-based sequential array position) as the
database primary key instead of `n3` (Tehran edition number).

WHY: n3 has ~3,100 duplicates across 15,545 entries, causing silent
overwrites (13.5% data loss in v1). idx is always unique and sequential.

Cross-references in the book still use n3 numbers, so we load both:
  - rijal_name_index.json  (idx → name/n3/n1/n2)
  - rijal_n3_index.json    (n3 → [list of idx values])

This allows correct resolution of "الآتي ٨٩٥" → n3=895 → idx(es).

Requirements:
    pip install requests anthropic

Usage:
    # Build name index first (one-time, ~5 seconds):
    python rijal_prepass.py

    # Test run (50 entries) with DeepSeek:
    python rijal_builder.py --deepseek --count 50

    # Full run with Ollama:
    python rijal_builder.py --ollama --model qwen3:14b

    # With Claude API (requires ANTHROPIC_API_KEY):
    python rijal_builder.py --count 50

    # IMPORTANT: If upgrading from v1, you MUST re-run from --start 0
    # because the old n3-keyed database has overwritten entries.
"""

import json
import os
import re
import sys
import time
import argparse
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# ─── Configuration ────────────────────────────────────────────────────────────
DEFAULT_BATCH_SIZE  = 4
DEFAULT_DELAY       = 0.5          # seconds between batches (Ollama is local)
CLAUDE_MODEL        = "claude-sonnet-4-20250514"
CLAUDE_DELAY        = 1.0          # polite delay for Claude API
OLLAMA_URL          = "http://localhost:11434/api/chat"
MAX_TOKENS          = 8192
MAX_RETRIES         = 3
MAX_ENTRY_CHARS     = 14000   # truncate entries longer than this before sending

SCRIPT_DIR      = Path(__file__).parent
ENTRIES_FILE    = SCRIPT_DIR / "rijal_entries.json"
DATABASE_FILE   = SCRIPT_DIR / "rijal_database.json"
PROGRESS_FILE   = SCRIPT_DIR / "rijal_progress.json"
ERROR_LOG       = SCRIPT_DIR / "rijal_errors.log"
NAME_INDEX_FILE = SCRIPT_DIR / "rijal_name_index.json"
N3_INDEX_FILE   = SCRIPT_DIR / "rijal_n3_index.json"

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rijal")

# ─── Arabic numeral utilities ─────────────────────────────────────────────────
_AR = {'٠':0,'١':1,'٢':2,'٣':3,'٤':4,'٥':5,'٦':6,'٧':7,'٨':8,'٩':9}
_INT = {v:k for k,v in _AR.items()}

def ar_to_int(s: str) -> int:
    result = 0
    for c in str(s):
        if c in _AR:
            result = result * 10 + _AR[c]
        elif c.isdigit():
            result = result * 10 + int(c)
    return result

def int_to_ar(n: int) -> str:
    if n <= 0:
        return '٠'
    digits = []
    while n:
        digits.append(_INT[n % 10])
        n //= 10
    return ''.join(reversed(digits))


# ─── System Prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are an expert in Shia Islamic hadith sciences (ʿilm al-rijāl). You are processing entries from "al-Mufīd min Muʿjam Rijāl al-Ḥadīth" by Muḥammad al-Jawāhirī (a summary of Sayyid al-Khoei's rijāl assessments).

A NARRATOR INDEX will be provided showing [entry_number] → arabic_name for entries referenced by the batch. Use it to resolve all cross-references:
- "متحد مع X" (identical to X) → extract the name, look it up in the index to get the entry number
- "الثقة الآتي" / "لاحقه" → the NEXT sequential entry in the index
- "السابق" / "سابقه" / "المتقدم" → the PREVIOUS sequential entry
- "الثقة الآتي [number]" → literal entry number given
- "راجع / انظر [number]" → the explicit entry number referenced

For each entry, extract ALL fields into a JSON object:

BASIC IDENTITY:
- "name_ar": Full Arabic name as written (after stripping any leading entry numbers)
- "name_en": English transliteration (e.g., "Ahmad ibn Muhammad ibn 'Isa al-Ash'ari al-Qummi")
- "father": Father's name if mentioned (via بن/ابن), null if not
- "grandfather": Grandfather's name if mentioned, null if not
- "nasab": Full lineage chain as string (e.g., "ibn X ibn Y ibn Z") if more than father, null otherwise
- "kunyah": Teknonym if present (e.g., "أبو الحسين"), null if not
- "laqab": Nickname/title if present (e.g., "اللؤلؤي", "الأحمر"), null if not
- "nisba": Geographic/tribal attribution (e.g., "الكوفي", "القمي", "الأشعري"), null if not

STATUS RULING:
- "status": EXACTLY one of: "thiqah", "majhul", "daif", "mamduh", "hasan", "muwaththaq", "unspecified"
  ثقة=thiqah | مجهول/مهمل=majhul | ضعيف=daif | ممدوح=mamduh | حسن=hasan | موثق=muwaththaq | no ruling→unspecified
- "status_detail": Exact Arabic phrase (e.g., "ثقة ثقة"), null if none
- "status_source": Who gave the ruling (e.g., "النجاشي", "الشيخ", "الكشي"), null if unstated
- "sect": Any specific theological/sectarian affiliation mentioned (e.g., "واقفي" (waqifi), "فطحي" (fatahi), "زيدي" (zaydi), "عامي" (aammi/sunni), "غالي" (ghali/extremist)). null if unstated.

NARRATION CONNECTIONS:
- "narrates_from_imams": Array of Imams they narrate from. Use ONLY these standard forms:
  ["رسول الله (ص)", "أمير المؤمنين (ع)", "الحسن (ع)", "الحسين (ع)", "السجاد (ع)",
   "الباقر (ع)", "الصادق (ع)", "الكاظم (ع)", "الرضا (ع)", "الجواد (ع)",
   "الهادي (ع)", "العسكري (ع)", "المهدي (عج)"]
  أبي عبد الله=الصادق | أبي جعفر=الباقر | أبي الحسن alone=الكاظم or الهادي (context) | أبي الحسن الرضا=الرضا | أبي محمد=العسكري
  Empty [] if none.
- "companions_of": Which Imam they are listed as companion of (أصحاب), null if not stated
- "narrates_from_narrators": Array of NAMED non-Imam narrators they narrate FROM (روى عن X where X is not an Imam). Extract the Arabic name as it appears. Empty [] if none.
- "narrated_from_by": Array of NAMED narrators who narrate FROM this person (روى عنه X). Extract the Arabic name as it appears. Empty [] if none.

BOOKS AND SOURCES:
- "books": Array of hadith books they appear in. Use: ["الكافي", "التهذيب", "الاستبصار", "الفقيه", "كامل الزيارات", "تفسير القمي"]. Empty [] if none.
- "hadith_count": Integer number of narrations if mentioned (روى ٤٢ رواية → 42), null if not
- "has_book": true if "له كتاب" or "له أصل" is mentioned, false otherwise
- "tariq_status": "sahih" if طريق...صحيح, "daif" if طريق...ضعيف, null if not mentioned

ALIASES AND IDENTITY:
- "aliases": Array of OTHER Arabic name forms this person is also known by. Empty [] if none.
- "alias_entry_nums": Array of entry numbers of those aliases (as Arabic numeral strings). Empty [] if none.
- "same_as_entry_nums": Array of entry numbers (Arabic numeral strings) that this person is CONFIRMED identical to (متحد مع). Use the NARRATOR INDEX to resolve names to entry numbers. Empty [] if none and no متحد مع mentioned.
- "same_as_names": Array of Arabic names this person is confirmed identical to. Empty [] if none.
- "identity_confidence": EXACTLY one of:
  - "certain" — source explicitly states متحد مع with a name or number
  - "probable" — source implies identity (e.g., "يشبه أن يكون هو") or context strongly suggests it
  - "uncertain" — same name exists elsewhere but no explicit cross-reference; ambiguous
  - "unique" — no cross-references at all; this appears to be a distinct individual

DISAMBIGUATION:
- "scribal_error_noted": true if the source flags "تحريف" (scribal/copying error) in the name, false otherwise
- "disambiguation_notes": Brief English explanation of any identity ambiguity, naming differences, or other complexity. null if entry is straightforward.
- "period_hint": Any temporal clue from the text indicating generation/era (e.g., "أصحاب الصادق", "من أصحاب الكاظم والرضا"). null if none.

EXTRA:
- "notes": Any other important info not captured above (brief English). null if nothing extra.

CRITICAL RULES:
1. Respond ONLY with a valid JSON array. No markdown, no backticks, no explanation, no preamble.
2. The array must have EXACTLY one object per input entry, in the SAME ORDER as the input. DO NOT skip any entries!
3. Every field must be present in every object (use null, [], or false for missing data). NEVER rename the keys.
4. Be precise: if entry says only "من أصحاب الصادق" with no explicit ثقة/ضعيف, status = "unspecified".
5. For same_as_entry_nums: use the NARRATOR INDEX to resolve the referenced name to an entry number.
6. "الثقة الآتي" without a number = the NEXT entry in the NARRATOR INDEX sequence.
7. The laqab field is for nicknames/epithets. The nisba field is for geographic/tribal origin.
8. Extract narrates_from_narrators and narrated_from_by ONLY for non-Imam individuals mentioned by name.

OUTPUT TEMPLATE (Repeat this structure exactly for every entry):
[
  {
    "name_ar": "", "name_en": "", "father": null, "grandfather": null, "nasab": null,
    "kunyah": null, "laqab": null, "nisba": null, "status": "unspecified", "status_detail": null,
    "status_source": null, "sect": null, "narrates_from_imams": [], "companions_of": null,
    "narrates_from_narrators": [], "narrated_from_by": [], "books": [], "hadith_count": null,
    "has_book": false, "tariq_status": null, "aliases": [], "alias_entry_nums": [],
    "same_as_entry_nums": [], "same_as_names": [], "identity_confidence": "unique",
    "scribal_error_noted": false, "disambiguation_notes": null, "period_hint": null, "notes": null
  }
]"""

# Narrator-lists-only system prompt — used in pass 2 of two-pass giant-entry extraction
NARRATOR_LISTS_SYSTEM_PROMPT = """You are an expert in Shia Islamic hadith sciences.
Extract from the given rijal entry text two complete lists of narrator names.

Return ONLY a valid JSON object (no markdown, no explanation):
{
  "narrates_from_narrators": ["name1", "name2", ...],
  "narrated_from_by": ["name1", "name2", ...]
}

Rules:
- "narrates_from_narrators": ALL non-Imam individuals this person narrates FROM (روى عن X). Extract the Arabic name as it appears.
- "narrated_from_by": ALL narrators who narrate FROM this person (روى عنه X). Extract the Arabic name as it appears.
- Do NOT include the Imams (الصادق, الكاظم, etc.) in either list — only non-Imam narrators.
- Include EVERY name mentioned, no matter how long the list.
- If a list is empty, return [].
- Return ONLY the JSON object. Nothing else."""

GIANT_ENTRY_THRESHOLD = 30_000  # chars above which two-pass extraction is used


def _call_gemini_with_system(backend_obj: dict, system: str, user: str) -> str:
    """Call Gemini with a custom system prompt."""
    api_key = backend_obj["key"]
    model   = backend_obj["model"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents":          [{"parts": [{"text": user}]}],
        "generationConfig":  {"temperature": 0.0, "maxOutputTokens": 65536},
    }
    while True:
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        if resp.status_code == 429:
            log.warning("  [Gemini RPM — sleeping 65s...]"); time.sleep(65); continue
        if resp.status_code == 503:
            log.warning("  [Gemini 503 — sleeping 60s...]"); time.sleep(60); continue
        if resp.status_code != 200:
            raise RuntimeError(f"Gemini API Error {resp.status_code}: {resp.text}")
        break
    data = resp.json()
    try:
        return data['candidates'][0]['content']['parts'][0]['text']
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Gemini response: {data}")


def _call_deepseek_with_system(backend_obj: dict, system: str, user: str) -> str:
    """Call DeepSeek with a custom system prompt."""
    api_key = backend_obj["key"]
    model   = backend_obj["model"]
    payload = {
        "model": model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.0,
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    while True:
        resp = requests.post("https://api.deepseek.com/chat/completions",
                             json=payload, headers=headers, timeout=700)
        if resp.status_code == 429:
            log.warning("  [DeepSeek RPM — sleeping 20s...]"); time.sleep(20); continue
        if resp.status_code != 200:
            raise RuntimeError(f"DeepSeek API Error {resp.status_code}: {resp.text}")
        data = resp.json()
        if "error" in data:
            msg = data["error"].get("message", "")
            if "timeout" in msg.lower() or "unable to start" in msg.lower():
                log.warning("  [DeepSeek timeout — sleeping 30s...]"); time.sleep(30); continue
            raise RuntimeError(f"DeepSeek body error: {msg}")
        break
    return data['choices'][0]['message']['content']


def process_giant_entry_two_pass(backend: str, backend_obj, entry: dict,
                                  name_index: dict, n3_index: dict) -> dict:
    """
    Two-pass extraction for entries too large to fit in one API response.
      Pass 1 — all fields except narrator lists (small output, always fits)
      Pass 2 — narrates_from_narrators + narrated_from_by only (focused prompt)
    Returns merged result dict.
    """
    log.info(f"  Two-pass extraction for {len(entry['text']):,}-char entry")

    # Pass 1: standard extraction, suppress narrator lists to keep output small
    hint = (
        "\n\nIMPORTANT: This entry is very long. Extract all fields normally EXCEPT "
        "narrates_from_narrators and narrated_from_by — set both to []. "
        "A dedicated second pass will extract the complete narrator lists."
    )
    entry_p1 = dict(entry, text=entry['text'] + hint)
    log.info("  Pass 1: identity/status/imams...")
    result = process_batch(backend, backend_obj, [entry_p1], name_index, n3_index)[0]

    # Pass 2: narrator lists only via focused lightweight prompt
    log.info("  Pass 2: full narrator lists...")
    user_p2 = f"Extract all narrator connections from this rijal entry:\n\n{entry['text']}"
    for attempt in range(1, 4):
        try:
            if backend == "gemini":
                raw = _call_gemini_with_system(backend_obj, NARRATOR_LISTS_SYSTEM_PROMPT, user_p2)
            else:
                raw = _call_deepseek_with_system(backend_obj, NARRATOR_LISTS_SYSTEM_PROMPT, user_p2)
            raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            lists = json.loads(raw)
            result["narrates_from_narrators"] = lists.get("narrates_from_narrators", [])
            result["narrated_from_by"]        = lists.get("narrated_from_by", [])
            log.info(f"  Pass 2 done: {len(result['narrates_from_narrators'])} teachers, "
                     f"{len(result['narrated_from_by'])} students")
            break
        except Exception as e:
            log.warning(f"  Pass 2 attempt {attempt}/3 failed: {e}")
            if attempt == 3:
                log.error("  Pass 2 failed — narrator lists will be empty")

    return result


# ─── Cross-reference resolution ───────────────────────────────────────────────

# Patterns indicating a cross-reference to another entry
_XREF_NEXT = re.compile(r'(?:الآتي|لاحقه)')
_XREF_PREV = re.compile(r'(?:السابق|سابقه|المتقدم|المذكور\s+آنفاً?|المتأخر)')
_XREF_NUM  = re.compile(r'(?:برقم|راجع|انظر|ينظر)\s*(?:رقم\s*)?([\u0660-\u0669]+)')
_XREF_AFTER_ATI = re.compile(r'الآتي\s+([\u0660-\u0669]+)')
# Catch numbers after cross-ref keywords, allowing comma/space between
# e.g., "المتقدم ٨٣٨" or "المتقدم ، ١٠٢١" or "المتقدم ١٠٢١"
_XREF_INLINE_NUM = re.compile(r'(?:المتقدم|المتأخر|السابق)\s*[،,]?\s*([\u0660-\u0669]+)')
# Catch any standalone n3 number near متحد مع (e.g., "متحد مع ... ١٠٢١")
_XREF_MUTTAHID   = re.compile(r'متحد\s+مع\b.*?([\u0660-\u0669]+)')


def find_referenced_entries(batch_entries: list, name_index: dict,
                            n3_index: dict) -> dict[str, dict]:
    """
    Scan the batch text and return a dict of {n3_str → {name_ar, idx, ...}}
    for all cross-referenced entries NOT already in the batch.

    Uses n3_index to resolve Tehran edition numbers to idx values,
    then name_index to look up the name.

    For "الآتي" (next) / "السابق" (previous) without explicit number,
    uses idx+1 / idx-1 (sequential array position) which is always correct
    regardless of n3 gaps.
    """
    referenced: dict[str, dict] = {}
    batch_idxs = {e['idx'] for e in batch_entries}

    # Track which idx values we've already added
    referenced_idxs: set[int] = set()

    def add_ref_by_idx(idx_int: int):
        """Add a reference by sequential idx."""
        if idx_int < 0 or idx_int > 15544:
            return
        if idx_int in batch_idxs or idx_int in referenced_idxs:
            return
        idx_str = str(idx_int)
        if idx_str in name_index:
            info = name_index[idx_str]
            n3 = info['n3']
            # Use n3 as display key for the narrator index (book's numbering)
            # but include idx for disambiguation when n3 has duplicates
            display_key = f"{n3}" if n3 not in referenced else f"{n3}(idx={idx_str})"
            referenced[display_key] = {
                'name_ar': info['name_ar'],
                'n3': n3,
                'idx': idx_int,
            }
            referenced_idxs.add(idx_int)

    def add_ref_by_n3(n3_int: int):
        """Add a reference by Tehran edition number (n3).
        Since n3 can have duplicates, we add ALL idx values for that n3."""
        if n3_int < 1 or n3_int > 15678:
            return
        n3_ar = int_to_ar(n3_int)
        if n3_ar in n3_index:
            for idx_str in n3_index[n3_ar]:
                add_ref_by_idx(int(idx_str))

    for entry in batch_entries:
        text = entry['text']
        entry_idx = entry['idx']

        # الآتي / لاحقه → next entry
        if _XREF_NEXT.search(text):
            # Check if there's an explicit n3 number after الآتي
            m = _XREF_AFTER_ATI.search(text)
            if m:
                # Explicit n3 reference: look up by n3
                add_ref_by_n3(ar_to_int(m.group(1)))
            else:
                # No explicit number: use idx+1 (always correct, no gaps)
                add_ref_by_idx(entry_idx + 1)

        # السابق / المتقدم etc. → previous entry
        if _XREF_PREV.search(text):
            # Check for explicit number after the reference word
            m = _XREF_INLINE_NUM.search(text)
            if m:
                add_ref_by_n3(ar_to_int(m.group(1)))
            else:
                # No explicit number: use idx-1
                add_ref_by_idx(entry_idx - 1)

        # برقم / راجع / انظر [number] — always an explicit n3 reference
        for m in _XREF_NUM.finditer(text):
            add_ref_by_n3(ar_to_int(m.group(1)))

        # متحد مع ... [number] — identity cross-reference with inline number
        m = _XREF_MUTTAHID.search(text)
        if m:
            add_ref_by_n3(ar_to_int(m.group(1)))

    return referenced


def format_cross_ref_context(refs: dict[str, dict]) -> str:
    """Format the referenced entries into a readable context block."""
    if not refs:
        return ""
    lines = ["NARRATOR INDEX (for resolving cross-references in this batch):"]
    for display_key, data in sorted(refs.items(),
                                     key=lambda x: x[1].get('idx', 0)):
        n3 = data['n3']
        lines.append(f"  [{n3}] {data['name_ar']}")
    return '\n'.join(lines)


# ─── Prompt building ──────────────────────────────────────────────────────────

def build_user_prompt(entries: list, cross_ref_context: str) -> str:
    parts = []
    if cross_ref_context:
        parts.append(cross_ref_context)
        parts.append("")

    items = []
    for i, e in enumerate(entries):
        text = e['text']
        if len(text) > MAX_ENTRY_CHARS:
            text = text[:MAX_ENTRY_CHARS] + f"\n... [TRUNCATED — entry is {len(e['text']):,} chars; extract from available text only]"
            log.warning(f"  Entry #{e['n3']} (idx={e['idx']}) truncated: {len(e['text']):,} → {MAX_ENTRY_CHARS:,} chars")
        items.append(f"[{i}] Entry #{e['n3']}: {text}")
    parts.append(f"Extract structured rijāl data from these {len(entries)} entries:\n")
    parts.append('\n\n'.join(items))
    return '\n'.join(parts)


# ─── Ollama backend ───────────────────────────────────────────────────────────

def strip_thinking(text: str) -> str:
    """Remove Qwen3 <think>...</think> and DeepSeek <reasoning>...</reasoning> blocks."""
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'<reasoning>.*?</reasoning>', '', text, flags=re.DOTALL)
    return text.strip()

def call_ollama(model: str, user_prompt: str, error_hint: str = "") -> str:
    """Call Ollama API and return raw response text."""
    prompt = user_prompt
    if error_hint:
        prompt = f"{user_prompt}\n\nNOTE: Previous attempt failed with: {error_hint}. Fix and return valid JSON only."

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": MAX_TOKENS,
        },
    }

    resp = requests.post(OLLAMA_URL, json=payload, timeout=600)
    resp.raise_for_status()
    raw = resp.json()["message"]["content"]
    return strip_thinking(raw)


# ─── Claude backend ───────────────────────────────────────────────────────────

def call_claude(client, user_prompt: str, error_hint: str = "") -> str:
    """Call Claude API and return raw response text."""
    prompt = user_prompt
    if error_hint:
        prompt = f"{user_prompt}\n\nNOTE: Previous attempt failed with: {error_hint}. Fix and return valid JSON only."

    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = ""
    for block in message.content:
        if hasattr(block, "text"):
            text += block.text
    return text


# ─── Gemini backend ───────────────────────────────────────────────────────────

def call_gemini(backend_obj: dict, user_prompt: str, error_hint: str = "") -> str:
    """Call Gemini API and return raw response text."""
    api_key = backend_obj["key"]
    model   = backend_obj["model"]
    prompt  = user_prompt
    if error_hint:
        prompt = f"{user_prompt}\n\nNOTE: Previous attempt failed with: {error_hint}. Fix and return valid JSON only."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0, "responseMimeType": "application/json",
                             "maxOutputTokens": 65536}
    }
    headers = {"Content-Type": "application/json"}

    while True:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 429:
            err_msg = resp.text.lower()
            if "perday" in err_msg or "daily" in err_msg:
                raise RuntimeError("You have successfully maxed out your Daily Quota (RPD) for Gemini! The script will now safely exit and save. Come back tomorrow!")

            log.warning("  [API Speed Limit Hit (RPM)! Sleeping for 65 seconds to reset minute...]")
            time.sleep(65)
            continue

        if resp.status_code == 503:
            log.warning("  [Gemini 503 — service unavailable, sleeping 60s and retrying...]")
            time.sleep(60)
            continue

        if resp.status_code != 200:
            raise RuntimeError(f"Gemini API Error {resp.status_code}: {resp.text}")
        break

    data = resp.json()
    try:
        raw_text = data['candidates'][0]['content']['parts'][0]['text']
        return raw_text
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Gemini response structure: {data}")


# ─── DeepSeek backend ─────────────────────────────────────────────────────────

def call_deepseek(backend_obj: dict, user_prompt: str, error_hint: str = "") -> str:
    """Call DeepSeek API and return raw response text."""
    api_key = backend_obj["key"]
    model   = backend_obj["model"]
    prompt  = user_prompt
    if error_hint:
        prompt = f"{user_prompt}\n\nNOTE: Previous attempt failed with: {error_hint}. Fix and return valid JSON only."

    url = "https://api.deepseek.com/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    while True:
        resp = requests.post(url, json=payload, headers=headers, timeout=700)
        if resp.status_code == 429:
            log.warning("  [DeepSeek Rate Limit! Sleeping for 20 seconds...]")
            time.sleep(20)
            continue

        if resp.status_code != 200:
            raise RuntimeError(f"DeepSeek API Error {resp.status_code}: {resp.text}")

        data = resp.json()

        # DeepSeek sometimes returns 200 with an error payload (e.g. server timeout)
        if "error" in data:
            msg = data["error"].get("message", str(data["error"]))
            if "timeout" in msg.lower() or "unable to start" in msg.lower():
                log.warning(f"  [DeepSeek server timeout — sleeping 30s and retrying]: {msg}")
                time.sleep(30)
                continue
            raise RuntimeError(f"DeepSeek API error in response body: {msg}")

        break

    try:
        raw = data["choices"][0]["message"]["content"]
        return strip_thinking(raw)
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected DeepSeek response structure: {data}")

# ─── JSON parsing ─────────────────────────────────────────────────────────────

def parse_json_array(text: str, expected_count: int) -> Optional[list]:
    """
    Try to extract and parse a JSON array from the model response.
    Returns the list if successful, None otherwise.

    Tolerates receiving MORE items than expected (fused OCR entries where one
    raw block contains two narrators). Returns None only if count is LESS.
    """
    text = text.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split('\n')
        # Remove first line (```json or ```) and last line (```)
        text = '\n'.join(lines[1:-1] if lines[-1].strip() == '```' else lines[1:])
    text = text.strip()

    # Try direct parse
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        # If it's a dict with a 'results' key
        if isinstance(result, dict) and 'results' in result:
            return result['results']
    except json.JSONDecodeError:
        pass

    # Advanced Error Healing: Find first [ and iteratively peel trailing garbage
    start = text.find('[')
    if start != -1:
        candidate = text[start:]
        # Limit peeling to prevent infinite lag on massively broken strings
        max_peel = min(100, len(candidate) - 2)
        for i in range(max_peel):
            try:
                result = json.loads(candidate)
                if isinstance(result, list):
                    return result
            except json.JSONDecodeError:
                candidate = candidate[:-1].strip()

    return None


# ─── Batch processing ─────────────────────────────────────────────────────────

def process_batch(backend: str, backend_obj, entries: list,
                  name_index: dict, n3_index: dict) -> list:
    """
    Process a batch of entries and return extracted data.
    Retries up to MAX_RETRIES times on failure.
    """
    refs = find_referenced_entries(entries, name_index, n3_index)
    ctx  = format_cross_ref_context(refs)
    user = build_user_prompt(entries, ctx)

    error_hint = ""
    raw = ""
    for attempt in range(MAX_RETRIES):
        try:
            if backend == "ollama":
                raw = call_ollama(backend_obj, user, error_hint)
            elif backend == "gemini":
                raw = call_gemini(backend_obj, user, error_hint)
            elif backend == "deepseek":
                raw = call_deepseek(backend_obj, user, error_hint)
            else:
                raw = call_claude(backend_obj, user, error_hint)

            results = parse_json_array(raw, len(entries))
            if results is None:
                raise ValueError("Could not find a JSON array in response")
            if len(results) < len(entries):
                raise ValueError(
                    f"Expected {len(entries)} items, got {len(results)} (too few — entries may have been skipped)"
                )
            if len(results) > len(entries):
                log.warning(
                    f"  Got {len(results)} items for {len(entries)} entries — "
                    f"one entry likely contains fused narrators. Accepting all {len(results)} items."
                )
            return results

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            error_hint = str(e)
            log.warning(f"  Attempt {attempt+1}/{MAX_RETRIES} failed: {e}")
            if raw:
                dump_path = SCRIPT_DIR / f"raw_error_batch_{attempt}.txt"
                dump_path.write_text(raw, encoding="utf-8")
                log.warning(f"  Dumped raw response to {dump_path.name}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)

        except requests.exceptions.RequestException as e:
            log.error(f"  Connection error: {e}")
            raise

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for batch")


# ─── File I/O helpers ─────────────────────────────────────────────────────────

def load_entries() -> list:
    if not ENTRIES_FILE.exists():
        log.error(f"Cannot find {ENTRIES_FILE}")
        sys.exit(1)
    with open(ENTRIES_FILE, 'r', encoding='utf-8') as f:
        entries = json.load(f)
    log.info(f"Loaded {len(entries):,} entries from {ENTRIES_FILE.name}")
    return entries

def load_name_index() -> dict:
    if not NAME_INDEX_FILE.exists():
        log.info("Name index not found. Run 'python rijal_prepass.py' first.")
        log.info("Proceeding WITHOUT cross-reference context (reduced accuracy).")
        return {}
    with open(NAME_INDEX_FILE, 'r', encoding='utf-8') as f:
        idx = json.load(f)
    log.info(f"Loaded name index: {len(idx):,} entries (keyed by idx)")
    return idx

def load_n3_index() -> dict:
    if not N3_INDEX_FILE.exists():
        log.info("N3 index not found. Run 'python rijal_prepass.py' first.")
        log.info("Proceeding WITHOUT n3 cross-reference lookup.")
        return {}
    with open(N3_INDEX_FILE, 'r', encoding='utf-8') as f:
        idx = json.load(f)
    log.info(f"Loaded n3 index: {len(idx):,} unique n3 values")
    return idx

def load_database() -> dict:
    if DATABASE_FILE.exists():
        with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
            db = json.load(f)
        log.info(f"Loaded existing database: {len(db):,} narrators")
        return db
    return {}

def save_database(db: dict):
    with open(DATABASE_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"last_idx": -1, "total_processed": 0, "total_errors": 0}

def save_progress(progress: dict):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)

def log_error(entry: dict, msg: str):
    with open(ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(f"Entry idx={entry.get('idx','?')} n3={entry.get('n3','?')}: {msg}\n")


# ─── Stats ────────────────────────────────────────────────────────────────────

def print_stats(db: dict):
    if not db:
        print("Database is empty. Run processing first.")
        return
    total = len(db)
    status_counts, imam_counts, book_counts = {}, {}, {}
    with_books = with_aliases = with_same_as = uncertain = 0

    for n in db.values():
        s = n.get('status', 'unspecified')
        status_counts[s] = status_counts.get(s, 0) + 1
        for im in (n.get('narrates_from_imams') or []):
            imam_counts[im] = imam_counts.get(im, 0) + 1
        for b in (n.get('books') or []):
            book_counts[b] = book_counts.get(b, 0) + 1
        if n.get('has_book'):           with_books += 1
        if n.get('aliases'):            with_aliases += 1
        if n.get('same_as_entry_nums'): with_same_as += 1
        if n.get('identity_confidence') == 'uncertain': uncertain += 1

    print(f"\n{'='*60}")
    print(f"  RIJĀL DATABASE STATISTICS (v2 — idx-keyed)")
    print(f"{'='*60}")
    print(f"  Total narrators:        {total:>6,}")
    print(f"  With books/uṣūl:        {with_books:>6,}")
    print(f"  With aliases:           {with_aliases:>6,}")
    print(f"  With identity cross-ref:{with_same_as:>6,}")
    print(f"  Uncertain identities:   {uncertain:>6,}")
    print(f"\n  STATUS DISTRIBUTION:")
    for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        bar = '█' * int(c / total * 40)
        print(f"    {s:15s} {c:5,} ({c/total*100:5.1f}%) {bar}")
    print(f"\n  NARRATION FROM IMAMS:")
    for im, c in sorted(imam_counts.items(), key=lambda x: -x[1])[:14]:
        print(f"    {im:30s} {c:5,}")
    print(f"\n  BOOK APPEARANCES:")
    for b, c in sorted(book_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {b:30s} {c:5,}")
    print(f"{'='*60}\n")


# ─── v1 → v2 Migration ───────────────────────────────────────────────────────

def detect_and_warn_v1_db(db: dict) -> bool:
    """Check if an existing database uses n3 keys (v1 format) and warn."""
    if not db:
        return False
    sample_keys = list(db.keys())[:10]
    # v1 keys are Arabic-Indic numerals (n3); v2 keys are plain integer strings
    has_arabic = any(any(c in _AR for c in k) for k in sample_keys)
    has_plain  = any(k.isdigit() for k in sample_keys)

    if has_arabic and not has_plain:
        log.warning("=" * 60)
        log.warning("DETECTED v1 DATABASE (keyed by n3 Tehran edition numbers)")
        log.warning("This format has ~13.5% data loss from n3 collisions.")
        log.warning("You MUST start fresh with --start 0 for accurate data.")
        log.warning("Rename or delete the old rijal_database.json first.")
        log.warning("=" * 60)
        return True
    return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Rijāl Database Builder v2 (idx-keyed)")
    parser.add_argument("--deepseek", action="store_true",
                        help="Use DeepSeek API instead of others")
    parser.add_argument("--deepseek-model", default="deepseek-chat",
                        help="DeepSeek model name (default: deepseek-chat)")
    parser.add_argument("--gemini", action="store_true",
                        help="Use Gemini API instead of Ollama/Claude")
    parser.add_argument("--gemini-model", default="gemini-2.5-flash",
                        help="Gemini model name (default: gemini-2.5-flash)")
    parser.add_argument("--ollama", action="store_true",
                        help="Use Ollama (local) instead of Claude API")
    parser.add_argument("--model", default="qwen3:14b",
                        help="Ollama model name (default: qwen3:14b)")
    parser.add_argument("--start", type=int, default=None,
                        help="Start from this entry index (overrides saved progress)")
    parser.add_argument("--count", type=int, default=None,
                        help="Process this many entries then stop")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Entries per API call (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--stats", action="store_true",
                        help="Show database statistics and exit")
    parser.add_argument("--delay", type=float, default=None,
                        help="Seconds between batches (default: 0.5 Ollama, 1.0 Claude)")
    parser.add_argument("--entries-file", type=Path, default=None,
                        help="Path to entries JSON (default: rijal_entries.json)")
    parser.add_argument("--db-file", type=Path, default=None,
                        help="Path to output database JSON (default: rijal_database.json)")
    parser.add_argument("--progress-file", type=Path, default=None,
                        help="Path to progress JSON (default: rijal_progress.json)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel API workers for wave-based processing (default: 1)")
    parser.add_argument("--name-index-file", type=Path, default=None,
                        help="Path to name index JSON (default: rijal_name_index.json)")
    parser.add_argument("--n3-index-file", type=Path, default=None,
                        help="Path to n3 index JSON (default: rijal_n3_index.json)")
    parser.add_argument("--rerun-large", type=int, default=None, metavar="CHARS",
                        help="Re-process all entries with text longer than CHARS (no truncation). "
                             "Overwrites existing DB entries. Use with a long-context model.")
    parser.add_argument("--skip-large", type=int, default=None, metavar="CHARS",
                        help="Skip entries with text longer than CHARS during main run "
                             "(write a stub). Use --rerun-large with Gemini to fill them in later.")
    args = parser.parse_args()

    # Override module-level file paths if custom paths were given
    global ENTRIES_FILE, DATABASE_FILE, PROGRESS_FILE, ERROR_LOG, NAME_INDEX_FILE, N3_INDEX_FILE
    if args.entries_file:
        ENTRIES_FILE = args.entries_file.resolve()
    if args.db_file:
        DATABASE_FILE = args.db_file.resolve()
    if args.progress_file:
        PROGRESS_FILE = args.progress_file.resolve()
    if args.name_index_file:
        NAME_INDEX_FILE = args.name_index_file.resolve()
    if args.n3_index_file:
        N3_INDEX_FILE = args.n3_index_file.resolve()
    if args.db_file or args.entries_file:
        ERROR_LOG = (DATABASE_FILE.parent / (DATABASE_FILE.stem + "_errors.log"))

    if args.stats:
        print_stats(load_database())
        return

    # ── Backend setup ────────────────────────────────────────────────────────
    if args.deepseek:
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            log.error("DEEPSEEK_API_KEY environment variable not set.")
            sys.exit(1)
        backend = "deepseek"
        backend_obj = {"key": api_key, "model": args.deepseek_model}
        delay = args.delay if args.delay is not None else 0.5
        log.info(f"Backend: DeepSeek API — model: {args.deepseek_model}")
    elif args.gemini:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            log.error("GEMINI_API_KEY environment variable not set.")
            sys.exit(1)
        backend = "gemini"
        backend_obj = {"key": api_key, "model": args.gemini_model}
        delay = args.delay if args.delay is not None else 0.5
        log.info(f"Backend: Gemini API — model: {args.gemini_model}")
    elif args.ollama:
        backend = "ollama"
        backend_obj = args.model
        delay = args.delay if args.delay is not None else DEFAULT_DELAY
        log.info(f"Backend: Ollama — model: {args.model}")

        # Quick connectivity check
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=5)
            r.raise_for_status()
            models = [m["name"] for m in r.json().get("models", [])]
            if args.model not in models and not any(args.model in m for m in models):
                log.warning(f"Model '{args.model}' not found in Ollama. Available: {models}")
                log.warning("Run: ollama pull qwen3:14b")
        except requests.exceptions.RequestException:
            log.error("Cannot connect to Ollama at localhost:11434")
            log.error("Start Ollama with: ollama serve")
            sys.exit(1)
    else:
        if not HAS_ANTHROPIC:
            log.error("anthropic package not installed: pip install anthropic")
            sys.exit(1)
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            log.error("ANTHROPIC_API_KEY not set.")
            sys.exit(1)
        backend = "claude"
        backend_obj = anthropic.Anthropic(api_key=api_key)
        delay = args.delay if args.delay is not None else CLAUDE_DELAY
        log.info(f"Backend: Claude API — model: {CLAUDE_MODEL}")

    # ── Load data ────────────────────────────────────────────────────────────
    entries    = load_entries()
    db         = load_database()
    progress   = load_progress()
    name_index = load_name_index()
    n3_index   = load_n3_index()

    # ── Detect v1 database and warn ──────────────────────────────────────────
    if detect_and_warn_v1_db(db):
        response = input("Continue anyway? Old entries will NOT be overwritten. [y/N] ")
        if response.lower() != 'y':
            log.info("Exiting. Rename/delete old database and re-run with --start 0.")
            sys.exit(0)

    # ── --rerun-large: re-process oversized entries with full context ─────────
    if args.rerun_large is not None:
        threshold = args.rerun_large
        all_large = [e for e in entries if len(e.get('text', '')) > threshold]
        # Skip entries already successfully rerun (avoid wasting daily quota)
        large = [e for e in all_large
                 if not db.get(str(e['idx']), {}).get('_rerun_large')]
        skipped_count = len(all_large) - len(large)
        log.info(f"--rerun-large {threshold}: {len(all_large)} large entries total, "
                 f"{skipped_count} already done, {len(large)} remaining")
        if not large:
            log.info("All large entries already processed. Nothing to do.")
            return

        # Temporarily disable truncation for this run
        global MAX_ENTRY_CHARS
        orig_max = MAX_ENTRY_CHARS
        MAX_ENTRY_CHARS = 10_000_000   # effectively unlimited

        processed = errors = 0
        start_time = time.time()
        for i, entry in enumerate(large):
            log.info(f"[{i+1}/{len(large)}] idx={entry['idx']} n3={entry['n3']} "
                     f"({len(entry['text']):,} chars)")
            try:
                if len(entry['text']) > GIANT_ENTRY_THRESHOLD:
                    result = process_giant_entry_two_pass(
                        backend, backend_obj, entry, name_index, n3_index)
                else:
                    result = process_batch(backend, backend_obj, [entry], name_index, n3_index)[0]
                result["_entry_idx"]  = entry["idx"]
                result["_num_najaf"]  = entry["n1"]
                result["_num_beirut"] = entry["n2"]
                result["_num_tehran"] = entry["n3"]
                result["_raw"]        = entry["text"]
                result["_rerun_large"] = True
                db[str(entry["idx"])] = result
                processed += 1
            except Exception as e:
                log.error(f"  FAILED idx={entry['idx']}: {e}")
                log_error(entry, str(e))
                errors += 1
                continue

            save_database(db)
            if delay > 0:
                time.sleep(delay)

        MAX_ENTRY_CHARS = orig_max
        elapsed = time.time() - start_time
        log.info(f"Done. Re-processed {processed}/{len(large)} entries "
                 f"({errors} errors) in {elapsed:.0f}s")
        save_database(db)
        return

    # ── Determine range ──────────────────────────────────────────────────────
    start_idx  = args.start if args.start is not None else (progress["last_idx"] + 1)
    max_count  = args.count if args.count is not None else len(entries)
    end_idx    = min(start_idx + max_count, len(entries))
    batch_size = args.batch_size

    if start_idx >= len(entries):
        log.info("All entries have been processed!")
        print_stats(db)
        return

    total_to_process = end_idx - start_idx
    workers = max(1, args.workers)
    log.info(f"Processing entries {start_idx}–{end_idx-1} ({total_to_process:,} entries)")
    log.info(f"Batch size: {batch_size} | Workers: {workers} | Delay: {delay}s | DB: {len(db):,} existing")
    log.info(f"Database key: idx (sequential, collision-free)")

    # ── Build full batch list ────────────────────────────────────────────────
    all_batches = []
    for bs in range(start_idx, end_idx, batch_size):
        be = min(bs + batch_size, end_idx)
        all_batches.append((bs, be, entries[bs:be]))

    def _write_results_to_db(batch: list, results: list):
        """Write extracted results into db dict (call with db_lock held if parallel)."""
        for i, result in enumerate(results):
            if i < len(batch):
                entry = batch[i]
                key   = str(entry["idx"])
            else:
                entry = batch[-1]
                overflow_idx = i - len(batch) + 1
                key   = f"{entry['idx']}_overflow_{overflow_idx}"
                log.info(f"  Storing overflow item {overflow_idx} under key '{key}'")
            result["_entry_idx"]  = entry["idx"]
            result["_num_najaf"]  = entry["n1"]
            result["_num_beirut"] = entry["n2"]
            result["_num_tehran"] = entry["n3"]
            result["_raw"]        = entry["text"]
            if i >= len(batch):
                result["_fused_overflow"] = True
            db[key] = result

    skip_large_threshold = args.skip_large  # None or int

    def _write_stubs_for_skipped(skipped: list):
        """Write placeholder DB entries for entries skipped due to --skip-large."""
        for entry in skipped:
            key = str(entry["idx"])
            if key not in db:   # don't overwrite a real entry from a previous run
                db[key] = {
                    "_entry_idx":        entry["idx"],
                    "_num_najaf":        entry["n1"],
                    "_num_beirut":       entry["n2"],
                    "_num_tehran":       entry["n3"],
                    "_raw":              entry["text"][:200],
                    "_skipped_for_rerun": True,
                    "name_ar":           "",
                    "status":            "unspecified",
                }
                log.info(f"  Stub written for idx={entry['idx']} n3={entry['n3']} "
                         f"({len(entry['text']):,} chars) — awaiting Gemini rerun")

    processed  = 0
    errors     = 0
    start_time = time.time()
    db_lock    = threading.Lock()

    try:
        # ── Wave-based loop (works for workers=1 too) ────────────────────────
        for wave_start in range(0, len(all_batches), workers):
            wave = all_batches[wave_start : wave_start + workers]

            elapsed   = time.time() - start_time
            rate      = processed / elapsed if elapsed > 0 and processed > 0 else 0
            remaining = (total_to_process - processed) / rate if rate > 0 else 0
            log.info(
                f"Wave {wave_start//workers + 1} | "
                f"Batches idx {wave[0][0]}–{wave[-1][1]-1} | "
                f"Done: {processed}/{total_to_process} | "
                f"DB: {len(db):,} | "
                f"{'Rate: {:.1f}/s | ETA: {:.1f}min'.format(rate, remaining/60) if rate > 0 else 'estimating...'}"
            )

            # ── Filter out oversized entries if --skip-large is set ──────────
            if skip_large_threshold is not None:
                filtered_wave = []
                for bs, be, batch in wave:
                    normal = [e for e in batch if len(e.get('text', '')) <= skip_large_threshold]
                    large  = [e for e in batch if len(e.get('text', '')) >  skip_large_threshold]
                    if large:
                        _write_stubs_for_skipped(large)
                        processed += len(large)
                    if normal:
                        filtered_wave.append((bs, be, normal))
                wave = filtered_wave
                if not wave:
                    # Entire wave was large entries — advance progress and continue
                    wave_max_idx = all_batches[min(wave_start + workers - 1,
                                                   len(all_batches) - 1)][1] - 1
                    progress["last_idx"]        = wave_max_idx
                    progress["total_processed"] = len(db)
                    save_database(db)
                    save_progress(progress)
                    continue

            if workers == 1:
                # Sequential path — no thread overhead
                bs, be, batch = wave[0]
                try:
                    results = process_batch(backend, backend_obj, batch, name_index, n3_index)
                    _write_results_to_db(batch, results)
                    processed += len(batch)
                except Exception as e:
                    log.error(f"  Batch idx {bs}–{be-1} FATALLY FAILED: {e}")
                    log.error(f"  Stopping immediately to prevent holes in database.")
                    for entry in batch:
                        log_error(entry, str(e))
                    sys.exit(1)
                wave_max_idx = be - 1

            else:
                # Parallel path — submit all batches in this wave concurrently
                wave_results: dict = {}
                wave_max_idx = -1

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_map = {
                        executor.submit(
                            process_batch, backend, backend_obj, batch, name_index, n3_index
                        ): (bs, be, batch)
                        for bs, be, batch in wave
                    }
                    for future in as_completed(future_map):
                        bs, be, batch = future_map[future]
                        try:
                            results = future.result()
                            wave_results[(bs, be)] = (batch, results)
                            log.info(f"  [worker] Batch {bs}–{be-1} done ({len(results)} results)")
                        except Exception as e:
                            log.error(f"  Batch idx {bs}–{be-1} FATALLY FAILED: {e}")
                            log.error(f"  Stopping immediately to prevent holes in database.")
                            for entry in batch:
                                log_error(entry, str(e))
                            sys.exit(1)

                # Write all wave results to db (single-threaded after all done)
                with db_lock:
                    for (bs, be), (batch, results) in sorted(wave_results.items()):
                        _write_results_to_db(batch, results)
                        processed += len(batch)
                        wave_max_idx = max(wave_max_idx, be - 1)

            # Save after each wave
            progress["last_idx"]        = wave_max_idx
            progress["total_processed"] = len(db)
            progress["total_errors"]    = progress.get("total_errors", 0) + errors
            save_database(db)
            save_progress(progress)

            if delay > 0 and wave_start + workers < len(all_batches):
                time.sleep(delay)

    except KeyboardInterrupt:
        log.info("\nInterrupted by user. Progress saved — run again to resume.")

    save_database(db)
    save_progress(progress)

    elapsed = time.time() - start_time
    log.info(
        f"\nDone. Processed {processed:,} entries in {elapsed:.0f}s "
        f"({errors} errors). DB has {len(db):,} narrators."
    )
    print_stats(db)


if __name__ == "__main__":
    main()

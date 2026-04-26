#!/usr/bin/env python3
"""
alf_rajul_extractor.py
======================
Extracts tabaqah (generational layer) data from Sayyid Ghayth Shubar's
"Mu'jam al-Alf Rajul" OCR text (output_surya_bbox.txt).

What it does:
  1. Parses ~988 narrator entries from the OCR text
  2. Extracts: name, tabaqah number (1-12), tabaqah detail text, alt names
  3. Saves standalone alf_rajul_database.json
  4. Backs up rijal_database.json
  5. Cross-matches to rijal_database.json -> adds tabaqah fields
     - Stage 1: deterministic key matching (key4 + key3 + key2)
     - Stage 2: OCR-correction variants (ismail, sulayman, etc.)
     - Stage 3: DeepSeek disambiguation (if DEEPSEEK_API_KEY is set)

Tabaqah system (al-Burujirdi / Shubar -- 12 generations):
  1  al-Ula         Companions of Prophet              ~10-40 AH
  2  al-Thaniya     Companions of Imam Ali             ~40-70 AH
  3  al-Thalitha    Era of Imam Sajjad                 ~70-100 AH
  4  al-Rabia       Companions of Imam Baqir           ~100-130 AH
  5  al-Khamisa     Companions of Imam Sadiq           ~130-170 AH
  6  al-Sadisa      Companions of Imam Kazim           ~170-200 AH
  7  al-Sabia       Companions of Imams Ridha/Jawad    ~200-240 AH
  8  al-Thamina     Companions of Imams Hadi/Askari    ~240-270 AH
  9  al-Tasia       Early minor occultation            ~270-300 AH
  10 al-Ashira      Later minor occultation            ~300-330 AH
  11 al-Hadiya Ashar Early major occultation           ~330-360 AH
  12 al-Thaniya Ashar Classical compilation era        ~360-400 AH

Run:
  python alf_rajul_extractor.py --stats      # parse only, show counts
  python alf_rajul_extractor.py --dry-run    # parse + match, don't write
  python alf_rajul_extractor.py              # full run (deterministic only)
  python alf_rajul_extractor.py --deepseek   # include DeepSeek disambiguation
"""

import json
import os
import re
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# --- Paths -------------------------------------------------------------------
TXT_PATH  = Path("output_surya_bbox.txt")
ALF_DB    = Path("alf_rajul_database.json")
RIJAL_DB  = Path("rijal_database.json")
BACKUP    = Path("rijal_database_backup_pre_tabaqah.json")

STATS_ONLY  = "--stats"    in sys.argv
DRY_RUN     = "--dry-run"  in sys.argv
USE_DEEPSEEK = "--deepseek" in sys.argv

# --- Tabaqah ordinal -> integer ----------------------------------------------
TABAQAH_MAP = {
    'الأولى':         1,  'الاولى':       1,
    'الثانية':        2,
    'الثالثة':        3,
    'الرابعة':        4,
    'الخامسة':        5,
    'السادسة':        6,
    'السابعة':        7,
    'الثامنة':        8,
    'التاسعة':        9,
    'العاشرة':       10,
    'الحادية عشرة':  11,
    'الثانية عشرة':  12,
}

# Imam -> canonical tabaqah (used later for inference)
IMAM_TABAQAH = {
    'النبي':           1, 'رسول الله':     1, 'محمد':           1,
    'أمير المؤمنين':  2, 'علي بن أبي طالب': 2,
    'الحسن':          2,
    'الحسين':         2,
    'السجاد':         3, 'زين العابدين':   3,
    'الباقر':         4,
    'الصادق':         5,
    'الكاظم':         6,
    'الرضا':          7,
    'الجواد':         7,
    'الهادي':         8,
    'العسكري':        8,
    'المهدي':         9, 'القائم':         9, 'صاحب الأمر':    9,
}

# Tabaqah -> expected imam keys (for candidate filtering)
TABAQAH_IMAM_KEYS = {
    2: {'IMAM_ALI', 'IMAM_HASAN', 'IMAM_HUSAYN'},
    3: {'IMAM_SAJJAD'},
    4: {'IMAM_BAQIR'},
    5: {'IMAM_SADIQ'},
    6: {'IMAM_KADHIM'},
    7: {'IMAM_REZA', 'IMAM_JAWAD'},
    8: {'IMAM_HADI', 'IMAM_ASKARI'},
    9: {'IMAM_MAHDI'},
}

def text_to_tabaqah(text: str) -> int | None:
    """Extract tabaqah integer from a text fragment."""
    for word, num in sorted(TABAQAH_MAP.items(), key=lambda x: -len(x[0])):
        if word in text:
            return num
    return None

# --- Arabic normalisation ----------------------------------------------------
_DIAC = re.compile(r'[\u064b-\u065f\u0670]')
_ALEF = re.compile(r'[إأآا]')

def norm(t: str) -> str:
    t = _DIAC.sub('', t)
    t = _ALEF.sub('ا', t)
    t = t.replace('\u0643', '\u0643')   # Arabic Kaf (no-op, keep as-is)
    t = t.replace('\u06a9', '\u0643')   # Persian Keheh -> Arabic Kaf
    t = t.replace('\u06cc', '\u064a')   # Farsi Yeh -> Arabic Yeh
    t = t.replace('\u06cd', '\u064a')   # Arabic Letter Yeh With Tail -> Yeh
    t = t.replace('\u0649', '\u064a')   # Alef Maksura -> Yeh (for matching)
    t = t.replace('\u06c1', '\u0647')   # HEH GOAL -> HEH
    t = t.replace('ـ', '')
    t = re.sub(r'\s+', ' ', t).strip()
    # Strip definite article (ال) from each word for consistent matching.
    # Guard: only strip if the word is longer than 4 chars after ال,
    # so الله (4 chars) and الآل (4 chars) etc. are preserved.
    words = t.split()
    words = [w[2:] if w.startswith('ال') and len(w) > 4 else w for w in words]
    return ' '.join(words)

def key2(name: str) -> str:
    return ' '.join(norm(name).split()[:2])

def key3(name: str) -> str:
    return ' '.join(norm(name).split()[:3])

def key4(name: str) -> str:
    return ' '.join(norm(name).split()[:4])

def key5(name: str) -> str:
    return ' '.join(norm(name).split()[:5])

def keyfull(name: str) -> str:
    return norm(name)

# --- Arabic-Indic numeral -> int ---------------------------------------------
# Covers both Arabic-Indic (U+0660-U+0669) and Extended Arabic-Indic/Persian (U+06F0-U+06F9)
_AR_DIGIT = {chr(0x660 + i): str(i) for i in range(10)}
_AR_DIGIT.update({chr(0x6f0 + i): str(i) for i in range(10)})

def ar2int(s: str) -> int:
    return int(''.join(_AR_DIGIT.get(c, c) for c in s))

# --- OCR correction ----------------------------------------------------------

# Word-level OCR corrections: known wrong -> right replacements
# Main pattern: OCR reads م (meem) as ه (ha) in many names
WORD_CORRECTIONS: dict[str, str] = {
    'إسهاعيل': 'إسماعيل',
    'اسهاعيل': 'إسماعيل',
    'سليهان':  'سليمان',
    'سهاعة':   'سماعة',
    'سَهاعة':  'سماعة',
    'اليهاني': 'اليماني',
    'آبي':     'أبي',
}

def ocr_variants(name: str) -> list[str]:
    """
    Generate OCR-corrected name variants.
    Returns a list of corrected name strings (may be empty if no corrections apply).
    """
    variants: list[str] = []

    # Apply known word-level corrections
    corrected = name
    for wrong, right in WORD_CORRECTIONS.items():
        corrected = corrected.replace(wrong, right)
    if corrected != name:
        variants.append(corrected)

    # Word-by-word ه->م substitution (only in words that are not known ه-words)
    # This catches systematic OCR confusion not covered by the dict above
    _KNOWN_HEH_WORDS = {
        'بن', 'ابن', 'ابو', 'ابي', 'عبد', 'الله', 'هارون', 'هشام',
        'هيثم', 'همام', 'هلال', 'هانئ', 'هرمز', 'هرون', 'هند',
        'الهمداني', 'الهاشمي', 'الهذلي', 'هاني', 'مهران', 'مهزيار',
        'شهاب', 'شهر', 'مشهد', 'جهم', 'ابراهيم', 'إبراهيم',
    }
    words = name.split()
    for i, w in enumerate(words):
        w_stripped = re.sub(r'^(ال)', '', w)
        if 'ه' in w_stripped and norm(w) not in {norm(x) for x in _KNOWN_HEH_WORDS}:
            new_words = words[:]
            new_words[i] = w.replace('ه', 'م')
            candidate = ' '.join(new_words)
            if candidate != name and candidate not in variants:
                variants.append(candidate)

    return variants

# --- Section header detection ------------------------------------------------

_HEADER_PATTERNS = re.compile(
    r'^(المعمرون|المدلس|الضعفاء|أصحاب|ذوي|الأسناد|الأسانيد)',
    re.UNICODE
)

def is_section_header(name: str) -> bool:
    """Return True if this entry is a section header, not a real narrator."""
    if ':' in name or '：' in name:
        return True
    if _HEADER_PATTERNS.match(name):
        return True
    return False

# --- Parse the OCR text ------------------------------------------------------

ENTRY_HEADER_RE = re.compile(
    r'^([0-9\u0660-\u0669\u06f0-\u06f9]+)\s*[.\-]\s*([\u0600-\u06ff][^\n]{3,100}?)$',
    re.MULTILINE
)
ALT_NAMES_RE = re.compile(
    r'يرد بعنوان\s*[:：]\s*([\s\S]+?)(?=\n\n|\Z)',
    re.MULTILINE
)
BOLD_RE = re.compile(r'<b>([^<]+)</b>')

_TABAQAH_WORDS_PAT = (
    'الأولى|الاولى|الثانية|الثالثة|الرابعة|الخامسة|'
    'السادسة|السابعة|الثامنة|التاسعة|العاشرة|'
    'الحادية عشرة|الثانية عشرة'
)
PLAIN_CONCLUSION_RE = re.compile(
    r'(?:من|وهو|فهو|فإنه|يكون|عدّه|عده|فيكون|والصحيح|والمناسب)\s+'
    r'(?:من\s+)?'
    r'(?:صغار\s+|كبار\s+|أوائل\s+|أواخر\s+)?'
    r'(' + _TABAQAH_WORDS_PAT + r')',
)


def parse_txt(text: str) -> list[dict]:
    """Parse all narrator entries from the Surya OCR text."""
    start_pos = text.find('١. آدم بن أبي أياس')
    end_pos = text.find('فهرس التراجم')
    if start_pos == -1: start_pos = 0
    if end_pos == -1: end_pos = len(text)

    headers: list[tuple[int, int, int, str]] = []
    for m in ENTRY_HEADER_RE.finditer(text, pos=start_pos, endpos=end_pos):
        try:
            num = ar2int(m.group(1))
        except ValueError:
            continue
        if not (1 <= num <= 1100):
            continue
        name = m.group(2).strip()
        if name == 'الثالثة': continue
        headers.append((m.start(), m.end(), num, name))

    print(f"  Raw header candidates: {len(headers)}")

    # Remove bounding-box overlaps (exactly same name sequentially)
    unique_headers = []
    seen_names = set()
    for h in headers:
        clean_name = re.sub(r'[^أ-ي]', '', h[3])
        # Only add if we haven't seen this exact name recently
        if clean_name not in seen_names:
            seen_names.add(clean_name)
            unique_headers.append(h)
            
    headers = unique_headers
    print(f"  Unique real entries: {len(headers)}")

    entries: list[dict] = []
    for i, (start, end, num, name) in enumerate(headers):
        body_end = headers[i + 1][0] if i + 1 < len(headers) else len(text)
        body = text[end:body_end]

        bolds = BOLD_RE.findall(body)
        tab_bolds = [(b.strip(), text_to_tabaqah(b)) for b in bolds
                     if text_to_tabaqah(b) is not None]
        if tab_bolds:
            tabaqah_detail, tabaqah = tab_bolds[-1]
        else:
            tail = body[-600:]
            matches = PLAIN_CONCLUSION_RE.findall(tail)
            if matches:
                tabaqah_detail = matches[-1]
                tabaqah = TABAQAH_MAP.get(tabaqah_detail)
            else:
                tabaqah_detail = None
                tabaqah = None

        tabaqah_sub = None
        if tabaqah_detail:
            if 'صغار' in tabaqah_detail:
                tabaqah_sub = 'junior'
            elif 'كبار' in tabaqah_detail:
                tabaqah_sub = 'senior'

        alt_names: list[str] = []
        alt_match = ALT_NAMES_RE.search(body)
        if alt_match:
            raw = alt_match.group(1)
            raw = re.sub(r'<[^>]+>', '', raw)
            for part in re.split(r'[،,]', raw):
                cleaned = part.strip().rstrip('.').strip()
                if len(cleaned) >= 3 and re.search(r'[\u0600-\u06ff]', cleaned):
                    alt_names.append(cleaned)

        entries.append({
            'num':            num,
            'name_ar':        name,
            'tabaqah':        tabaqah,
            'tabaqah_detail': tabaqah_detail,
            'tabaqah_sub':    tabaqah_sub,
            'alt_names':      alt_names,
            '_raw':           body.strip()[:3000],
        })

    return entries


# --- Name-matching helpers ---------------------------------------------------

def build_rijal_index(db: dict) -> dict[str, list[str]]:
    """
    Build normalised key -> list of DB keys index.
    Indexes full name down to key2 for matching at all granularities.
    """
    idx: dict[str, list[str]] = defaultdict(list)
    for db_key, entry in db.items():
        name = entry.get('name_ar', '')
        if not name:
            continue
        prev = ''
        for keyfn in (keyfull, key5, key4, key3, key2):
            k = keyfn(name)
            if k != prev:
                idx[k].append(db_key)
                prev = k
    return idx


def _candidate_imam_keys(entry: dict) -> set[str]:
    """Extract imam keys from a rijal_db entry's companion/narrator fields."""
    result = set()
    for field in ('companions_of', 'narrates_from_imams'):
        val = entry.get(field) or []
        if isinstance(val, list):
            result.update(val)
        elif isinstance(val, str):
            result.add(val)
    return result


def filter_by_tabaqah(candidates: list[str], tabaqah: int | None,
                       rijal_db: dict) -> list[str]:
    """
    Narrow candidates using tabaqah -> expected imam-key mapping.
    Only filters if it reduces (not eliminates) candidates.
    """
    if tabaqah is None or tabaqah not in TABAQAH_IMAM_KEYS:
        return candidates
    expected = TABAQAH_IMAM_KEYS[tabaqah]
    filtered = [c for c in candidates
                if _candidate_imam_keys(rijal_db.get(c, {})) & expected]
    return filtered if 0 < len(filtered) < len(candidates) else candidates


def find_match(name: str, alt_names: list[str],
               idx: dict[str, list[str]], db: dict,
               tabaqah: int | None = None) -> tuple[str | None, list[str]]:
    """
    Try to find a single unambiguous match in rijal_database.
    Returns (db_key, []) on unique match, (None, candidates) if ambiguous,
    (None, []) if no candidates at all.
    """
    all_candidates: list[str] = []

    def try_keys(search_name: str) -> str | None:
        """Try full->key5->key4->key3->key2, return single match or None."""
        for keyfn in (keyfull, key5, key4, key3, key2):
            hits = idx.get(keyfn(search_name), [])
            if len(hits) == 1:
                return hits[0]
            all_candidates.extend(hits)
        return None

    # --- Stage 1: direct name matching ---
    result = try_keys(name)
    if result:
        return result, []

    # Alt names
    for alt in alt_names:
        result = try_keys(alt)
        if result:
            return result, []

    # --- Stage 2: OCR-corrected variants ---
    for variant in ocr_variants(name):
        result = try_keys(variant)
        if result:
            return result, []
        # Also try corrected alt names
    for alt in alt_names:
        for variant in ocr_variants(alt):
            result = try_keys(variant)
            if result:
                return result, []

    # --- Deduplicate candidates ---
    unique = list(dict.fromkeys(all_candidates))

    # Apply tabaqah filtering to reduce ambiguity
    if len(unique) > 1 and tabaqah:
        unique = filter_by_tabaqah(unique, tabaqah, db)

    # If filtering left a single candidate
    if len(unique) == 1:
        return unique[0], []

    return None, unique


# --- DeepSeek disambiguation -------------------------------------------------

_DISAMBIG_SYSTEM = (
    "You are an expert in Islamic hadith sciences (rijal al-hadith) specializing "
    "in Shia narrators. Your task is to identify which entry in a rijal database "
    "corresponds to a given narrator from Mu'jam al-Alf Rajul. "
    "Respond with ONLY a JSON object: {\"index\": <0-based integer>} or {\"index\": null} "
    "if none match. No explanations."
)

def _build_candidate_desc(entry: dict) -> str:
    """Build a compact description of a rijal_db candidate."""
    parts = []
    name = entry.get('name_ar', '')
    if name:
        parts.append(f"الاسم: {name}")
    companions = entry.get('companions_of') or []
    if companions:
        parts.append(f"أصحاب: {', '.join(companions[:3])}")
    imams = entry.get('narrates_from_imams') or []
    if imams:
        parts.append(f"يروي عن: {', '.join(imams[:3])}")
    status = entry.get('status', '')
    if status:
        parts.append(f"الحكم: {status}")
    nisba = entry.get('nisba', '') or entry.get('laqab', '')
    if nisba:
        parts.append(f"النسبة: {nisba}")
    return ' | '.join(parts)


def call_deepseek_disambig(api_key: str, model: str,
                           alf_entry: dict, candidates: list[str],
                           rijal_db: dict) -> str | None:
    """
    Ask DeepSeek to pick which candidate matches the alf_rajul entry.
    Returns a rijal_db key, or None.
    """
    if not HAS_REQUESTS:
        return None

    name = alf_entry.get('name_ar', '')
    tabaqah = alf_entry.get('tabaqah')
    bio_snippet = (alf_entry.get('_raw') or '')[:800]

    candidate_lines = []
    for i, c in enumerate(candidates[:12]):
        desc = _build_candidate_desc(rijal_db.get(c, {}))
        candidate_lines.append(f"{i}. {desc}")

    prompt = (
        f"الراوي من معجم الألف رجال:\n"
        f"الاسم: {name}\n"
        f"الطبقة: {tabaqah}\n"
        f"نبذة: {bio_snippet[:400]}\n\n"
        f"المرشحون من قاعدة بيانات الرجال:\n"
        + '\n'.join(candidate_lines) +
        "\n\nأي المرشحين (رقم 0-أساس) يطابق هذا الراوي؟ "
        "إذا لم يوجد تطابق واضح أجب بـ null."
    )

    url = "https://api.deepseek.com/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _DISAMBIG_SYSTEM},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.0,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code == 429:
                time.sleep(20)
                continue
            if resp.status_code != 200:
                print(f"  [DeepSeek error {resp.status_code}: {resp.text[:100]}]")
                return None
            raw = resp.json()["choices"][0]["message"]["content"]
            # Strip <think>...</think> blocks
            raw = re.sub(r'<think>[\s\S]*?</think>', '', raw).strip()
            # Parse JSON
            m = re.search(r'\{[^{}]*\}', raw)
            if m:
                data = json.loads(m.group())
                idx_val = data.get('index')
                if idx_val is not None and 0 <= idx_val < len(candidates):
                    return candidates[idx_val]
            return None
        except Exception as e:
            print(f"  [DeepSeek exception: {e}]")
            return None
    return None


# --- Main --------------------------------------------------------------------

def main():
    print(f"Reading {TXT_PATH} ...")
    text = TXT_PATH.read_text(encoding='utf-8')
    print(f"  {len(text):,} chars")

    print("Parsing entries ...")
    entries = parse_txt(text)
    print(f"  {len(entries):,} entries parsed")

    has_tabaqah   = sum(1 for e in entries if e['tabaqah'] is not None)
    has_alt_names = sum(1 for e in entries if e['alt_names'])
    print(f"  With tabaqah:   {has_tabaqah:,}")
    print(f"  With alt names: {has_alt_names:,}")

    dist: dict[int, int] = defaultdict(int)
    for e in entries:
        if e['tabaqah']:
            dist[e['tabaqah']] += 1
    print("\n  Tabaqah distribution:")
    labels = {1:'Companions',2:'T2',3:'T3',4:'Baqir era',5:'Sadiq era',
              6:'Kazim era',7:'Ridha era',8:'Hadi era',9:'T9',10:'T10',11:'T11',12:'T12'}
    for t in sorted(dist):
        bar = '#' * (dist[t] // 5)
        print(f"    {t:2d} ({labels.get(t,''):<12}): {dist[t]:4d}  {bar}")

    if STATS_ONLY:
        print("\n[--stats: no files written]")
        return

    # Build alf_db from parsed entries
    alf_db: dict[str, dict] = {}
    for e in entries:
        key = str(e['num'])
        while key in alf_db:
             key += "_2"
        alf_db[key] = {k: v for k, v in e.items() if k != '_raw'}
        alf_db[key]['_raw'] = e['_raw']

    # Merge with existing alf_rajul_database (preserves gap-fixer entries)
    if ALF_DB.exists() and not DRY_RUN:
        existing_alf = json.loads(ALF_DB.read_text(encoding='utf-8'))
        for key, entry in existing_alf.items():
            if key not in alf_db and entry.get('_source') == 'llm_gap_fixer':
                alf_db[key] = entry
        print(f"  Merged: {len(alf_db):,} total entries (parsed + gap-recovered)")

    if not DRY_RUN:
        ALF_DB.write_text(
            json.dumps(alf_db, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
        print(f"\nSaved {ALF_DB}  ({ALF_DB.stat().st_size / 1e6:.1f} MB)")
        print("Done. (Matching to merged DB is handled by infer_tabaqah.py!)")
        return



if __name__ == '__main__':
    main()

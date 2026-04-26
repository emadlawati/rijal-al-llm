#!/usr/bin/env python3
"""
OCR summary.pdf using Claude vision, then compare against rijal_db.json.
Extracts thiqah/mamduh narrator entries from al-Mufid min Mu'jam Rijal al-Hadith.
"""
import anthropic, base64, json, re, sys, io, os
from pathlib import Path
from rijal_resolver import normalize_ar, DATABASE_FILE

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

EXTRACTED_FILE = Path('summary_extracted.json')
REPORT_FILE    = Path('summary_comparison.txt')

PROMPT = """This is a page from al-Mufid min Mu'jam Rijal al-Hadith listing thiqah and mamduh narrators from Khoei's Mu'jam.
Each entry has the format: [Arabic number] – [name]: [grade rationale text]

Extract EVERY narrator entry on this page. Return a JSON array where each element is:
{"num": <integer>, "name": "<Arabic name only>", "grade": "<grade>"}

Rules:
- "num": convert the Arabic numeral to an integer (٤=4, ٤٩٤=494, ١٢٣٤=1234, etc.)
- "name": the narrator's name only, no grade text
- "grade": one of: "thiqah" (if وثقه/ثقة/موثق appears), "hasan" (if حسن/من الحسان), "mamduh" (praised but not explicitly thiqah)
- Skip page numbers shown in parentheses like (٧٦٤) at the bottom
- If an entry spans multiple lines, combine into one entry
- Do NOT include entries that are just continuation text (no number at start)

Return ONLY a valid JSON array, no markdown, no explanation."""


def arabic_to_int(s: str) -> int | None:
    """Convert Arabic-Indic numeral string to int."""
    arabic_digits = '٠١٢٣٤٥٦٧٨٩'
    western = '0123456789'
    table = str.maketrans(arabic_digits, western)
    converted = s.translate(table)
    try:
        return int(converted)
    except ValueError:
        return None


def ocr_all_pages(client) -> list[dict]:
    pages_dir = Path('summary_pages')
    page_files = sorted(pages_dir.glob('page_*.png'))
    print(f'Processing {len(page_files)} pages...')

    all_entries = []
    for page_file in page_files:
        with open(page_file, 'rb') as f:
            img_data = base64.standard_b64encode(f.read()).decode('utf-8')

        resp = client.messages.create(
            model='claude-opus-4-6',
            max_tokens=2048,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image', 'source': {'type': 'base64', 'media_type': 'image/png', 'data': img_data}},
                    {'type': 'text', 'text': PROMPT}
                ]
            }]
        )

        text = resp.content[0].text.strip()
        # Strip markdown fences if present
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'```\s*$', '', text, flags=re.MULTILINE)

        try:
            entries = json.loads(text)
            all_entries.extend(entries)
            print(f'  {page_file.name}: {len(entries)} entries', flush=True)
        except Exception as e:
            print(f'  {page_file.name}: PARSE ERROR – {e}', flush=True)
            print(f'    raw: {text[:300]}', flush=True)

    return all_entries


def build_num_index(db: dict) -> dict:
    """Build index: mu'jam entry number → DB key."""
    idx = {}
    for key, entry in db.items():
        for field in ('_num_najaf', '_num_beirut', '_num_tehran', '_entry_idx'):
            raw = entry.get(field)
            if raw is None:
                continue
            n = arabic_to_int(str(raw)) if isinstance(raw, str) else int(raw)
            if n is not None:
                idx.setdefault(n, []).append(key)
    return idx


def run_comparison(entries: list[dict], db: dict, num_idx: dict) -> str:
    """Compare extracted entries against DB. Returns report text."""
    lines = []
    lines.append(f'Summary PDF entries: {len(entries)}')
    lines.append('')

    missing_from_db   = []  # in PDF but no DB entry found by number
    wrong_status      = []  # found in DB but status doesn't match
    ok                = []  # correctly graded

    expected_positive = {'thiqah', 'hasan'}  # grades that should be in DB as thiqah/hasan

    for e in entries:
        num   = e.get('num')
        name  = e.get('name', '')
        grade = e.get('grade', '')

        db_keys = num_idx.get(num, [])
        if not db_keys:
            missing_from_db.append((num, name, grade))
            continue

        # Check status of matched DB entries
        db_entry = db.get(db_keys[0], {})
        db_status = db_entry.get('status', '')
        db_name   = db_entry.get('name_ar', '')

        if grade in ('thiqah', 'hasan'):
            if db_status not in ('thiqah', 'hasan'):
                wrong_status.append((num, name, grade, db_name, db_status, db_keys[0]))
            else:
                ok.append((num, name, grade))
        else:  # mamduh
            if db_status in ('daif',):
                wrong_status.append((num, name, grade, db_name, db_status, db_keys[0]))
            else:
                ok.append((num, name, grade))

    lines.append(f'MATCHED (correct status) : {len(ok)}')
    lines.append(f'WRONG STATUS in DB       : {len(wrong_status)}')
    lines.append(f'NOT FOUND in DB by num   : {len(missing_from_db)}')
    lines.append('')

    if wrong_status:
        lines.append('═' * 70)
        lines.append('WRONG STATUS — narrator is thiqah/mamduh in PDF but DB differs:')
        lines.append('═' * 70)
        for num, name, grade, db_name, db_status, db_key in wrong_status:
            lines.append(f'  [{num:>5}]  PDF={grade:<8}  DB={db_status:<12}  PDF-name: {name}')
            lines.append(f'           DB-name: {db_name}  (key={db_key})')
        lines.append('')

    if missing_from_db:
        lines.append('═' * 70)
        lines.append('NOT FOUND IN DB (entry number missing):')
        lines.append('═' * 70)
        for num, name, grade in missing_from_db:
            lines.append(f'  [{num:>5}]  PDF={grade:<8}  {name}')

    return '\n'.join(lines)


def get_client():
    """Get Anthropic client using API key or Claude Code OAuth token."""
    import json as _json, pathlib as _pathlib
    # 1. Prefer explicit env var
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if api_key:
        return anthropic.Anthropic(api_key=api_key)
    # 2. Fall back to Claude Code credentials file
    creds_file = _pathlib.Path.home() / '.claude' / '.credentials.json'
    if creds_file.exists():
        with open(creds_file) as f:
            creds = _json.load(f)
        token = (creds.get('claudeAiOauth') or {}).get('accessToken')
        if token:
            return anthropic.Anthropic(auth_token=token)
    print('ERROR: no Anthropic credentials found.')
    sys.exit(1)


def main():
    client = get_client()

    # Step 1: OCR (or load cached result)
    if EXTRACTED_FILE.exists():
        print(f'Loading cached OCR from {EXTRACTED_FILE}...')
        with open(EXTRACTED_FILE, 'r', encoding='utf-8') as f:
            entries = json.load(f)
        print(f'Loaded {len(entries)} entries.')
    else:
        entries = ocr_all_pages(client)
        with open(EXTRACTED_FILE, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        print(f'\nSaved {len(entries)} entries to {EXTRACTED_FILE}')

    # Step 2: Load DB
    print('Loading rijal DB...')
    with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
        db = json.load(f)
    num_idx = build_num_index(db)
    print(f'DB: {len(db)} entries, {len(num_idx)} unique entry numbers indexed.')

    # Step 3: Compare
    report = run_comparison(entries, db, num_idx)
    print('\n' + report)

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'\nReport saved to {REPORT_FILE}')


if __name__ == '__main__':
    main()

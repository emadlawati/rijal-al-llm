#!/usr/bin/env python3
"""
Extract thiqah/mamduh narrator entries directly from 349.pdf
(al-Mufid min Mu'jam Rijal al-Hadith), pages 1599 to end.

Replaces the OCR approach in ocr_summary.py — no API key required.
The summary section lists narrators approved by al-Khoei with their
Mu'jam entry numbers and a brief ruling explanation.

Entry format in the PDF (RTL rendering, extracted as):
    [name]: [ruling text]- [Arabic numeral]
Multi-line entries: number appears on the first line only;
continuation lines (without a trailing number) extend the ruling.
"""
import fitz  # PyMuPDF
import re, json, sys, io
from pathlib import Path
from rijal_resolver import normalize_ar, DATABASE_FILE

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

PDF_FILE       = Path('349.pdf')
START_PAGE     = 1598   # 0-indexed = page 1599 in book numbering
EXTRACTED_FILE = Path('mufid_extracted.json')
REPORT_FILE    = Path('mufid_comparison.txt')

# Arabic-Indic → Western digit translation table
_DIGIT_TABLE = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')


def arabic_to_int(s: str) -> int | None:
    try:
        return int(s.translate(_DIGIT_TABLE))
    except (ValueError, AttributeError):
        return None


# Match a line that ends with:  - [Arabic digits]
# Captures: (name_part):(ruling_partial)-(num)
_ENTRY_RE = re.compile(
    r'^([^:]+):\s*(.*?)-\s*([٠-٩]+)\s*$'
)

# Lines to discard entirely
_PAGE_NUM_RE  = re.compile(r'^\([٠-٩]+\)\s*$')          # book page numbers: (٧٦٤)
_SEPARATOR_RE = re.compile(r'^[-\s]{5,}$')               # -------------------- separator


def parse_grade(ruling: str) -> str:
    """Classify the ruling text into a grade string."""
    if any(w in ruling for w in (
        'وثقه', 'ثقة', 'ثقتان', 'وثاقته', 'موثوق',
        'أجمعت العصابة', 'أصحاب الإجماع', 'من أصحاب الإجماع',
        'عدة روايات', 'وثقه الشيخ صريحا',
    )):
        return 'thiqah'
    if any(w in ruling for w in (
        'من الحسان', 'لا أقل إنه من الحسان',
        'حسن', 'صالح', 'خصيصا', 'اعتمده',
        'يدل على الحسن', 'دال على الحسن',
    )):
        return 'hasan'
    if any(w in ruling for w in (
        'مجهول', 'مجهولة',
        'لا يدل على الوثاقة',
        'لم تثبت وثاقته', 'لم تثبت',
        'ضعيف',
    )):
        return 'daif_or_majhul'
    if 'ممدوح' in ruling:
        return 'mamduh'
    # Everything included in al-Mufid is at minimum mamduh
    return 'mamduh'


def extract_entries(pdf_path: Path, start_idx: int) -> list[dict]:
    """
    Open pdf_path, iterate pages from start_idx onward, extract and
    parse narrator entries. Returns list of {num, name, ruling, grade}.
    """
    doc = fitz.open(str(pdf_path))
    total = len(doc)
    print(f'PDF has {total} pages. Extracting pages {start_idx + 1}–{total}...')

    entries: list[dict] = []
    current: dict | None = None   # accumulator for the entry being built
    done = False

    def flush():
        if current:
            ruling = ' '.join(current['ruling_parts']).strip()
            entries.append({
                'num':    current['num'],
                'name':   current['name'],
                'ruling': ruling,
                'grade':  parse_grade(ruling),
            })

    for page_idx in range(start_idx, total):
        if done:
            break
        page = doc[page_idx]
        for line in page.get_text().splitlines():
            line = line.strip()

            # Skip blank lines and book page numbers like (٧٦٤)
            if not line or _PAGE_NUM_RE.match(line):
                continue

            # Footnote separator — everything after this on the last pages is footnotes
            if _SEPARATOR_RE.match(line):
                flush()
                current = None
                done = True
                break

            # Strip leading period-space artifact from RTL rendering
            line = line.lstrip('. \u200f\u200e')

            m = _ENTRY_RE.match(line)
            if m:
                flush()
                name_raw   = m.group(1).strip().rstrip('.')
                ruling_raw = m.group(2).strip()
                num        = arabic_to_int(m.group(3))
                if num is None:
                    current = None
                    continue
                current = {
                    'num':          num,
                    'name':         name_raw,
                    'ruling_parts': [ruling_raw] if ruling_raw else [],
                }
            elif current:
                # Continuation line — append to ruling
                current['ruling_parts'].append(line)

    flush()
    doc.close()
    return entries


# ── Comparison against rijal_db.json ────────────────────────────────────────

def build_num_index(db: dict) -> dict:
    """Index: Mu'jam entry number (int) → list of DB keys."""
    idx: dict[int, list[str]] = {}
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
    lines: list[str] = []
    lines.append(f'al-Mufid entries extracted : {len(entries)}')
    lines.append('')

    matched_ok    : list[tuple] = []
    wrong_status  : list[tuple] = []
    not_in_db     : list[tuple] = []

    for e in entries:
        num   = e['num']
        name  = e['name']
        grade = e['grade']

        db_keys = num_idx.get(num, [])
        if not db_keys:
            not_in_db.append((num, name, grade))
            continue

        db_entry  = db.get(db_keys[0], {})
        db_status = db_entry.get('status', '')
        db_name   = db_entry.get('name_ar', '')

        if grade in ('thiqah', 'hasan'):
            if db_status not in ('thiqah', 'hasan'):
                wrong_status.append((num, name, grade, db_name, db_status, db_keys[0]))
            else:
                matched_ok.append((num, name, grade))
        elif grade == 'daif_or_majhul':
            # al-Khoei discussed this narrator but concluded he is weak/unknown
            if db_status in ('thiqah', 'hasan'):
                wrong_status.append((num, name, grade, db_name, db_status, db_keys[0]))
            else:
                matched_ok.append((num, name, grade))
        else:  # mamduh
            if db_status == 'daif':
                wrong_status.append((num, name, grade, db_name, db_status, db_keys[0]))
            else:
                matched_ok.append((num, name, grade))

    lines.append(f'Matched (status OK)        : {len(matched_ok)}')
    lines.append(f'Wrong status in DB         : {len(wrong_status)}')
    lines.append(f'Not found in DB by number  : {len(not_in_db)}')
    lines.append('')

    if wrong_status:
        lines.append('═' * 72)
        lines.append('WRONG STATUS — al-Mufid grade differs from DB:')
        lines.append('═' * 72)
        for num, name, grade, db_name, db_status, db_key in wrong_status:
            lines.append(f'  [{num:>6}]  PDF={grade:<15}  DB={db_status:<12}  PDF-name: {name}')
            lines.append(f'            DB-name: {db_name}  (key={db_key})')
        lines.append('')

    if not_in_db:
        lines.append('═' * 72)
        lines.append('NOT FOUND IN DB (entry number missing from all edition fields):')
        lines.append('═' * 72)
        for num, name, grade in not_in_db:
            lines.append(f'  [{num:>6}]  PDF={grade:<15}  {name}')

    return '\n'.join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if EXTRACTED_FILE.exists():
        print(f'Loading cached extraction from {EXTRACTED_FILE}...')
        with open(EXTRACTED_FILE, 'r', encoding='utf-8') as f:
            entries = json.load(f)
        print(f'Loaded {len(entries)} entries.')
    else:
        entries = extract_entries(PDF_FILE, START_PAGE)
        with open(EXTRACTED_FILE, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        print(f'\nSaved {len(entries)} entries to {EXTRACTED_FILE}')

    print('\nLoading rijal DB...')
    with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
        db = json.load(f)
    num_idx = build_num_index(db)
    print(f'DB: {len(db)} entries, {len(num_idx)} unique entry numbers indexed.')

    report = run_comparison(entries, db, num_idx)
    print('\n' + report)

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'\nReport saved to {REPORT_FILE}')


if __name__ == '__main__':
    main()

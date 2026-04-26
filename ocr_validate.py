#!/usr/bin/env python3
"""
OCR Validation Pipeline for al-Mufid Summary
============================================

Validates the rijal database against al-Mufid min Mu'jam Rijal al-Hadith.
Can use:
1. Existing summary_extracted.json (if already OCR'd)
2. Local OCR (pytesseract/easyocr) as fallback
3. Claude vision API (if ANTHROPIC_API_KEY is set)

Usage:
    python ocr_validate.py --report          # Validate using cached extraction
    python ocr_validate.py --ocr-local       # Run local OCR first, then validate
    python ocr_validate.py --ocr-claude      # Run Claude vision OCR, then validate
"""

import json
import sys
import re
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

from rijal_resolver import normalize_ar, DATABASE_FILE

# ── Constants ───────────────────────────────────────────────────────────────

EXTRACTED_FILE = Path('summary_extracted.json')
PAGES_DIR = Path('summary_pages')
REPORT_FILE = Path('ocr_validation_report.txt')
CORRECTIONS_FILE = Path('ocr_suggested_corrections.json')

# ── Arabic numeral conversion ───────────────────────────────────────────────

_ARABIC_DIGITS = '٠١٢٣٤٥٦٧٨٩'
_WESTERN_DIGITS = '0123456789'
_ARABIC_TO_WESTERN = str.maketrans(_ARABIC_DIGITS, _WESTERN_DIGITS)


def arabic_to_int(s: str) -> Optional[int]:
    """Convert Arabic-Indic numeral string to int."""
    if not s:
        return None
    converted = s.translate(_ARABIC_TO_WESTERN)
    try:
        return int(converted)
    except ValueError:
        return None


# ── Local OCR Fallback ──────────────────────────────────────────────────────

def try_local_ocr() -> List[dict]:
    """Attempt local OCR using pytesseract or easyocr."""
    entries = []

    # Try pytesseract
    try:
        import pytesseract
        from PIL import Image
        print("Using pytesseract for local OCR...")
        for page_file in sorted(PAGES_DIR.glob('page_*.png')):
            text = pytesseract.image_to_string(Image.open(page_file), lang='ara')
            page_entries = _parse_ocr_text(text)
            entries.extend(page_entries)
            print(f"  {page_file.name}: {len(page_entries)} entries")
        return entries
    except ImportError:
        pass

    # Try easyocr
    try:
        import easyocr
        print("Using EasyOCR for local OCR...")
        reader = easyocr.Reader(['ar'])
        for page_file in sorted(PAGES_DIR.glob('page_*.png')):
            result = reader.readtext(str(page_file), detail=0)
            text = '\n'.join(result)
            page_entries = _parse_ocr_text(text)
            entries.extend(page_entries)
            print(f"  {page_file.name}: {len(page_entries)} entries")
        return entries
    except ImportError:
        pass

    print("WARNING: No local OCR engine available. Install pytesseract or easyocr.")
    return []


def _parse_ocr_text(text: str) -> List[dict]:
    """Parse raw OCR text into structured entries."""
    entries = []
    lines = text.split('\n')
    current_entry = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Match: [number] – [name]: [grade text]
        # Arabic numbers: ١ ٢ ٣ etc.
        m = re.match(r'^([٠١٢٣٤٥٦٧٨٩]+)\s*[-–]\s*(.+?)(?:\s*:\s*(.+))?$', line)
        if m:
            if current_entry:
                entries.append(current_entry)
            num = arabic_to_int(m.group(1))
            name = m.group(2).strip()
            grade_text = (m.group(3) or '').strip()
            grade = _extract_grade(grade_text)
            current_entry = {'num': num, 'name': name, 'grade': grade, 'raw': line}
        elif current_entry and line:
            # Continuation of previous entry
            current_entry['name'] += ' ' + line
            current_entry['raw'] += ' ' + line
            # Re-extract grade from combined text
            current_entry['grade'] = _extract_grade(current_entry['raw'])

    if current_entry:
        entries.append(current_entry)

    return entries


def _extract_grade(text: str) -> str:
    """Extract grade from Arabic text."""
    text = normalize_ar(text)
    if re.search(r'\bثقة\b|\bوثقه\b|\bالثقة\b', text):
        return 'thiqah'
    if re.search(r'\bحسن\b|\bمن\s+الحسان\b', text):
        return 'hasan'
    if re.search(r'\bممدوح\b|\bمدح\b', text):
        return 'mamduh'
    return 'unknown'


# ── DB Index Builder ────────────────────────────────────────────────────────

def build_num_index(db: Dict) -> Dict[int, List[str]]:
    """Build index: mu'jam entry number → DB key."""
    idx = defaultdict(list)
    for key, entry in db.items():
        for field in ('_num_najaf', '_num_beirut', '_num_tehran', '_entry_idx'):
            raw = entry.get(field)
            if raw is None:
                continue
            n = arabic_to_int(str(raw)) if isinstance(raw, str) else int(raw)
            if n is not None:
                idx[n].append(key)
    return dict(idx)


# ── Validation Engine ───────────────────────────────────────────────────────

class OCRValidationEngine:
    """Compares OCR-extracted entries against the rijal database."""

    def __init__(self, db: Dict, extracted_entries: List[dict]):
        self.db = db
        self.extracted = extracted_entries
        self.num_idx = build_num_index(db)

        # Results
        self.matched = []
        self.wrong_status = []
        self.missing_from_db = []
        self.missing_from_pdf = []  # DB entries not found in PDF

    def validate(self):
        """Run full validation."""
        pdf_nums = set()

        for entry in self.extracted:
            num = entry.get('num')
            if num is None:
                continue
            pdf_nums.add(num)

            name = entry.get('name', '')
            grade = entry.get('grade', 'unknown')

            db_keys = self.num_idx.get(num, [])
            if not db_keys:
                self.missing_from_db.append(entry)
                continue

            # Check status of matched DB entries
            db_entry = self.db.get(db_keys[0], {})
            db_status = db_entry.get('status', 'unspecified')
            db_name = db_entry.get('name_ar', '')

            if grade in ('thiqah', 'hasan'):
                if db_status not in ('thiqah', 'hasan'):
                    self.wrong_status.append({
                        'pdf': entry,
                        'db_key': db_keys[0],
                        'db_name': db_name,
                        'db_status': db_status,
                    })
                else:
                    self.matched.append(entry)
            elif grade == 'mamduh':
                if db_status in ('daif', 'majhul'):
                    self.wrong_status.append({
                        'pdf': entry,
                        'db_key': db_keys[0],
                        'db_name': db_name,
                        'db_status': db_status,
                    })
                else:
                    self.matched.append(entry)
            else:
                # Unknown grade — skip
                pass

        # Find DB entries that SHOULD be in al-Mufid but aren't
        # (al-Mufid only includes thiqah/mamduh narrators)
        self._check_pdf_coverage(pdf_nums)

    def _check_pdf_coverage(self, pdf_nums: set):
        """Check which DB entries with thiqah/hasan status are missing from PDF."""
        for key, entry in self.db.items():
            status = entry.get('status', '')
            if status not in ('thiqah', 'hasan'):
                continue

            # Check if any of this entry's numbers appear in the PDF
            nums = []
            for field in ('_num_najaf', '_num_beirut', '_num_tehran', '_entry_idx'):
                raw = entry.get(field)
                if raw is not None:
                    n = arabic_to_int(str(raw)) if isinstance(raw, str) else int(raw)
                    if n is not None:
                        nums.append(n)

            if nums and not any(n in pdf_nums for n in nums):
                self.missing_from_pdf.append({
                    'db_key': key,
                    'name_ar': entry.get('name_ar', ''),
                    'status': status,
                    'nums': nums,
                })

    def generate_report(self) -> str:
        """Generate human-readable validation report."""
        lines = []
        lines.append("=" * 70)
        lines.append("OCR VALIDATION REPORT")
        lines.append("=" * 70)
        lines.append(f"PDF entries processed: {len(self.extracted)}")
        lines.append(f"DB entries indexed: {len(self.db)}")
        lines.append("")

        lines.append(f"MATCHED (correct status): {len(self.matched)}")
        lines.append(f"WRONG STATUS in DB: {len(self.wrong_status)}")
        lines.append(f"NOT FOUND in DB by num: {len(self.missing_from_db)}")
        lines.append(f"DB thiqah/hasan MISSING from PDF: {len(self.missing_from_pdf)}")
        lines.append("")

        if self.wrong_status:
            lines.append("=" * 70)
            lines.append("WRONG STATUS — PDF says thiqah/hasan but DB differs:")
            lines.append("=" * 70)
            for item in self.wrong_status[:50]:
                pdf = item['pdf']
                lines.append(f"  [{pdf['num']:>5}]  PDF={pdf['grade']:<8}  DB={item['db_status']:<12}")
                lines.append(f"           PDF-name: {pdf['name']}")
                lines.append(f"           DB-name:  {item['db_name']}  (key={item['db_key']})")
            if len(self.wrong_status) > 50:
                lines.append(f"  ... and {len(self.wrong_status) - 50} more")
            lines.append("")

        if self.missing_from_db:
            lines.append("=" * 70)
            lines.append("NOT FOUND IN DB (entry number missing):")
            lines.append("=" * 70)
            for entry in self.missing_from_db[:30]:
                lines.append(f"  [{entry['num']:>5}]  {entry['name'][:60]}")
            if len(self.missing_from_db) > 30:
                lines.append(f"  ... and {len(self.missing_from_db) - 30} more")
            lines.append("")

        if self.missing_from_pdf:
            lines.append("=" * 70)
            lines.append("DB ENTRIES MARKED THIQAH/HASAN BUT NOT IN PDF:")
            lines.append("(May be: 1) OCR miss, 2) Entry not in al-Mufid, or 3) Different number)")
            lines.append("=" * 70)
            for item in self.missing_from_pdf[:30]:
                lines.append(f"  [{item['db_key']:>6}]  {item['name_ar'][:50]}  (status={item['status']}) nums={item['nums']}")
            if len(self.missing_from_pdf) > 30:
                lines.append(f"  ... and {len(self.missing_from_pdf) - 30} more")
            lines.append("")

        return '\n'.join(lines)

    def generate_corrections(self) -> List[dict]:
        """Generate list of high-confidence corrections for the DB."""
        corrections = []
        for item in self.wrong_status:
            pdf = item['pdf']
            # Only suggest when PDF is explicit thiqah and DB says otherwise
            if pdf['grade'] == 'thiqah' and item['db_status'] in ('daif', 'majhul', 'unspecified'):
                corrections.append({
                    'db_key': item['db_key'],
                    'db_name': item['db_name'],
                    'current_status': item['db_status'],
                    'suggested_status': 'thiqah',
                    'source': f"al-Mufid entry #{pdf['num']}",
                    'pdf_name': pdf['name'],
                    'confidence': 'high',
                })
        return corrections


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser(description="OCR Validation Pipeline")
    p.add_argument('--ocr-local', action='store_true', help='Run local OCR before validating')
    p.add_argument('--ocr-claude', action='store_true', help='Run Claude vision OCR before validating')
    p.add_argument('--report', action='store_true', help='Print validation report')
    p.add_argument('--corrections', action='store_true', help='Generate suggested corrections JSON')
    p.add_argument('--save-report', type=str, default=str(REPORT_FILE), help='Save report to file')
    args = p.parse_args()

    # ── Step 1: Get extracted entries ──────────────────────────────────────
    entries = []

    if EXTRACTED_FILE.exists() and not args.ocr_local and not args.ocr_claude:
        print(f"Loading cached extraction from {EXTRACTED_FILE}...")
        with open(EXTRACTED_FILE, 'r', encoding='utf-8') as f:
            entries = json.load(f)
        print(f"Loaded {len(entries)} entries.")

    elif args.ocr_local:
        entries = try_local_ocr()
        if entries:
            with open(EXTRACTED_FILE, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(entries)} entries to {EXTRACTED_FILE}")

    elif args.ocr_claude:
        print("Claude vision OCR requested. Delegating to ocr_summary.py...")
        import subprocess
        result = subprocess.run([sys.executable, 'ocr_summary.py'], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: ocr_summary.py failed: {result.stderr}")
            sys.exit(1)
        if EXTRACTED_FILE.exists():
            with open(EXTRACTED_FILE, 'r', encoding='utf-8') as f:
                entries = json.load(f)

    else:
        print("ERROR: No extracted data found. Run with --ocr-local or --ocr-claude first.")
        print(f"       Or ensure {EXTRACTED_FILE} exists.")
        sys.exit(1)

    # ── Step 2: Load DB ────────────────────────────────────────────────────
    print(f"Loading DB from {DATABASE_FILE}...")
    with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
        db = json.load(f)
    print(f"DB: {len(db)} entries")

    # ── Step 3: Validate ───────────────────────────────────────────────────
    print("Running validation...")
    engine = OCRValidationEngine(db, entries)
    engine.validate()

    # ── Step 4: Output ─────────────────────────────────────────────────────
    if args.report or not args.corrections:
        report = engine.generate_report()
        print('\n' + report)
        with open(args.save_report, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\nReport saved to {args.save_report}")

    if args.corrections:
        corrections = engine.generate_corrections()
        with open(CORRECTIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(corrections, f, ensure_ascii=False, indent=2)
        print(f"\nGenerated {len(corrections)} suggested corrections → {CORRECTIONS_FILE}")
        for c in corrections[:10]:
            print(f"  [{c['db_key']}] {c['db_name']}: {c['current_status']} → {c['suggested_status']} ({c['source']})")


if __name__ == '__main__':
    main()

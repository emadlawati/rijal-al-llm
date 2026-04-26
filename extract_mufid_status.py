#!/usr/bin/env python3
"""
Extract al-Khoei's final status verdicts from al-Mufid (7194.md)
================================================================
Parses the clean markdown text of المفيد من معجم رجال الحديث and extracts
the status verdict for every entry, keyed by the Najaf entry number.

Output: mufid_statuses.json — a dict keyed by Najaf entry number (str)
with fields: status, status_detail, status_source, n3 (Tehran number).
"""

import json
import re
import sys
from pathlib import Path

if sys.platform == "win32":
    import io
    if getattr(sys.stdout, 'encoding', '').lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

MUFID_PATH = Path(__file__).parent / "books_md" / "7194.md"
OUTPUT_PATH = Path(__file__).parent / "mufid_statuses.json"

# ── Status detection patterns ─────────────────────────────────────────────────
# Priority: detect in each entry text, ordered from most to least specific.

# Explicit thiqah keywords (al-Khoei's summary uses these exact phrases)
THIQAH_PATTERNS = [
    # Direct status words
    (r'\bثقة\s+ثقة\b', 'thiqah', 'ثقة ثقة'),
    (r'\bثقة\s+عين\s+صحيح\s+الحديث\b', 'thiqah', 'ثقة عين صحيح الحديث'),
    (r'\bثقة\s+عين\s+نقي\s+الحديث\b', 'thiqah', 'ثقة عين نقي الحديث'),
    (r'\bثقة\s+عين\s+كثير\s+الرواية\b', 'thiqah', 'ثقة عين كثير الرواية'),
    (r'\bثقة\s+عين\b', 'thiqah', 'ثقة عين'),
    (r'\bثقة\s+وجه\b', 'thiqah', 'ثقة وجه'),
    (r'\bثقة\s+عدل\b', 'thiqah', 'ثقة عدل'),
    (r'\bثقة\s+صدوق\b', 'thiqah', 'ثقة صدوق'),
    (r'\bثقة\s+صحيح\b', 'thiqah', 'ثقة صحيح'),
    (r'\bثقة\s+ثبت\b', 'thiqah', 'ثقة ثبت'),
    (r'\bثقة\s+جليل\b', 'thiqah', 'ثقة جليل'),
    (r'\bثقة\s+معتمد\b', 'thiqah', 'ثقة معتمد'),
    (r'\bثقة\s+واقفي\b', 'thiqah', 'ثقة'),  # thiqah but waqifi
    (r'\bثقة\s+فطحي\b', 'thiqah', 'ثقة'),  # thiqah but fathi
    # Derived thiqah phrases
    (r'\bفهو\s+ثقة\b', 'thiqah', 'ثقة'),
    (r'\bوهو\s+ثقة\b', 'thiqah', 'ثقة'),
    (r'\bالثقة\s+الآتي\b', 'thiqah', 'ثقة'),     # "the trustworthy one coming [later]"
    (r'\bالثقة\s+المتقدم\b', 'thiqah', 'ثقة'),    # "the trustworthy one mentioned [earlier]"
    (r'\bالثقة\s+الآتي\b', 'thiqah', 'ثقة'),
    # Plain ثقة — must be standalone (not part of "لم تثبت وثاقته" etc.)
    # This is checked separately with exclusion logic below
]

# Weak / daif patterns
DAIF_PATTERNS = [
    (r'\bضعيف\s+جدا\b', 'daif', 'ضعيف جداً'),
    (r'\bضعيف\s+في\s+نفسه\b', 'daif', 'ضعيف في نفسه'),
    # Plain ضعيف (standalone verdict, not "طريق ... ضعيف")
]

# Majhul pattern
MAJHUL_PATTERN = r'\bمجهول\b'

# Mamduh / Hasan patterns
MAMDUH_PATTERNS = [
    (r'\bممدوح\b', 'mamduh', 'ممدوح'),
    (r'\bمن\s+الحسان\b', 'hasan', 'من الحسان'),
    (r'\bحسن\s+الحال\b', 'hasan', 'حسن الحال'),
    (r'\bوجه\s+أصحابنا\b', 'hasan', 'وجه أصحابنا'),  # "face of our companions" = praiseworthy
    (r'\bعظيم\s+المنزلة\b', 'mamduh', 'عظيم المنزلة'),
    (r'\bجليل\s+القدر\b', 'mamduh', 'جليل القدر'),
]

# Muwaththaq — trustworthy but non-Imami
MUWATHTHAQ_PATTERNS = [
    (r'\bموثق\b', 'muwaththaq', 'موثق'),
]

# Negative patterns — sentences that negate or qualify thiqah/status
# These indicate "not proven trustworthy" or "debated"
NEGATION_PATTERNS = [
    r'لم\s+تثبت\s+وثاقته',       # "his trustworthiness was not established"
    r'لم\s+يثبت\s+توثيقه',       # "his tawthiq was not established"
    r'لا\s+يثبت\s+التوثيق',      # "the tawthiq is not established"
    r'لم\s+تثبت\s+وثاقة',        # "the trustworthiness was not..."
    r'فلم\s+تثبت\s+وثاقته',      # "so his trustworthiness was not..."
    r'فلم\s+يثبت\s+توثيقه',
    r'المتعارض\s+فيه\s+التوثيق',  # "conflicting tawthiq"
    r'تعارض\s+التوثيق',
    r'لم\s+ينبه',
    r'وإن\s+كان\s+لا\s+حاجة',
]

# Status source extraction
SOURCE_PATTERNS = [
    (r'وثقه\s+النجاشي', 'النجاشي'),
    (r'قاله?\s+النجاشي', 'النجاشي'),
    (r'وثقه\s+الشيخ', 'الشيخ الطوسي'),
    (r'قاله?\s+الشيخ\s+منتجب\s+الدين', 'الشيخ منتجب الدين'),
    (r'قاله?\s+الشيخ\s+الحر', 'الشيخ الحر'),
    (r'قاله?\s+الشيخ', 'الشيخ الطوسي'),
    (r'وثقه\s+المفيد', 'الشيخ المفيد'),
    (r'قاله?\s+المفيد', 'الشيخ المفيد'),
    (r'وثقه\s+البرقي', 'البرقي'),
    (r'قاله?\s+البرقي', 'البرقي'),
    (r'وثقه\s+الصدوق', 'الصدوق'),
    (r'وثقه\s+الكشي', 'الكشي'),
    (r'قاله?\s+الكشي', 'الكشي'),
    (r'قاله?\s+الوحيد', 'الوحيد'),
    (r'نص\s+(?:أحد\s+)?المعصومين', 'نص المعصومين'),
    (r'وثقه\s+ابن\s+شهرآشوب', 'ابن شهرآشوب'),
]


def parse_entry_numbers(line_start):
    """
    Parse the triple numbering: 'N1 - N2 - N3 - NAME : ...'
    N1 = Najaf, N2 = Beirut, N3 = Tehran
    Some entries have 'ج 24' or missing numbers.
    Returns (najaf_num, beirut_num, tehran_num, name_start_idx)
    """
    # Pattern: digits or "ج‍ 24" or "ج 24" separated by " - "
    # Allow for missing numbers
    m = re.match(
        r'^\s*(\d+|ج‍?\s*\d+)\s*-\s*(\d+|ج‍?\s*\d+)?\s*-?\s*(\d+|ج‍?\s*\d+)?\s*-\s*',
        line_start
    )
    if m:
        n1 = m.group(1).strip() if m.group(1) else None
        n2 = m.group(2).strip() if m.group(2) else None
        n3 = m.group(3).strip() if m.group(3) else None
        return n1, n2, n3, m.end()
    return None, None, None, 0


def extract_status(text):
    """
    Extract the final al-Khoei status from an entry's text.
    Returns (status, status_detail, sources_list)
    """
    # Check for negation first — if present, the entry is contested
    has_negation = any(re.search(p, text) for p in NEGATION_PATTERNS)

    # Check for "متحد مع ... الثقة" — if the entry says it's identical to a thiqah
    ittihad_thiqah = bool(re.search(r'متحد\s+مع\s+.*?\s+الثقة', text))
    # Also check "وهو ... الثقة"
    wahwa_thiqah = bool(re.search(r'وهو\s+\S+.*?\s+الثقة', text))

    # Extract sources
    sources = []
    for pattern, source_name in SOURCE_PATTERNS:
        if re.search(pattern, text):
            if source_name not in sources:
                sources.append(source_name)

    # Priority 1: Check for explicit ثقة as a standalone verdict
    # Must NOT be in a phrase like "لم تثبت وثاقته" or "طريق ... ضعيف ثقة"
    # Look for ثقة that appears as a direct verdict
    for pattern, status, detail in THIQAH_PATTERNS:
        if re.search(pattern, text):
            if not has_negation:
                return status, detail, sources
            # If negated but also has positive thiqah, complex — check context
            break

    # Check standalone " ثقة " or "- ثقة -" or ": ثقة"
    # Exclude "الثقة" (article = referencing someone else)
    # Exclude "طريق ... ضعيف"/"طريق ... صحيح" context (those describe the chain, not narrator)
    standalone_thiqah = re.search(r'(?:^|\s|[-:])ثقة(?:\s|[-,.]|$)', text)
    if standalone_thiqah and not has_negation:
        # Make sure it's not just referencing someone else's identity
        # "متحد مع X الثقة" = this person IS the thiqah
        # But "الثقة الآتي" when used standalone (not after متحد) = reference
        return 'thiqah', 'ثقة', sources

    # Priority 2: ثقة via identity — "متحد مع ... الثقة" or "وهو ... الثقة"
    if (ittihad_thiqah or wahwa_thiqah) and not has_negation:
        return 'thiqah', 'ثقة (متحد)', sources

    # Priority 3: Check for ضعيف as direct verdict
    for pattern, status, detail in DAIF_PATTERNS:
        m = re.search(pattern, text)
        if m:
            return status, detail, sources
    # Standalone ضعيف — but NOT "طريق ... ضعيف"
    daif_match = re.search(r'(?:^|\s|[-:])ضعيف(?:\s|[-,.]|$)', text)
    if daif_match:
        # Exclude if preceded by "طريق" context (path weakness, not narrator weakness)
        # Check within ~40 chars before the match
        start = max(0, daif_match.start() - 60)
        context_before = text[start:daif_match.start()]
        if not re.search(r'طريق|طرق', context_before):
            return 'daif', 'ضعيف', sources

    # Priority 4: Muwaththaq
    for pattern, status, detail in MUWATHTHAQ_PATTERNS:
        if re.search(pattern, text):
            return status, detail, sources

    # Priority 5: Mamduh/Hasan
    for pattern, status, detail in MAMDUH_PATTERNS:
        if re.search(pattern, text):
            return status, detail, sources

    # "من الحسان" or "حسن" as verdict
    if re.search(r'\bحسن\b', text) and not re.search(r'(?:أبو|بن|بنت)\s+حسن', text):
        # Make sure "حسن" is a status, not a name part
        if re.search(r'[-:\s]حسن[-:\s,.]', text):
            return 'hasan', 'حسن', sources

    # Priority 6: مجهول
    if re.search(MAJHUL_PATTERN, text):
        return 'majhul', 'مجهول', sources

    # Priority 7: Check tafsir al-Qummi thiqah derivation
    if re.search(r'روى\s+في\s+تفسير\s+القمي\s+فهو\s+ثقة', text):
        return 'thiqah', 'ثقة (تفسير القمي)', ['علي بن إبراهيم']
    if re.search(r'لروايته\s+في\s+تفسير\s+القمي', text) and ittihad_thiqah:
        return 'thiqah', 'ثقة (تفسير القمي)', ['علي بن إبراهيم']

    # Priority 8: mashayikh ibn quluwayh
    if re.search(r'مشايخ\s+ابن\s+قولويه.*?فهو\s+ثقة', text):
        return 'thiqah', 'ثقة (مشايخ ابن قولويه)', ['ابن قولويه']
    if re.search(r'من\s+مشايخ\s+النجاشي.*?فهو\s+ثقة', text):
        return 'thiqah', 'ثقة (مشايخ النجاشي)', ['النجاشي']

    return 'unspecified', None, sources


def parse_mufid_file(filepath):
    """Parse the entire al-Mufid MD file and extract statuses."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split into lines
    lines = content.split('\n')

    # Entry pattern: starts with NUMBER - NUMBER - NUMBER - NAME :
    # Allow for some entries that start mid-line after previous entry
    ENTRY_RE = re.compile(
        r'^(?:ج‍?\s*24\s*[-.]?\s*)?'  # optional "ج 24" prefix
        r'(\d+)\s*-\s*'               # Najaf number
        r'(\d+)?\s*-?\s*'             # Beirut number (optional)
        r'(\d+)?\s*-\s*'              # Tehran number (optional)
        r'(.+)'                         # Rest of entry
    )

    # Also match entries starting with just numbers without dash (some formatting issues)
    ENTRY_ALT_RE = re.compile(
        r'^(\d{1,5})\s*[-–]\s*(\d{1,5})\s*[-–]\s*(\d{1,5})\s*[-–]\s*(.+)'
    )

    entries = {}
    current_entry = None
    current_najaf = None
    current_beirut = None
    current_tehran = None
    current_name_ar = None
    current_text_parts = []
    skip_sections = True  # Skip intro pages

    for line_num, line in enumerate(lines, 1):
        stripped = line.strip()

        # Skip page headers, footnotes, and HR lines
        if stripped.startswith('## Page') or stripped.startswith('---') or stripped.startswith('> **Footnote'):
            if '## Page 1' in stripped and 'Page 1' == stripped.split('##')[1].strip().split()[0:2]:
                skip_sections = False
            # Actually, detect when main content begins
            if 'باب الألف' in stripped or 'آدم أبو الحسين' in stripped:
                skip_sections = False
            continue

        if stripped.startswith('#') or stripped.startswith('>'):
            continue

        if skip_sections:
            if 'آدم أبو الحسين' in stripped and re.match(r'\d+\s*-', stripped):
                skip_sections = False
            else:
                continue

        # Try to match a new entry
        m = ENTRY_RE.match(stripped) or ENTRY_ALT_RE.match(stripped)
        if m:
            # Save previous entry
            if current_najaf is not None:
                full_text = ' '.join(current_text_parts)
                status, detail, sources = extract_status(full_text)
                entries[current_najaf] = {
                    'najaf': current_najaf,
                    'beirut': current_beirut,
                    'tehran': current_tehran,
                    'name_ar': current_name_ar,
                    'status': status,
                    'status_detail': detail,
                    'status_source': ', '.join(sources) if sources else None,
                    'text_preview': full_text[:200],
                }

            # Start new entry
            current_najaf = m.group(1).strip()
            current_beirut = m.group(2).strip() if m.group(2) else None
            current_tehran = m.group(3).strip() if m.group(3) else None
            rest = m.group(4).strip()
            
            # Extract name_ar before the first colon, if present
            name_parts = rest.split(':', 1)
            current_name_ar = name_parts[0].strip()
            # Clean up the name if it still has dashes or weird chars at the start/end
            current_name_ar = re.sub(r'^\s*[-–]\s*', '', current_name_ar)
            
            current_text_parts = [rest]
        elif current_najaf is not None and stripped:
            # Continuation of current entry
            current_text_parts.append(stripped)

    # Save last entry
    if current_najaf is not None:
        full_text = ' '.join(current_text_parts)
        status, detail, sources = extract_status(full_text)
        entries[current_najaf] = {
            'najaf': current_najaf,
            'beirut': current_beirut,
            'tehran': current_tehran,
            'name_ar': current_name_ar,
            'status': status,
            'status_detail': detail,
            'status_source': ', '.join(sources) if sources else None,
            'text_preview': full_text[:200],
        }

    return entries


def parse_jadwal_section(filepath):
    """
    Parse the jadwal (table at end of book) for additional tawthiq source info.
    The jadwal entries look like:
        12742 - موسى بن بكر الواسطي : ثقة لشهادة ...
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # The jadwal starts after the main entries section
    # Look for it — it has a different format: just number - name : status source explanation
    # It starts around page 768+

    # Find jadwal section by looking for the jadwal header or by page number
    jadwal_re = re.compile(
        r'^(\d{3,5})\s*[-–]\s*(.+?)(?:\s*:\s*(.+))?$'
    )

    jadwal_entries = {}
    in_jadwal = False
    lines = content.split('\n')

    for i, line in enumerate(lines):
        stripped = line.strip()
        # The jadwal section is at the end of the book (after page ~768)
        # It has entries like "12742 - name : source info"
        if '## Page 768' in stripped or '## Page 769' in stripped or '## Page 770' in stripped:
            in_jadwal = True
            continue

        if not in_jadwal:
            continue

        m = jadwal_re.match(stripped)
        if m:
            najaf_num = m.group(1).strip()
            name = m.group(2).strip()
            detail = m.group(3).strip() if m.group(3) else ''
            if not detail:
                # Detail might be on next lines
                detail_parts = []
                for j in range(i+1, min(i+5, len(lines))):
                    next_line = lines[j].strip()
                    if not next_line or next_line.startswith('#') or next_line.startswith('>') or next_line.startswith('---'):
                        break
                    if jadwal_re.match(next_line):
                        break
                    detail_parts.append(next_line)
                detail = ' '.join(detail_parts)

            # Extract source from jadwal detail
            full_text = name + ' ' + detail
            sources = []
            for pattern, source_name in SOURCE_PATTERNS:
                if re.search(pattern, full_text):
                    if source_name not in sources:
                        sources.append(source_name)

            jadwal_entries[najaf_num] = {
                'sources': sources,
                'detail': detail,
            }

    return jadwal_entries


def main():
    print("Parsing al-Mufid (7194.md)...")
    entries = parse_mufid_file(MUFID_PATH)
    print(f"  Extracted {len(entries):,} entries from main text")

    # Parse jadwal for additional source info
    print("Parsing jadwal (tawthiq table)...")
    jadwal = parse_jadwal_section(MUFID_PATH)
    print(f"  Extracted {len(jadwal):,} jadwal entries")

    # Merge jadwal source info into main entries
    merged_count = 0
    for najaf_num, j_data in jadwal.items():
        if najaf_num in entries:
            e = entries[najaf_num]
            # Add sources from jadwal if not already present
            existing_sources = set((e.get('status_source') or '').split(', '))
            for src in j_data['sources']:
                if src not in existing_sources:
                    if e['status_source']:
                        e['status_source'] += f', {src}'
                    else:
                        e['status_source'] = src
                    merged_count += 1

    print(f"  Merged {merged_count} source attributions from jadwal")

    # Statistics
    from collections import Counter
    status_counts = Counter(e['status'] for e in entries.values())
    print(f"\n{'─'*60}")
    print(f"  STATUS DISTRIBUTION")
    print(f"{'─'*60}")
    for status, count in status_counts.most_common():
        pct = 100 * count / len(entries)
        print(f"  {status:<20} {count:>6,} ({pct:>5.1f}%)")
    print(f"  {'─'*40}")
    print(f"  {'TOTAL':<20} {len(entries):>6,}")

    # Save output
    # Remove text_preview for the final output (it's just for debugging)
    output = {}
    for k, e in entries.items():
        output[k] = {
            'najaf': e['najaf'],
            'beirut': e.get('beirut'),
            'tehran': e.get('tehran'),
            'name_ar': e.get('name_ar', ''),
            'status': e['status'],
            'status_detail': e.get('status_detail'),
            'status_source': e.get('status_source'),
        }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to {OUTPUT_PATH.name}")

    # Sample outputs
    print(f"\nSample entries:")
    for k in sorted(list(entries.keys()), key=lambda x: int(x) if x.isdigit() else 0)[:20]:
        e = entries[k]
        src = f" [{e['status_source']}]" if e['status_source'] else ""
        print(f"  [{k:>5}] {e['status']:<12} {e.get('status_detail',''):<20}{src}")
        print(f"         {e['text_preview'][:100]}")


if __name__ == '__main__':
    main()

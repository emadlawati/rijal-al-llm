#!/usr/bin/env python3
"""
Compare our isnad grading against Allamah Majlisi's gradings from allBooks.json.

Key fix over v1: extract_isnad() now works entirely on normalized Arabic text,
avoiding the index-mismatch bug that cut isnads to a few words.
"""

import json
import sys
import re
from pathlib import Path
from collections import Counter, defaultdict

# Configure stdout for Windows Unicode support
if sys.platform == "win32":
    import io
    if getattr(sys.stdout, 'encoding', '').lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

from isnad_analyzer import IsnadAnalyzer
from rijal_resolver import normalize_ar


# ─── Isnad extraction ────────────────────────────────────────────────────────

from isnad_extractor import extract_isnad as _extract_isnad


def extract_isnad(full_text: str) -> str:
    """
    Extract the isnad from a full hadith text (isnad + matn combined).
    Delegates to isnad_extractor.robust backward-Imam search.
    """
    return _extract_isnad(full_text)


# ─── Comparison ──────────────────────────────────────────────────────────────

# Map Majlisi Arabic grading strings to canonical categories
_MAJLISI_MAP = {
    'صحيح':              'Sahih',
    'حسن كالصحيح':       'Sahih',     # Hasan-like-Sahih (counts as Sahih for our purposes)
    'موثق كالصحيح':      'Sahih',
    'حسن':               'Hasan',
    'موثق':              'Muwaththaq',
    'حسن أو موثق':       'Hasan',
    'مجهول كالصحيح':    'Majhul',
    'مجهول':             'Majhul',
    'ضعيف':              'Daif',
    'ضعيف على المشهور':  'Daif',
    'مرسل':              'Mursal',
    'مرفوع':             'Mursal',
    'مرسل كالموثق':      'Mursal',
    'مختلف فيه':         'Disputed',
}


def our_category(grade: str) -> str:
    """Collapse our verbose grade string into a short category."""
    if 'Sahih'       in grade: return 'Sahih'
    if 'Hasan'       in grade: return 'Hasan'
    if 'Muwaththaq'  in grade: return 'Muwaththaq'
    if "Da'if"       in grade: return 'Daif'
    if 'Majhul'      in grade: return 'Majhul'
    if 'Undetermined' in grade: return 'Undetermined'
    return 'Other'


def compare(limit=100, book_filter='Kafi', verbose=False, show_failures=5, show_unresolved=0):
    db_path = Path("allBooks.json")
    if not db_path.exists():
        print("Error: allBooks.json not found.")
        return

    print(f"Loading {db_path}...")
    with open(db_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Filter to the requested book
    if book_filter:
        data = [h for h in data if book_filter.lower() in h.get('bookId', '').lower()]

    # Filter to Sahih (and Sahih-equivalent) hadiths
    target_hadiths = [
        h for h in data
        if _MAJLISI_MAP.get(h.get('majlisiGrading', '').strip()) == 'Sahih'
    ]
    print(f"Book filter: '{book_filter}'  →  {len(data):,} hadiths total")
    print(f"Majlisi Sahih (incl. Hasan-ka-Sahih / Muwaththaq-ka-Sahih): {len(target_hadiths):,}")

    analyzer = IsnadAnalyzer()

    total      = min(limit, len(target_hadiths))
    matches    = 0
    our_grades = Counter()
    failure_reasons = Counter()   # why Undetermined
    failures   = []               # detailed failure records
    unresolved_names: Counter = Counter()   # tracks unresolved segment names

    print(f"\nComparing first {total} hadiths ...\n")
    if verbose:
        print(f"{'ID':<6} | {'Majlisi':<22} | {'Ours':<30} | {'Match'}")
        print("-" * 75)

    for i in range(total):
        h   = target_hadiths[i]
        hid = h.get('id', i)
        arabic = h.get('arabicText', '')
        majlisi_raw = h.get('majlisiGrading', '').strip()

        isnad_norm = extract_isnad(arabic)
        names      = analyzer.parse_isnad_string(isnad_norm)
        analysis   = analyzer.analyze(names)
        our_grade  = analysis['final_status']['grade']
        our_cat    = our_category(our_grade)

        our_grades[our_cat] += 1
        is_match = (our_cat == 'Sahih')
        if is_match:
            matches += 1

        if not is_match:
            # Diagnose why
            statuses = analysis['final_status']['narrator_statuses']
            if 'unresolved' in statuses:
                failure_reasons['unresolved_link'] += 1
                # Track which segment names are unresolved
                for item in analysis['chain']:
                    if item['resolution'].get('top_match') is None:
                        unresolved_names[item['original_query']] += 1
            elif our_cat == 'Daif':
                failure_reasons['has_daif_link'] += 1
            elif our_cat == 'Majhul':
                failure_reasons['has_majhul_link'] += 1
            else:
                failure_reasons[our_cat] += 1

            failures.append({
                'id':       hid,
                'majlisi':  majlisi_raw,
                'isnad':    isnad_norm,
                'names':    names,
                'our_grade': our_grade,
                'statuses': statuses,
                'chain':    analysis['chain'],
            })

        if verbose:
            marker = '✓' if is_match else '✗'
            print(f"{hid:<6} | {majlisi_raw:<22} | {our_grade[:30]:<30} | {marker}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "═" * 55)
    print(f"  RESULTS  (n={total})")
    print("═" * 55)
    print(f"  Majlisi Sahih → Our Sahih :  {matches:>4} / {total}  ({matches/total:.1%})")
    print()
    print("  Our grade distribution for Majlisi-Sahih hadiths:")
    for cat, cnt in our_grades.most_common():
        bar = '█' * int(cnt / total * 30)
        print(f"    {cat:<15} {cnt:>4}  ({cnt/total:.1%})  {bar}")
    print()
    print("  Failure breakdown (non-Sahih):")
    for reason, cnt in failure_reasons.most_common():
        print(f"    {reason:<25} {cnt:>4}")
    print("═" * 55)

    # ── Top unresolved names ──────────────────────────────────────────────────
    if show_unresolved > 0 and unresolved_names:
        top_n = min(show_unresolved, len(unresolved_names))
        print(f"\nTop {top_n} unresolved (n={total}):")
        for name, cnt in unresolved_names.most_common(top_n):
            print(f"  [{cnt:>3}x] {name}")

    # ── Show sample failures ──────────────────────────────────────────────────
    if failures and show_failures > 0:
        print(f"\n── SAMPLE FAILURES (first {min(show_failures, len(failures))}) ──")
        for d in failures[:show_failures]:
            print(f"\n  ID {d['id']}  |  Majlisi: {d['majlisi']}  |  Ours: {d['our_grade']}")
            print(f"  Extracted isnad: {d['isnad'][:200]}")
            print(f"  Parsed names ({len(d['names'])}): {d['names']}")
            # Show which link failed
            for j, item in enumerate(d['chain']):
                tm = item['resolution'].get('top_match')
                st = tm['status'] if tm else 'UNRESOLVED'
                sc = f"{tm['confidence_score']:.1f}" if tm else '-'
                name_ar = tm['name_ar'] if tm else '—'
                print(f"    [{j+1}] {item['original_query']:<35} → {st:<12} (score {sc})  {name_ar[:50]}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('limit',        type=int,  nargs='?', default=100,
                   help='Number of hadiths to test (default 100)')
    p.add_argument('--verbose',    action='store_true',
                   help='Print per-hadith result table')
    p.add_argument('--failures',   type=int,  default=5,
                   help='Number of sample failures to show (default 5)')
    p.add_argument('--unresolved', type=int,  default=0,
                   help='Print top N unresolved segment names (default 0 = off)')
    p.add_argument('--book',       type=str,  default='Kafi',
                   help='Book filter substring (default "Kafi")')
    args = p.parse_args()

    compare(limit=args.limit, book_filter=args.book,
            verbose=args.verbose, show_failures=args.failures,
            show_unresolved=args.unresolved)

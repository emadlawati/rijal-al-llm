#!/usr/bin/env python3
"""
Rijal Database Comparator
=========================
Compares rijal_database.json (al-Mufid summary) vs rijal_database_mujam.json (full 24-volume)
to quantify the benefit of the full extraction and assess which should be the primary database.
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

DB1_PATH = Path(__file__).parent / "rijal_database.json"
DB2_PATH = Path(__file__).parent / "rijal_database_mujam.json"

def load(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def count_non_empty(entry, field):
    """Count if a field has meaningful content."""
    val = entry.get(field)
    if val is None:
        return 0
    if isinstance(val, list):
        return 1 if len(val) > 0 else 0
    if isinstance(val, str):
        return 1 if val.strip() else 0
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return 1
    return 1

def field_richness(db, fields):
    """For each field, count how many entries have non-empty values."""
    counts = {}
    for field in fields:
        counts[field] = sum(count_non_empty(e, field) for e in db.values())
    return counts

def status_distribution(db):
    dist = Counter()
    for e in db.values():
        dist[e.get('status', 'unspecified')] += 1
    return dist

def chain_info_richness(db):
    """Count total items across all narrates_from/narrated_from_by/books arrays."""
    total_from_imams = sum(len(e.get('narrates_from_imams') or []) for e in db.values())
    total_from_narrs = sum(len(e.get('narrates_from_narrators') or []) for e in db.values())
    total_narrated_by = sum(len(e.get('narrated_from_by') or []) for e in db.values())
    total_aliases = sum(len(e.get('aliases') or []) for e in db.values())
    total_same_as = sum(len(e.get('same_as_names') or []) for e in db.values())
    total_books = sum(len(e.get('books') or []) for e in db.values())
    return {
        'narrates_from_imams': total_from_imams,
        'narrates_from_narrators': total_from_narrs,
        'narrated_from_by': total_narrated_by,
        'aliases': total_aliases,
        'same_as_names': total_same_as,
        'books': total_books,
    }

def compare_entry_pair(e1, e2):
    """Compare two entries with the same index and find differences."""
    diffs = {}
    key_fields = [
        'name_ar', 'status', 'status_detail', 'status_source', 'sect',
        'father', 'grandfather', 'nasab', 'kunyah', 'laqab', 'nisba',
        'companions_of', 'has_book', 'tariq_status',
        'identity_confidence', 'scribal_error_noted',
        'tabaqah', 'tabaqah_source', 'tabaqah_confidence',
    ]
    for field in key_fields:
        v1 = e1.get(field)
        v2 = e2.get(field)
        if v1 != v2:
            diffs[field] = {'mufid': v1, 'mujam': v2}

    # Compare list fields by count
    list_fields = [
        'narrates_from_imams', 'narrates_from_narrators', 'narrated_from_by',
        'books', 'aliases', 'same_as_names', 'same_as_entry_nums',
    ]
    for field in list_fields:
        l1 = e1.get(field) or []
        l2 = e2.get(field) or []
        if len(l2) > len(l1):
            diffs[field] = {'mufid_count': len(l1), 'mujam_count': len(l2)}

    # Check _raw_full presence
    has_raw_full_1 = bool(e1.get('_raw_full'))
    has_raw_full_2 = bool(e2.get('_raw'))  # mujam uses _raw which is the full text
    raw_len_1 = len(e1.get('_raw_full', '') or '')
    raw_len_2 = len(e2.get('_raw', '') or '')
    if abs(raw_len_2 - raw_len_1) > 50:
        diffs['raw_text_length'] = {'mufid': raw_len_1, 'mujam': raw_len_2}

    return diffs

def find_status_upgrades(db1, db2):
    """Find narrators where mujam has a more specific/better status."""
    upgrades = []
    downgrades = []
    status_hierarchy = {
        'unspecified': 0, 'majhul': 1, 'daif': 1, 'mamduh': 2,
        'hasan': 3, 'muwaththaq': 3, 'thiqah': 4
    }
    for key in db1:
        if key not in db2:
            continue
        s1 = db1[key].get('status', 'unspecified')
        s2 = db2[key].get('status', 'unspecified')
        if s1 == s2:
            continue
        h1 = status_hierarchy.get(s1, 0)
        h2 = status_hierarchy.get(s2, 0)
        entry = {
            'idx': key,
            'name': db1[key].get('name_ar', ''),
            'mufid_status': s1,
            'mujam_status': s2,
        }
        if h2 > h1:
            upgrades.append(entry)
        else:
            downgrades.append(entry)
    return upgrades, downgrades

def find_new_chain_links(db1, db2):
    """Find narrators where mujam has MORE chain information."""
    gains = []
    for key in db1:
        if key not in db2:
            continue
        e1, e2 = db1[key], db2[key]
        from1 = len(e1.get('narrates_from_narrators') or [])
        from2 = len(e2.get('narrates_from_narrators') or [])
        by1 = len(e1.get('narrated_from_by') or [])
        by2 = len(e2.get('narrated_from_by') or [])
        if from2 > from1 or by2 > by1:
            gains.append({
                'idx': key,
                'name': e1.get('name_ar', ''),
                'from_gain': from2 - from1,
                'by_gain': by2 - by1,
                'total_gain': (from2 - from1) + (by2 - by1),
            })
    gains.sort(key=lambda x: x['total_gain'], reverse=True)
    return gains

def text_length_comparison(db1, db2):
    """Compare raw text richness between the two databases."""
    total_mufid = 0
    total_mujam = 0
    for key in db1:
        # al-Mufid db has _raw (summary) and _raw_full (full text)
        total_mufid += len(db1[key].get('_raw_full', '') or db1[key].get('_raw', '') or '')
    for key in db2:
        total_mujam += len(db2[key].get('_raw', '') or '')
    return total_mufid, total_mujam


def main():
    print("Loading databases...")
    db1 = load(DB1_PATH)
    db2 = load(DB2_PATH)

    print(f"\n{'='*80}")
    print(f"  RIJAL DATABASE COMPARISON REPORT")
    print(f"  al-Mufid (summary) vs Full 24-Volume Mu'jam")
    print(f"{'='*80}")

    # ── 1. Basic Stats ────────────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  1. BASIC STATISTICS")
    print(f"{'─'*80}")
    print(f"  {'Metric':<40} {'al-Mufid':>12} {'Mujam':>12} {'Delta':>10}")
    print(f"  {'─'*74}")
    print(f"  {'Total entries':<40} {len(db1):>12,} {len(db2):>12,} {len(db2)-len(db1):>+10,}")

    # File size
    s1 = DB1_PATH.stat().st_size / (1024*1024)
    s2 = DB2_PATH.stat().st_size / (1024*1024)
    print(f"  {'File size (MB)':<40} {s1:>12.1f} {s2:>12.1f} {s2-s1:>+10.1f}")

    # Raw text
    t1, t2 = text_length_comparison(db1, db2)
    print(f"  {'Total raw text (chars)':<40} {t1:>12,} {t2:>12,} {t2-t1:>+10,}")

    # ── 2. Status Distribution ────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  2. STATUS DISTRIBUTION")
    print(f"{'─'*80}")
    sd1 = status_distribution(db1)
    sd2 = status_distribution(db2)
    all_statuses = sorted(set(list(sd1.keys()) + list(sd2.keys())))
    print(f"  {'Status':<25} {'al-Mufid':>10} {'Mujam':>10} {'Delta':>10}")
    print(f"  {'─'*55}")
    for s in all_statuses:
        v1 = sd1.get(s, 0)
        v2 = sd2.get(s, 0)
        print(f"  {s:<25} {v1:>10,} {v2:>10,} {v2-v1:>+10,}")

    # ── 3. Field Richness ─────────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  3. FIELD RICHNESS (entries with non-empty values)")
    print(f"{'─'*80}")
    fields = [
        'status', 'status_detail', 'status_source', 'sect',
        'father', 'grandfather', 'nasab', 'kunyah', 'laqab', 'nisba',
        'companions_of', 'has_book', 'tariq_status',
        'narrates_from_imams', 'narrates_from_narrators', 'narrated_from_by',
        'books', 'aliases', 'same_as_names', 'same_as_entry_nums',
        'disambiguation_notes', 'notes',
        'tabaqah', 'tabaqah_source',
    ]
    fr1 = field_richness(db1, fields)
    fr2 = field_richness(db2, fields)
    print(f"  {'Field':<30} {'al-Mufid':>10} {'Mujam':>10} {'Delta':>10}")
    print(f"  {'─'*60}")
    for f in fields:
        v1, v2 = fr1[f], fr2[f]
        delta = v2 - v1
        marker = " ★" if delta > 50 else (" ▲" if delta > 0 else ("" if delta == 0 else " ▼"))
        print(f"  {f:<30} {v1:>10,} {v2:>10,} {delta:>+10,}{marker}")

    # ── 4. Chain Information Totals ───────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  4. CHAIN INFORMATION (total items across all entries)")
    print(f"{'─'*80}")
    cr1 = chain_info_richness(db1)
    cr2 = chain_info_richness(db2)
    print(f"  {'Chain Field':<30} {'al-Mufid':>10} {'Mujam':>10} {'Delta':>10}")
    print(f"  {'─'*60}")
    for f in cr1:
        v1, v2 = cr1[f], cr2[f]
        delta = v2 - v1
        marker = " ★" if delta > 100 else (" ▲" if delta > 0 else ("" if delta == 0 else " ▼"))
        print(f"  {f:<30} {v1:>10,} {v2:>10,} {delta:>+10,}{marker}")

    # ── 5. Status Changes ────────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  5. STATUS CHANGES (entries present in both)")
    print(f"{'─'*80}")
    upgrades, downgrades = find_status_upgrades(db1, db2)
    print(f"  Status UPGRADES (Mujam has better status): {len(upgrades)}")
    print(f"  Status DOWNGRADES (Mufid had better status): {len(downgrades)}")
    
    if upgrades:
        print(f"\n  Top upgrades:")
        for u in upgrades[:20]:
            print(f"    [{u['idx']:>5}] {u['name']:<40} {u['mufid_status']:>12} → {u['mujam_status']}")
    
    if downgrades:
        print(f"\n  Top downgrades:")
        for d in downgrades[:20]:
            print(f"    [{d['idx']:>5}] {d['name']:<40} {d['mufid_status']:>12} → {d['mujam_status']}")

    # ── 6. Chain Link Gains ──────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  6. CHAIN LINK GAINS (Mujam has more narrates_from / narrated_by)")
    print(f"{'─'*80}")
    gains = find_new_chain_links(db1, db2)
    print(f"  Narrators with MORE chain data in Mujam: {len(gains)}")
    if gains:
        print(f"\n  Top 20 biggest gains:")
        for g in gains[:20]:
            print(f"    [{g['idx']:>5}] {g['name']:<40} from+{g['from_gain']:<5} by+{g['by_gain']:<5} total+{g['total_gain']}")

    # ── 7. Entries only in one database ───────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  7. ENTRY DIFFERENCES")
    print(f"{'─'*80}")
    only_in_1 = set(db1.keys()) - set(db2.keys())
    only_in_2 = set(db2.keys()) - set(db1.keys())
    common = set(db1.keys()) & set(db2.keys())
    print(f"  Common entries: {len(common):,}")
    print(f"  Only in al-Mufid: {len(only_in_1):,}")
    print(f"  Only in Mujam: {len(only_in_2):,}")
    
    if only_in_2:
        print(f"\n  Sample entries ONLY in Mujam (first 15):")
        for k in sorted(only_in_2, key=lambda x: int(x) if x.isdigit() else 0)[:15]:
            e = db2[k]
            print(f"    [{k:>5}] {e.get('name_ar', '?'):<40} status={e.get('status', '?')}")

    # ── 8. Head-to-head: same entry, different content ────────────────────────
    print(f"\n{'─'*80}")
    print(f"  8. HEAD-TO-HEAD DIFFERENCES (sample of shared entries)")
    print(f"{'─'*80}")
    diff_count = 0
    diff_summary = Counter()
    entries_with_diffs = []
    for key in sorted(common, key=lambda x: int(x) if x.isdigit() else 0):
        diffs = compare_entry_pair(db1[key], db2[key])
        if diffs:
            diff_count += 1
            for field in diffs:
                diff_summary[field] += 1
            if len(entries_with_diffs) < 10:
                entries_with_diffs.append((key, diffs))

    print(f"  Entries with ANY difference: {diff_count:,} / {len(common):,} ({100*diff_count/max(len(common),1):.1f}%)")
    print(f"\n  Fields most often different:")
    for field, cnt in diff_summary.most_common(20):
        print(f"    {field:<35} {cnt:>6,} entries differ")

    if entries_with_diffs:
        print(f"\n  Sample diffs (first 10):")
        for key, diffs in entries_with_diffs:
            name = db1[key].get('name_ar', '?')
            print(f"\n    [{key}] {name}")
            for field, diff_val in list(diffs.items())[:5]:
                if 'mufid' in diff_val:
                    print(f"      {field}: {diff_val['mufid']} → {diff_val['mujam']}")
                elif 'mufid_count' in diff_val:
                    print(f"      {field}: {diff_val['mufid_count']} items → {diff_val['mujam_count']} items")

    # ── 9. VERDICT & RECOMMENDATION ──────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"  VERDICT & RECOMMENDATION")
    print(f"{'='*80}")
    
    # Score based on advantages
    mujam_advantages = []
    mufid_advantages = []
    
    if len(db2) > len(db1):
        mujam_advantages.append(f"+{len(db2)-len(db1)} more entries")
    if len(upgrades) > len(downgrades):
        mujam_advantages.append(f"+{len(upgrades)} status upgrades (vs {len(downgrades)} downgrades)")
    else:
        mufid_advantages.append(f"+{len(downgrades)} status advantages (vs {len(upgrades)} for mujam)")
    
    for f in cr1:
        if cr2[f] > cr1[f]:
            mujam_advantages.append(f"+{cr2[f]-cr1[f]:,} more {f} links")
        elif cr1[f] > cr2[f]:
            mufid_advantages.append(f"+{cr1[f]-cr2[f]:,} more {f} links")
    
    if t2 > t1:
        mujam_advantages.append(f"+{t2-t1:,} chars more raw text")
    elif t1 > t2:
        mufid_advantages.append(f"+{t1-t2:,} chars more raw text")
    
    # Check if mufid has _raw_full (full text from 24-vol) already
    mufid_has_raw_full = sum(1 for e in db1.values() if e.get('_raw_full'))
    mujam_has_raw_full = sum(1 for e in db2.values() if e.get('_raw_full'))
    print(f"\n  al-Mufid entries with _raw_full: {mufid_has_raw_full:,}")
    print(f"  Mujam entries with _raw_full:    {mujam_has_raw_full:,}")
    
    print(f"\n  Mujam advantages ({len(mujam_advantages)}):")
    for a in mujam_advantages:
        print(f"    ✓ {a}")
    
    print(f"\n  al-Mufid advantages ({len(mufid_advantages)}):")
    for a in mufid_advantages:
        print(f"    ✓ {a}")
    
    winner = "MUJAM" if len(mujam_advantages) > len(mufid_advantages) else "AL-MUFID" if len(mufid_advantages) > len(mujam_advantages) else "TIE"
    print(f"\n  ══ OVERALL WINNER: {winner} ══")
    
    if winner == "MUJAM":
        print(f"""
  To switch isnad_analyzer to use the Mujam database:
    1. In rijal_resolver.py, change:
         DATABASE_FILE = SCRIPT_DIR / "rijal_database.json"
       to:
         DATABASE_FILE = SCRIPT_DIR / "rijal_database_mujam.json"
    2. Rebuild the resolver index:
         python rijal_resolver.py --build
    3. Test with a known hadith chain to verify.
    
  OR: Merge the best of both databases (recommended approach).
""")


if __name__ == '__main__':
    main()

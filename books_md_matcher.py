"""
books_md_matcher.py
Match unresolved alf_rajul entries using books_md (Mu'jam Rijal al-Hadith).

Key insight: books_md entry numbers == _num_najaf in rijal_database.

Strategy:
  1. Parse books_md -> {najaf_num: name}
  2. For each unresolved alf_rajul name:
     a. Search books_md by normalized name → get najaf_num
     b. Look up all rijal_database entries with that _num_najaf
     c. Pick the best name match → the resolved db_key

Output: books_md_matches.json with {alf_num: db_key} mappings
"""

import re
import json
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

BOOKS_MD_DIR = "c:/Imad Works/Religious - Copy - Copy/books_md"
RIJAL_DB_PATH = "c:/Imad Works/Religious - Copy - Copy/rijal_database.json"
UNRESOLVED_PATH = "c:/Imad Works/Religious - Copy - Copy/unresolved_alf_rajul.txt"

# ─── Normalisation ────────────────────────────────────────────────────────────

_DIAC = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]')
_ALEF = re.compile(r'[أإآاٱ]')

def norm(t: str) -> str:
    if not t:
        return ''
    t = _DIAC.sub('', t)
    t = _ALEF.sub('ا', t)
    t = t.replace('\u06a9', '\u0643').replace('\u06cc', '\u064a')
    t = t.replace('\u0649', '\u064a').replace('\u06c1', '\u0647')
    t = t.replace('\u0671', '\u0627').replace('ـ', '')
    t = re.sub(r'\s+', ' ', t).strip()
    words = t.split()
    words = [w[2:] if w.startswith('ال') and len(w) > 4 else w for w in words]
    return ' '.join(words)

def ar2int(s: str) -> int | None:
    if not s:
        return None
    table = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
    try:
        return int(s.translate(table).strip())
    except ValueError:
        return None

def given_name(name: str) -> str:
    """Extract the person's given name (first word, or first two if starts with عبد)."""
    words = norm(name).split()
    if not words:
        return ''
    if words[0] == 'عبد' and len(words) >= 2:
        return words[0] + ' ' + words[1]
    return words[0]

def word_match(aw: str, bw: str) -> bool:
    """True if words match, allowing one to be a prefix of the other (OCR split-word fix)."""
    if aw == bw:
        return True
    # Handle OCR split words: "مهز" should match "مهزيار", "بز" match "بزيع"
    if len(aw) >= 3 and len(bw) >= 3:
        return bw.startswith(aw) or aw.startswith(bw)
    return False

_CONNECTIVE = {'بن', 'بنت', 'ابو', 'ابي', 'ابا', 'عن', 'من'}

def meaningful_words(name: str) -> list[str]:
    """Significant words in a name (excluding connectives like بن, أبو)."""
    return [w for w in norm(name).split() if w not in _CONNECTIVE]

def name_score(alf: str, db_n: str) -> float:
    """
    Compute name match score. Returns 0 if given names don't match.
    Returns fraction of ALF's meaningful words found in db name (OCR-tolerant).
    """
    an = norm(alf)
    bn = norm(db_n)
    # Given name must match
    if given_name(alf) != given_name(db_n):
        return 0.0
    # Use meaningful (non-connective) words for scoring
    aw = meaningful_words(alf)
    bw = meaningful_words(db_n)
    if not aw:
        return 0.0
    # Count how many alf meaningful words match any db meaningful word
    matched = sum(1 for a in aw if any(word_match(a, b) for b in bw))
    return matched / len(aw)

# ─── Parse books_md ───────────────────────────────────────────────────────────

ENTRY_PATTERN = re.compile(r'^(\d{2,6})\s*-\s*([\u0600-\u06ff][^:]*?)\s*:\s*$')

def parse_books_md():
    """Build {najaf_num: name} and {norm_name: [nums]} indexes."""
    num_to_name: dict[int, str] = {}
    norm_to_nums: dict[str, list[int]] = {}

    for fname in sorted(os.listdir(BOOKS_MD_DIR)):
        if not fname.endswith('.md'):
            continue
        with open(os.path.join(BOOKS_MD_DIR, fname), encoding='utf-8') as f:
            for line in f:
                m = ENTRY_PATTERN.match(line.strip())
                if not m:
                    continue
                num = int(m.group(1))
                if num < 10:
                    continue
                name = m.group(2).strip()
                if num not in num_to_name:
                    num_to_name[num] = name
                n = norm(name)
                norm_to_nums.setdefault(n, []).append(num)

    return num_to_name, norm_to_nums

# ─── Parse rijal_database ─────────────────────────────────────────────────────

def build_najaf_index(db: dict) -> dict[int, list[str]]:
    idx: dict[int, list[str]] = {}
    for key, entry in db.items():
        n = ar2int(entry.get('_num_najaf', ''))
        if n is not None:
            idx.setdefault(n, []).append(key)
    return idx

# ─── Parse unresolved_alf_rajul.txt ──────────────────────────────────────────

def parse_unresolved():
    no_match = []    # [(alf_num, tab, name), ...]
    ambiguous = []   # [(alf_num, tab, name), ...]  (candidates list not needed)

    with open(UNRESOLVED_PATH, encoding='utf-8') as f:
        content = f.read()

    NO_MATCH_RE = re.compile(r'^\[\s*(\d+)\]\s+T(\d+|None)\s+(.+)$')
    AMB_HEADER_RE = re.compile(r'^\[\s*(\d+)\]\s+T(\d+|None)\s+(.+?)\s+\(best match:')

    in_no_match = False
    in_ambiguous = False

    for line in content.split('\n'):
        if '=== NO CANDIDATES' in line:
            in_no_match = True
            in_ambiguous = False
            continue
        if '=== AMBIGUOUS' in line:
            in_no_match = False
            in_ambiguous = True
            continue

        if in_no_match:
            m = NO_MATCH_RE.match(line)
            if m:
                tab_str = m.group(2)
                tab = int(tab_str) if tab_str != 'None' else None
                no_match.append((int(m.group(1)), tab, m.group(3).strip()))

        elif in_ambiguous:
            m = AMB_HEADER_RE.match(line)
            if m:
                tab_str = m.group(2)
                tab = int(tab_str) if tab_str != 'None' else None
                ambiguous.append((int(m.group(1)), tab, m.group(3).strip()))

    return no_match, ambiguous

# ─── Core matching ────────────────────────────────────────────────────────────

def find_in_books(alf_name: str, norm_to_nums: dict) -> list[int]:
    """Find books_md najaf_nums for an alf_rajul name (exact then partial)."""
    n = norm(alf_name)

    # 1) Exact normalized match
    if n in norm_to_nums:
        return norm_to_nums[n]

    # 2) All alf words appear in books_md entry name (at least 2 alf words)
    alf_words = n.split()
    if len(alf_words) < 2:
        return []

    results = []
    alf_set = set(alf_words)
    for bname_n, nums in norm_to_nums.items():
        b_set = set(bname_n.split())
        if alf_set.issubset(b_set):
            results.extend(nums)
    return results


def resolve(alf_name: str, book_nums: list[int],
            num_to_name: dict, najaf_idx: dict, db: dict
) -> list[dict]:
    """
    For each books_md num, search rijal_db entries at that _num_najaf (±2 range).
    Pick the best name-matching one with first-word validation.
    Returns list of match dicts.
    """
    matches = []
    seen_keys = set()

    for bnum in sorted(set(book_nums)):
        bname = num_to_name.get(bnum, '?')

        # Try the exact num and ±2 neighbors (for off-by-one discrepancies)
        candidate_keys = []
        for try_num in range(bnum - 2, bnum + 3):
            candidate_keys.extend(najaf_idx.get(try_num, []))

        if not candidate_keys:
            continue

        # Score each candidate with first-word-validated scoring
        scored = []
        for key in candidate_keys:
            entry = db[key]
            db_name = entry.get('name_ar', '') or ''
            score = name_score(alf_name, db_name)
            if score > 0:
                scored.append((score, key, db_name, entry.get('tabaqah'), entry.get('tabaqah_source', '')))

        if not scored:
            continue

        scored.sort(reverse=True)
        best_score, best_key, best_name, best_tab, best_src = scored[0]

        # Require at least 75% word overlap (filters given-name-only matches)
        if best_score < 0.75:
            continue

        if best_key in seen_keys:
            continue
        seen_keys.add(best_key)

        matches.append({
            'book_num': bnum,
            'book_name': bname,
            'db_key': best_key,
            'db_name': best_name,
            'current_tab': best_tab,
            'tab_source': best_src,
            'name_score': round(best_score, 2),
            'all_at_num': len(candidate_keys),
        })

    return matches


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    db = json.load(open(RIJAL_DB_PATH, encoding='utf-8'))
    najaf_idx = build_najaf_index(db)
    num_to_name, norm_to_nums = parse_books_md()
    no_match, ambiguous = parse_unresolved()

    print(f"  rijal_database: {len(db)} entries | najaf_idx: {len(najaf_idx)} nums")
    print(f"  books_md: {len(num_to_name)} entries")
    print(f"  unresolved: {len(no_match)} no-match, {len(ambiguous)} ambiguous")
    print()

    all_results = []

    # ─── Process all unresolved (no-match + ambiguous) ───────────────────────
    for section_name, entries in [("NO-MATCH", no_match), ("AMBIGUOUS", ambiguous)]:
        print("=" * 78)
        print(section_name)
        print("=" * 78)

        for alf_num, tab, name in entries:
            book_nums = find_in_books(name, norm_to_nums)
            if not book_nums:
                continue

            matches = resolve(name, book_nums, num_to_name, najaf_idx, db)
            if not matches:
                continue

            # Deduplicate by db_key
            seen = set()
            unique = []
            for m in matches:
                if m['db_key'] not in seen:
                    seen.add(m['db_key'])
                    unique.append(m)
            matches = unique

            if len(matches) == 1:
                m = matches[0]
                status = "UNIQUE"
            else:
                # Among multiple: prefer exact name match, then highest score
                exact = [m for m in matches if norm(m['db_name']) == norm(name)]
                if len(exact) == 1:
                    matches = exact
                    status = "EXACT"
                elif len(exact) > 1:
                    # Prefer already-tabaqah'd or tabaqah-matching
                    tab_match = [m for m in exact if m['current_tab'] == tab]
                    if len(tab_match) == 1:
                        matches = tab_match
                        status = "TAB_MATCH"
                    else:
                        status = "STILL_AMB"
                else:
                    status = "MULTI"

            if status in ("UNIQUE", "EXACT", "TAB_MATCH"):
                m = matches[0]
                print(f"\n[{alf_num:4}] T{tab}  {name}")
                print(f"  → books #{m['book_num']}: {m['book_name']}")
                print(f"  → db[{m['db_key']}] = {m['db_name']}  tab={m['current_tab']} ({m['tab_source']})  score={m['name_score']}")
                all_results.append({
                    'alf_num': alf_num, 'alf_name': name, 'alf_tab': tab,
                    'book_num': m['book_num'], 'book_name': m['book_name'],
                    'db_key': m['db_key'], 'db_name': m['db_name'],
                    'current_tab': m['current_tab'], 'status': status,
                })
            elif status == "MULTI":
                print(f"\n[{alf_num:4}] T{tab}  {name}  → {len(matches)} candidates:")
                for m in matches[:4]:
                    print(f"     #{m['book_num']} -> db[{m['db_key']}] {m['db_name']}  tab={m['current_tab']}  score={m['name_score']}")

        print()

    # ─── Summary ─────────────────────────────────────────────────────────────
    total_unresolved = len(no_match) + len(ambiguous)
    print("=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"Total unresolved: {total_unresolved} ({len(no_match)} no-match + {len(ambiguous)} ambiguous)")
    print(f"Resolved via books_md: {len(all_results)}")
    print(f"Remaining unresolved:  {total_unresolved - len(all_results)}")
    print()

    # Save
    out_path = "c:/Imad Works/Religious - Copy - Copy/books_md_matches.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(all_results)} matches to books_md_matches.json")

    # Also save as disambiguation map: {alf_num: db_key}
    disamap = {str(r['alf_num']): r['db_key'] for r in all_results}
    map_path = "c:/Imad Works/Religious - Copy - Copy/alf_rajul_disambiguation.json"
    with open(map_path, 'w', encoding='utf-8') as f:
        json.dump(disamap, f, ensure_ascii=False, indent=2)
    print(f"Saved disambiguation map ({len(disamap)} entries) to alf_rajul_disambiguation.json")

    return all_results


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Merge Rijal Databases — Build the definitive rijal_database_merged.json
=======================================================================
Combines:
  1. rijal_database_mujam.json  → base (best chain data, 15,636 entries)
  2. mufid_statuses.json        → accurate al-Khoei status verdicts from clean text

Also:
  • Resolves cross-references transitively (متحد مع X الثقة → inherit status)
  • Builds robust same_as / alias links so the IsnadAnalyzer can treat them as one
  • Enriches the identity graph for downstream tabaqah inference

No tabaqah is set here — that will be done separately from alf rajul data.
"""

import json
import re
import sys
from pathlib import Path
from collections import Counter, defaultdict
from copy import deepcopy

if sys.platform == "win32":
    import io
    if getattr(sys.stdout, 'encoding', '').lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

SCRIPT_DIR    = Path(__file__).parent
MUJAM_PATH    = SCRIPT_DIR / "rijal_database_mujam.json"
MUFID_PATH    = SCRIPT_DIR / "mufid_statuses.json"
MUFID_MD_PATH = SCRIPT_DIR / "books_md" / "7194.md"
OUTPUT_PATH   = SCRIPT_DIR / "rijal_database_merged.json"

# Status hierarchy (lower = better)
STATUS_RANK = {
    'thiqah':      0,
    'muwaththaq':  1,
    'hasan':       2,
    'mamduh':      3,
    'majhul':      4,
    'daif':        5,
    'unspecified': 6,
}


def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


# ── Step 1: Build cross-reference graph from al-Mufid text ──────────────────

def parse_cross_references(md_path):
    """
    Parse the al-Mufid MD text for cross-reference patterns like:
      - "متحد مع X الثقة الآتي NNN"
      - "متحد مع X المتقدم NNN"
      - "وهو X الثقة الآتي NNN"
      - "وهو X المجهول الآتي NNN"
      - "تقدم في X المجهول NNN"
      - "يأتي في X الثقة NNN"
    Returns dict: entry_num → list of (target_num, relationship_type)
    """
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern: "الثقة" or "المجهول" or "الضعيف" followed by optional الآتي/المتقدم and a number
    # We want to find: source_entry → target_entry with status info

    xrefs = defaultdict(list)  # source_entry_num → [(target_num, status_hint)]

    # Split into lines and find entry boundaries
    lines = content.split('\n')
    current_najaf = None
    current_text_parts = []
    entry_re = re.compile(
        r'^(\d+)\s*-\s*\d+\s*-?\s*\d*\s*-\s*(.+)'
    )

    def process_entry(najaf, text):
        """Extract cross-references from a single entry's text."""
        refs = []

        # Pattern 1: "متحد مع ... الثقة ... NNN" or "متحد مع ... المجهول ... NNN"
        for m in re.finditer(
            r'متحد\s+مع\s+(.+?)\s+(الثقة|المجهول|الضعيف|الممدوح).*?(\d{1,5})',
            text
        ):
            target_name = m.group(1).strip()
            status_word = m.group(2)
            target_num = m.group(3)
            refs.append({
                'target': target_num,
                'type': 'identical',
                'target_name': target_name,
                'status_hint': _status_word_to_code(status_word),
            })

        # Pattern 2: "وهو X الثقة/المجهول الآتي/المتقدم NNN"
        for m in re.finditer(
            r'(?:وهو|هو)\s+(.+?)\s+(الثقة|المجهول|الضعيف|الممدوح).*?(\d{1,5})',
            text
        ):
            refs.append({
                'target': m.group(3),
                'type': 'identical',
                'target_name': m.group(1).strip(),
                'status_hint': _status_word_to_code(m.group(2)),
            })

        # Pattern 3: "تقدم في X الثقة/المجهول NNN"
        for m in re.finditer(
            r'تقدم\s+في\s+(.+?)\s+(الثقة|المجهول|الضعيف|الممدوح).*?(\d{1,5})',
            text
        ):
            refs.append({
                'target': m.group(3),
                'type': 'cross_ref',
                'target_name': m.group(1).strip(),
                'status_hint': _status_word_to_code(m.group(2)),
            })

        # Pattern 4: "يأتي في X الثقة/المجهول NNN"
        for m in re.finditer(
            r'يأتي\s+في\s+(.+?)\s+(الثقة|المجهول|الضعيف|الممدوح).*?(\d{1,5})',
            text
        ):
            refs.append({
                'target': m.group(3),
                'type': 'cross_ref',
                'target_name': m.group(1).strip(),
                'status_hint': _status_word_to_code(m.group(2)),
            })

        # Pattern 5: simple number references like "الآتي NNN" or "المتقدم NNN"
        for m in re.finditer(
            r'(?:الآتي|المتقدم|الآتية|المتقدمة)\s+(\d{1,5})',
            text
        ):
            # Deduplicate — only add if not already captured by a more specific pattern
            target = m.group(1)
            if not any(r['target'] == target for r in refs):
                refs.append({
                    'target': target,
                    'type': 'reference',
                    'target_name': None,
                    'status_hint': None,
                })

        # Pattern 6: "متحد مع لاحقه" or "متحد مع سابقه" (adjacent entry)
        if re.search(r'متحد\s+مع\s+لاحقه', text):
            next_num = str(int(najaf) + 1)
            refs.append({
                'target': next_num,
                'type': 'identical',
                'target_name': None,
                'status_hint': None,
            })
        if re.search(r'متحد\s+مع\s+سابقه', text):
            prev_num = str(int(najaf) - 1) if int(najaf) > 0 else None
            if prev_num:
                refs.append({
                    'target': prev_num,
                    'type': 'identical',
                    'target_name': None,
                    'status_hint': None,
                })

        # Pattern 7: "متحد مع لاحقه الثقة" or "متحد مع سابقه الثقة"
        m = re.search(r'متحد\s+مع\s+لاحقه\s+(الثقة|المجهول|الضعيف)', text)
        if m:
            next_num = str(int(najaf) + 1)
            # Update existing ref if present
            for r in refs:
                if r['target'] == next_num:
                    r['status_hint'] = _status_word_to_code(m.group(1))
                    break

        m = re.search(r'متحد\s+مع\s+سابقه\s+(الثقة|المجهول|الضعيف)', text)
        if m and int(najaf) > 0:
            prev_num = str(int(najaf) - 1)
            for r in refs:
                if r['target'] == prev_num:
                    r['status_hint'] = _status_word_to_code(m.group(1))
                    break

        return refs

    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#') or stripped.startswith('>') or stripped.startswith('---'):
            continue

        m = entry_re.match(stripped)
        if m:
            # Process previous entry
            if current_najaf is not None:
                full_text = ' '.join(current_text_parts)
                refs = process_entry(current_najaf, full_text)
                if refs:
                    xrefs[current_najaf] = refs
            current_najaf = m.group(1).strip()
            current_text_parts = [m.group(2).strip()]
        elif current_najaf is not None and stripped:
            current_text_parts.append(stripped)

    # Process last entry
    if current_najaf is not None:
        full_text = ' '.join(current_text_parts)
        refs = process_entry(current_najaf, full_text)
        if refs:
            xrefs[current_najaf] = refs

    return dict(xrefs)


def _status_word_to_code(word):
    mapping = {
        'الثقة': 'thiqah',
        'المجهول': 'majhul',
        'الضعيف': 'daif',
        'الممدوح': 'mamduh',
    }
    return mapping.get(word, None)


# ── Step 2: Build identity clusters (Union-Find) ────────────────────────────

class UnionFind:
    """Union-Find for building identity clusters from cross-references."""
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1

    def clusters(self):
        groups = defaultdict(set)
        for x in self.parent:
            groups[self.find(x)].add(x)
        return dict(groups)


def _name_tokens(entry):
    """Extract meaningful name tokens from an entry (excluding particles)."""
    STOP = {'بن', 'ابن', 'بنت', 'أبي', 'أبو', 'ابي', 'ابو', 'أم', 'ام', 'آل', 'عليه', 'السلام', 'عليهم'}
    name = entry.get('name_ar', '')
    tokens = set(name.split()) - STOP
    for alias in (entry.get('aliases') or []):
        tokens |= set(alias.split()) - STOP
    return tokens


def _first_personal_name(name_ar):
    """Extract the first personal name (not a particle) from an Arabic name."""
    STOP = {'بن', 'ابن', 'بنت', 'أبي', 'أبو', 'ابي', 'ابو', 'أم', 'ام', 'آل'}
    for word in name_ar.split():
        if word not in STOP:
            return word
    return None


def _arabic_to_int(arabic_str):
    """Convert Arabic-Indic numeral string ١٢٣ to int 123."""
    AR_DIGITS = {'٠': '0', '١': '1', '٢': '2', '٣': '3', '٤': '4',
                 '٥': '5', '٦': '6', '٧': '7', '٨': '8', '٩': '9'}
    western = ''.join(AR_DIGITS.get(c, c) for c in arabic_str.strip())
    try:
        return int(western)
    except ValueError:
        return None


def _normalize_name_for_match(name):
    """Normalize name strings for robust mapping between DBs."""
    import re
    if not name:
        return ""
    n = re.sub(r'[ـ.,:;\"\'-]', '', name)
    n = n.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
    # sometimes there's a space after بن
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def build_najaf_to_mujam_map(mufid_statuses, mujam_db):
    """
    Build a mapping from Mufid's Najaf entry number (int) to mujam DB key (str).
    Because `mujam_db` features OCR artifacts and offset `_num_najaf` (Tehran vs Najaf),
    we match primarily using normalized names. If names appear multiple times,
    we use the difference in `najaf_num` vs `_num_najaf` to disambiguate.
    """
    najaf_to_key = {}
    
    # 1. Index Mujam DB by normalized name
    mujam_name_idx = {}
    for key, entry in mujam_db.items():
        norm_name = _normalize_name_for_match(entry.get('name_ar', ''))
        if not norm_name:
            continue
        if norm_name not in mujam_name_idx:
            mujam_name_idx[norm_name] = []
        mujam_name_idx[norm_name].append(key)
        
    # 2. Main mapping procedure
    for mufid_najaf_str, m_entry in mufid_statuses.items():
        mufid_najaf_int = int(mufid_najaf_str)
        norm_name = _normalize_name_for_match(m_entry.get('name_ar', ''))
        matched_key = None
        
        if norm_name in mujam_name_idx:
            candidates = mujam_name_idx[norm_name]
            if len(candidates) == 1:
                matched_key = candidates[0]
            else:
                # Disambiguate by closest numeric indexing match
                best_diff = 999999
                for cand in candidates:
                    najaf_ar = mujam_db[cand].get('_num_najaf', '')
                    cand_int = _arabic_to_int(najaf_ar) if najaf_ar else (int(cand) + 1)
                    diff = abs(cand_int - mufid_najaf_int) if cand_int else 999999
                    if diff < best_diff:
                        best_diff = diff
                        matched_key = cand
        
        if not matched_key:
            # Fallback: attempt lookup by _num_najaf if at least 1 token is shared
            mufid_tokens = set(norm_name.split())
            best_diff = 999999
            for k, e in mujam_db.items():
                najaf_ar = e.get('_num_najaf', '')
                if najaf_ar:
                    cand_int = _arabic_to_int(najaf_ar)
                    if cand_int == mufid_najaf_int:
                        cand_name = _normalize_name_for_match(e.get('name_ar', ''))
                        cand_tokens = set(cand_name.split())
                        # Need at least one shared token (ignoring common ones)
                        stop = {'بن', 'ابن', 'ابو', 'ابي', 'الكوفي'}
                        if (mufid_tokens - stop) & (cand_tokens - stop):
                            matched_key = k
                            break

        if matched_key:
            najaf_to_key[mufid_najaf_int] = matched_key

    return najaf_to_key


def build_identity_clusters(xrefs, mujam_db, najaf_to_mujam):
    """
    Build clusters of identical entries using:
    1. Cross-references from al-Mufid text (متحد مع patterns) — TRUSTED
    2. same_as_entry_nums from mujam — VALIDATED (require name token overlap)

    The mujam DB's same_as_entry_nums contain LLM hallucinations, so we
    only trust them when the two entries share at least one meaningful
    name token.
    """
    uf = UnionFind()

    # From cross-references (al-Mufid) — these are reliable
    # Use the proper Najaf→mujam mapping
    mufid_linked = 0
    mufid_missed = 0
    for source_najaf, refs in xrefs.items():
        for ref in refs:
            if ref['type'] == 'identical':
                src_najaf_int = int(source_najaf) if source_najaf.isdigit() else None
                tgt_najaf_int = int(ref['target']) if ref['target'].isdigit() else None

                src_key = najaf_to_mujam.get(src_najaf_int) if src_najaf_int else None
                tgt_key = najaf_to_mujam.get(tgt_najaf_int) if tgt_najaf_int else None

                if src_key and tgt_key and src_key in mujam_db and tgt_key in mujam_db:
                    uf.union(src_key, tgt_key)
                    mufid_linked += 1
                else:
                    mufid_missed += 1

    print(f"   Al-Mufid identity links: {mufid_linked:,} applied, {mufid_missed:,} missed (no mapping)")

    # From mujam database's same_as_entry_nums — VALIDATED
    validated = 0
    rejected = 0
    for key, entry in mujam_db.items():
        src_tokens = _name_tokens(entry)
        if not src_tokens:
            continue

        # Get the first personal name (not a particle) for stricter matching
        src_name = entry.get('name_ar', '')
        src_first = _first_personal_name(src_name)

        same_as = entry.get('same_as_entry_nums') or []
        for target in same_as:
            if target not in mujam_db:
                continue
            tgt_tokens = _name_tokens(mujam_db[target])
            tgt_name = mujam_db[target].get('name_ar', '')
            tgt_first = _first_personal_name(tgt_name)

            shared = src_tokens & tgt_tokens

            # Require EITHER:
            # - 2+ shared content tokens, OR
            # - 1 shared token where the first personal names match
            if len(shared) >= 2 or (len(shared) >= 1 and src_first and src_first == tgt_first):
                uf.union(key, target)
                validated += 1
            else:
                rejected += 1

    print(f"   Mujam same_as links: {validated:,} validated, {rejected:,} rejected (no name overlap)")
    return uf


# ── Step 3: Resolve statuses and merge ──────────────────────────────────────

def find_best_status_in_cluster(cluster_keys, mufid_statuses, mujam_db):
    """
    For a cluster of identical entries, find the best (highest priority) status.
    Priority: thiqah > muwaththaq > hasan > mamduh > majhul > daif > unspecified
    """
    best_status = 'unspecified'
    best_detail = None
    best_source = None
    best_rank = STATUS_RANK.get('unspecified', 99)

    for key in cluster_keys:
        # Check mufid_statuses first (most accurate)
        if key in mufid_statuses:
            ms = mufid_statuses[key]
            rank = STATUS_RANK.get(ms['status'], 99)
            if rank < best_rank:
                best_rank = rank
                best_status = ms['status']
                best_detail = ms.get('status_detail')
                best_source = ms.get('status_source')

        # Also check mujam DB
        if key in mujam_db:
            me = mujam_db[key]
            rank = STATUS_RANK.get(me.get('status', 'unspecified'), 99)
            if rank < best_rank:
                best_rank = rank
                best_status = me['status']
                best_detail = me.get('status_detail')
                best_source = me.get('status_source')

    return best_status, best_detail, best_source


def collect_all_names_in_cluster(cluster_keys, mujam_db):
    """Collect all name forms across a cluster of identical entries."""
    names = set()
    for key in cluster_keys:
        if key in mujam_db:
            e = mujam_db[key]
            if e.get('name_ar'):
                names.add(e['name_ar'])
            for alias in (e.get('aliases') or []):
                if alias:
                    names.add(alias)
    return list(names)


def merge_chain_data(cluster_keys, mujam_db):
    """Merge chain data across all entries in a cluster."""
    merged_imams = set()
    merged_from = set()
    merged_by = set()
    merged_books = set()
    merged_aliases = set()
    merged_companions = set()

    for key in cluster_keys:
        if key in mujam_db:
            e = mujam_db[key]
            for x in (e.get('narrates_from_imams') or []):
                merged_imams.add(x)
            for x in (e.get('narrates_from_narrators') or []):
                merged_from.add(x)
            for x in (e.get('narrated_from_by') or []):
                merged_by.add(x)
            for x in (e.get('books') or []):
                merged_books.add(x)
            for x in (e.get('aliases') or []):
                if x:
                    merged_aliases.add(x)
            if e.get('companions_of'):
                merged_companions.add(e['companions_of'])

    return {
        'narrates_from_imams': sorted(merged_imams),
        'narrates_from_narrators': sorted(merged_from),
        'narrated_from_by': sorted(merged_by),
        'books': sorted(merged_books),
        'aliases': sorted(merged_aliases),
        'companions_of': ', '.join(sorted(merged_companions)) if merged_companions else None,
    }


def main():
    print("═" * 70)
    print("  RIJAL DATABASE MERGER")
    print("═" * 70)

    # ── Load data ──────────────────────────────────────────────────────────────
    print("\n1. Loading databases...")
    mujam_db = load_json(MUJAM_PATH)
    mufid_statuses = load_json(MUFID_PATH)
    print(f"   Mujam DB:       {len(mujam_db):,} entries")
    print(f"   Mufid statuses: {len(mufid_statuses):,} entries")

    # ── Parse cross-references ────────────────────────────────────────────────
    print("\n2. Parsing cross-references from al-Mufid text...")
    xrefs = parse_cross_references(MUFID_MD_PATH)
    total_refs = sum(len(v) for v in xrefs.values())
    identical_refs = sum(
        1 for refs in xrefs.values()
        for r in refs if r['type'] == 'identical'
    )
    print(f"   Entries with cross-refs: {len(xrefs):,}")
    print(f"   Total references:        {total_refs:,}")
    print(f"   Identity links (متحد):   {identical_refs:,}")

    # ── Build Najaf → mujam key mapping ────────────────────────────────────────
    print("\n3. Building identity clusters...")
    najaf_to_mujam = build_najaf_to_mujam_map(mufid_statuses, mujam_db)
    print(f"   Najaf→mujam mapping: {len(najaf_to_mujam):,} entries")

    # Build reverse map: mujam key → Najaf int
    mujam_to_najaf = {v: k for k, v in najaf_to_mujam.items()}

    uf = build_identity_clusters(xrefs, mujam_db, najaf_to_mujam)
    clusters = uf.clusters()

    # ── Post-processing: break oversized clusters ─────────────────────────
    # Clusters > MAX_CLUSTER entries are likely contaminated by transitive
    # chain errors. Break them into individual entries.
    MAX_CLUSTER = 6
    broken = 0
    cleaned_clusters = {}
    for root, members in clusters.items():
        if len(members) > MAX_CLUSTER:
            # Break into individual singletons
            for m in members:
                cleaned_clusters[m] = {m}
            broken += 1
        else:
            cleaned_clusters[root] = members
    clusters = cleaned_clusters
    if broken:
        print(f"   ⚠ Broke {broken} oversized clusters (>{MAX_CLUSTER} members) into singletons")

    multi_clusters = {k: v for k, v in clusters.items() if len(v) > 1}
    print(f"   Total clusters:     {len(clusters):,}")
    print(f"   Multi-entry clusters: {len(multi_clusters):,}")
    print(f"   Entries in clusters:  {sum(len(v) for v in multi_clusters.values()):,}")

    # Show largest clusters
    largest = sorted(multi_clusters.items(), key=lambda x: -len(x[1]))[:10]
    print(f"\n   Top 10 largest identity clusters:")
    for root, members in largest:
        names = []
        for m in sorted(members, key=lambda x: int(x) if x.isdigit() else 99999):
            if m in mujam_db:
                names.append(mujam_db[m].get('name_ar', m))
            else:
                names.append(f"#{m}")
        print(f"     [{root:>5}] ({len(members)} entries): {' = '.join(names[:4])}")

    # ── Merge databases ──────────────────────────────────────────────────────
    print("\n4. Merging databases...")

    # Entry → canonical mapping
    entry_to_canonical = {}
    for root, members in clusters.items():
        for m in members:
            entry_to_canonical[m] = root

    merged_db = {}
    status_updates = Counter()
    identity_enrichments = 0

    for key in sorted(mujam_db.keys(), key=lambda x: int(x) if x.isdigit() else 99999):
        entry = deepcopy(mujam_db[key])

        # ── Step A: Apply mufid status (most accurate) ─────────────────────
        # Map mujam key → Najaf number → mufid key (which is keyed by Najaf num)
        najaf_num = mujam_to_najaf.get(key)
        mufid_key = str(najaf_num) if najaf_num else None

        if mufid_key and mufid_key in mufid_statuses:
            ms = mufid_statuses[mufid_key]
            old_status = entry.get('status', 'unspecified')
            new_status = ms['status']

            # Always trust the clean mufid extraction over mujam's LLM extraction
            # Exception: if mufid says unspecified but mujam has a real status,
            # keep mujam's (the LLM may have found it in the full text)
            if new_status != 'unspecified':
                entry['status'] = new_status
                entry['status_detail'] = ms.get('status_detail')
                if ms.get('status_source'):
                    entry['status_source'] = ms['status_source']
                status_updates[f"{old_status} → {new_status}"] += 1
            elif old_status == 'unspecified' and new_status == 'unspecified':
                pass  # both agree
            else:
                # mufid=unspecified, mujam has something — keep mujam's
                status_updates[f"kept mujam: {old_status}"] += 1

        # ── Step B: Resolve status from identity cluster ───────────────────
        canonical = entry_to_canonical.get(key, key)
        cluster_members = clusters.get(canonical, {key})

        if len(cluster_members) > 1:
            # Find the best status in the cluster (using mufid keys)
            mufid_cluster_keys = set()
            for m in cluster_members:
                najaf = mujam_to_najaf.get(m)
                if najaf:
                    mufid_cluster_keys.add(str(najaf))
                else:
                    mufid_cluster_keys.add(m)

            best_status, best_detail, best_source = find_best_status_in_cluster(
                mufid_cluster_keys, mufid_statuses, mujam_db
            )

            # If this entry's status is worse than the cluster's best, upgrade it
            current_rank = STATUS_RANK.get(entry.get('status', 'unspecified'), 99)
            best_rank = STATUS_RANK.get(best_status, 99)

            if best_rank < current_rank:
                entry['status'] = best_status
                if best_detail:
                    entry['status_detail'] = best_detail
                if best_source:
                    entry['status_source'] = best_source
                entry['status_inherited_from'] = canonical
                identity_enrichments += 1

            # ── Step C: Enrich aliases and same_as with cluster info ────────
            all_cluster_names = collect_all_names_in_cluster(cluster_members, mujam_db)
            my_name = entry.get('name_ar', '')

            # Add other names from cluster as aliases
            existing_aliases = set(entry.get('aliases') or [])
            for name in all_cluster_names:
                if name != my_name and name not in existing_aliases:
                    existing_aliases.add(name)
            entry['aliases'] = sorted(existing_aliases)

            # Set same_as links
            other_keys = [m for m in sorted(cluster_members) if m != key]
            entry['same_as_entry_nums'] = other_keys
            entry['same_as_names'] = [
                mujam_db[m].get('name_ar', '') for m in other_keys if m in mujam_db
            ]

            # Set canonical key for the cluster
            entry['canonical_entry'] = canonical

            # ── Step D: Merge chain data from cluster ──────────────────────
            merged_chains = merge_chain_data(cluster_members, mujam_db)
            # Union-merge: keep existing + add from cluster
            for field in ['narrates_from_imams', 'narrates_from_narrators',
                         'narrated_from_by', 'books']:
                existing = set(entry.get(field) or [])
                existing.update(merged_chains[field])
                entry[field] = sorted(existing)

            # Merge companions_of
            if merged_chains['companions_of'] and not entry.get('companions_of'):
                entry['companions_of'] = merged_chains['companions_of']

        # ── Step E: Clear tabaqah (will be re-inferred later) ─────────────
        entry['tabaqah'] = None
        entry['tabaqah_source'] = None
        entry['tabaqah_confidence'] = None

        merged_db[key] = entry

    # ── Statistics ─────────────────────────────────────────────────────────────
    print(f"\n   Status updates applied:")
    for change, count in status_updates.most_common(20):
        print(f"     {change}: {count:,}")
    print(f"   Identity-inherited upgrades: {identity_enrichments:,}")

    # Final status distribution
    final_dist = Counter(e.get('status', 'unspecified') for e in merged_db.values())
    print(f"\n5. Final status distribution (merged DB):")
    print(f"   {'Status':<15} {'Count':>8}")
    print(f"   {'─'*25}")
    for status in sorted(final_dist.keys(), key=lambda s: STATUS_RANK.get(s, 99)):
        count = final_dist[status]
        pct = 100 * count / len(merged_db)
        print(f"   {status:<15} {count:>8,} ({pct:>5.1f}%)")
    print(f"   {'─'*25}")
    print(f"   {'TOTAL':<15} {len(merged_db):>8,}")

    # Count cluster-enriched entries
    has_canonical = sum(1 for e in merged_db.values() if e.get('canonical_entry'))
    has_same_as = sum(1 for e in merged_db.values() if len(e.get('same_as_entry_nums') or []) > 0)
    has_aliases = sum(1 for e in merged_db.values() if len(e.get('aliases') or []) > 0)
    print(f"\n   Identity graph enrichment:")
    print(f"   Entries with canonical_entry: {has_canonical:,}")
    print(f"   Entries with same_as links:   {has_same_as:,}")
    print(f"   Entries with aliases:         {has_aliases:,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    print(f"\n6. Saving to {OUTPUT_PATH.name}...")
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(merged_db, f, ensure_ascii=False, indent=2)
    size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"   Saved: {len(merged_db):,} entries, {size_mb:.1f} MB")

    # ── Save identity index for IsnadAnalyzer ─────────────────────────────────
    identity_index_path = SCRIPT_DIR / "rijal_identities.json"
    identity_data = {
        'entry_to_canonical': {},
        'clusters': {},
    }
    for root, members in clusters.items():
        if len(members) <= 1:
            continue
        for m in members:
            identity_data['entry_to_canonical'][m] = root

        # Build cluster data with merged chain info
        merged_chains = merge_chain_data(members, merged_db)
        # Find the best entry for display
        best_key = root
        best_rank = 99
        for m in members:
            if m in merged_db:
                rank = STATUS_RANK.get(merged_db[m].get('status', 'unspecified'), 99)
                if rank < best_rank:
                    best_rank = rank
                    best_key = m

        identity_data['clusters'][root] = {
            'members': sorted(members),
            'best_entry': best_key,
            'status': merged_db.get(best_key, {}).get('status', 'unspecified'),
            'name_ar': merged_db.get(best_key, {}).get('name_ar', ''),
            **merged_chains,
        }

    with open(identity_index_path, 'w', encoding='utf-8') as f:
        json.dump(identity_data, f, ensure_ascii=False, indent=2)
    print(f"   Identity index: {len(identity_data['clusters']):,} clusters → {identity_index_path.name}")

    print(f"\n{'═'*70}")
    print(f"  MERGE COMPLETE")
    print(f"  Update rijal_resolver.py to use: {OUTPUT_PATH.name}")
    print(f"  Then rebuild: python rijal_resolver.py --build")
    print(f"{'═'*70}")


if __name__ == '__main__':
    main()

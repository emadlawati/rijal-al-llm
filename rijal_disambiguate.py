#!/usr/bin/env python3
"""
Rijāl Disambiguation Pass
==========================
Runs AFTER rijal_builder.py has fully extracted the database.

1. Builds a transitive identity graph from all cross-references
   (متحد مع chains: if A=B and B=C then A=B=C even without A→C explicitly)
2. Detects conflicts within an identity cluster (e.g., one entry says thiqah,
   another says daif for the "same" person)
3. Selects a canonical entry per cluster (the richest/most detailed one)
4. Identifies homonyms — narrators with identical Arabic names but who are
   actually different individuals (different identity clusters)

Output:
    rijal_identities.json   — identity clusters, canonical IDs, homonyms

Usage:
    python rijal_disambiguate.py
    python rijal_disambiguate.py --stats
"""

import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR       = Path(__file__).parent
DATABASE_FILE    = SCRIPT_DIR / "rijal_database.json"
IDENTITIES_FILE  = SCRIPT_DIR / "rijal_identities.json"


# ─── Union-Find (Disjoint Set Union) ─────────────────────────────────────────

class UnionFind:
    def __init__(self, keys):
        self.parent = {k: k for k in keys}
        self.rank   = {k: 0 for k in keys}

    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # path compression
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        # Union by rank
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


# ─── Helpers ──────────────────────────────────────────────────────────────────

def entry_richness_score(entry: dict) -> int:
    """
    Score an entry by how much data it contains.
    Higher = more complete = preferred as canonical.
    """
    score = 0
    non_null_fields = ['name_ar', 'name_en', 'father', 'grandfather', 'nasab',
                       'kunyah', 'laqab', 'nisba', 'status_detail', 'status_source',
                       'companions_of', 'hadith_count', 'tariq_status',
                       'disambiguation_notes', 'period_hint', 'notes']
    for f in non_null_fields:
        if entry.get(f):
            score += 1

    list_fields = ['narrates_from_imams', 'books', 'aliases',
                   'narrates_from_narrators', 'narrated_from_by']
    for f in list_fields:
        score += len(entry.get(f) or [])

    if entry.get('has_book'):
        score += 2
    if entry.get('status') and entry['status'] != 'unspecified':
        score += 3
    return score


def detect_era_conflict(entries: list[dict]) -> tuple[bool, str]:
    """
    Check if entries in a cluster have incompatible Imam narration eras.
    Returns (conflict: bool, description: str).
    """
    # Ordered Imam eras (rough AH century)
    IMAM_ERA = {
        "رسول الله (ص)":    1,
        "أمير المؤمنين (ع)": 1,
        "الحسن (ع)":        1,
        "الحسين (ع)":       1,
        "السجاد (ع)":       1,
        "الباقر (ع)":       2,
        "الصادق (ع)":       2,
        "الكاظم (ع)":       2,
        "الرضا (ع)":        3,
        "الجواد (ع)":       3,
        "الهادي (ع)":       3,
        "العسكري (ع)":      3,
        "المهدي (عج)":      3,
    }

    all_eras = set()
    for entry in entries:
        for imam in (entry.get('narrates_from_imams') or []):
            era = IMAM_ERA.get(imam)
            if era:
                all_eras.add(era)

    if len(all_eras) > 1:
        span = max(all_eras) - min(all_eras)
        if span > 1:
            return True, f"Entries narrate across {len(all_eras)} different Imam eras (centuries {sorted(all_eras)})"
    return False, ""


def detect_status_conflict(entries: list[dict]) -> tuple[bool, str]:
    """
    Check if entries in a cluster have contradictory status rulings.
    """
    definite_statuses = [e.get('status') for e in entries
                         if e.get('status') and e['status'] not in ('unspecified', None)]
    unique_definite = set(definite_statuses)

    # These pairs are incompatible
    INCOMPATIBLE = [
        {'thiqah', 'daif'},
        {'thiqah', 'majhul'},
        {'muwaththaq', 'daif'},
    ]
    for pair in INCOMPATIBLE:
        if pair.issubset(unique_definite):
            return True, f"Conflicting rulings in cluster: {unique_definite}"
    return False, ""


# ─── Main ─────────────────────────────────────────────────────────────────────

def build_identities(db: dict) -> dict:
    """
    Build the full identity graph and return the identities structure.
    """
    keys = list(db.keys())
    uf = UnionFind(keys)

    # ── Step 1: Union all explicit cross-references ──────────────────────────
    linked = 0
    for key, entry in db.items():
        # From same_as_entry_nums (new field from enhanced builder)
        for ref in (entry.get('same_as_entry_nums') or []):
            if ref in db:
                uf.union(key, ref)
                linked += 1

        # From aliases / alias_entry_nums (original field)
        for ref in (entry.get('alias_entry_nums') or []):
            if ref in db:
                uf.union(key, ref)
                linked += 1

    print(f"  Linked {linked} explicit identity pairs")

    # ── Step 2: Group into clusters ──────────────────────────────────────────
    clusters: dict[str, list[str]] = defaultdict(list)
    for key in keys:
        root = uf.find(key)
        clusters[root].append(key)

    singleton_clusters = sum(1 for c in clusters.values() if len(c) == 1)
    multi_clusters = sum(1 for c in clusters.values() if len(c) > 1)
    print(f"  Total clusters: {len(clusters):,}  "
          f"(singleton: {singleton_clusters:,}, multi-entry: {multi_clusters:,})")

    # ── Step 3: Analyze each cluster ─────────────────────────────────────────
    cluster_data = {}
    conflict_count = 0
    total_members = 0

    for root, members in clusters.items():
        entries = [db[m] for m in members]
        total_members += len(members)

        # Pick canonical entry (highest richness score)
        scored = [(entry_richness_score(db[m]), m) for m in members]
        scored.sort(reverse=True)
        canonical_key = scored[0][1]
        canonical = db[canonical_key]

        # Detect conflicts
        status_conflict, status_msg = detect_status_conflict(entries)
        era_conflict, era_msg = detect_era_conflict(entries)
        has_conflict = status_conflict or era_conflict

        if has_conflict:
            conflict_count += 1

        # Merge status: if canonical says 'unspecified' but another member
        # has a definite status, inherit it
        merged_status = canonical.get('status', 'unspecified')
        if merged_status == 'unspecified':
            for e in entries:
                if e.get('status') and e['status'] != 'unspecified':
                    merged_status = e['status']
                    break

        # Merge Imam list across all cluster members
        all_imams = set()
        for e in entries:
            all_imams.update(e.get('narrates_from_imams') or [])

        # Merge narration network
        all_teachers = set()
        all_students = set()
        for e in entries:
            all_teachers.update(e.get('narrates_from_narrators') or [])
            all_students.update(e.get('narrated_from_by') or [])

        cluster_data[root] = {
            "canonical_key":        canonical_key,
            "members":              sorted(members),
            "member_count":         len(members),
            "canonical_name_ar":    canonical.get('name_ar', ''),
            "canonical_name_en":    canonical.get('name_en', ''),
            "status":               merged_status,
            "conflict":             has_conflict,
            "conflict_notes":       '; '.join(filter(None, [status_msg, era_msg])),
            "narrates_from_imams":  sorted(all_imams),
            "narrates_from_narrators": sorted(all_teachers),
            "narrated_from_by":     sorted(all_students),
            "has_book":             any(e.get('has_book') for e in entries),
            "books":                sorted({b for e in entries for b in (e.get('books') or [])}),
        }

    print(f"  Conflicts detected: {conflict_count}")

    # ── Step 4: Detect homonyms ───────────────────────────────────────────────
    # Group canonical entries by Arabic name
    name_to_clusters: dict[str, list[str]] = defaultdict(list)
    for root, cdata in cluster_data.items():
        name = cdata['canonical_name_ar'].strip()
        if name:
            name_to_clusters[name].append(root)

    homonyms: dict[str, list[dict]] = {}
    for name, roots in name_to_clusters.items():
        if len(roots) > 1:
            homonyms[name] = [
                {
                    "cluster_root": r,
                    "canonical_key": cluster_data[r]['canonical_key'],
                    "status": cluster_data[r]['status'],
                    "narrates_from_imams": cluster_data[r]['narrates_from_imams'],
                    "member_count": cluster_data[r]['member_count'],
                }
                for r in roots
            ]

    print(f"  Homonyms detected: {len(homonyms):,} names with multiple distinct narrators")

    # ── Step 5: Build per-entry lookup of canonical_key ───────────────────────
    entry_to_canonical: dict[str, str] = {}
    for root, cdata in cluster_data.items():
        for member in cdata['members']:
            entry_to_canonical[member] = cdata['canonical_key']

    return {
        "clusters":            cluster_data,
        "homonyms":            homonyms,
        "entry_to_canonical":  entry_to_canonical,
        "stats": {
            "total_entries":       len(db),
            "total_clusters":      len(cluster_data),
            "singleton_clusters":  singleton_clusters,
            "multi_clusters":      multi_clusters,
            "total_homonym_names": len(homonyms),
            "conflict_clusters":   conflict_count,
        }
    }


def print_stats(identities: dict) -> None:
    s = identities.get('stats', {})
    print(f"\n{'='*60}")
    print(f"  RIJĀL IDENTITY GRAPH STATISTICS")
    print(f"{'='*60}")
    print(f"  Total entries in DB:    {s.get('total_entries', '?'):>6,}")
    print(f"  Identity clusters:      {s.get('total_clusters', '?'):>6,}")
    print(f"    Singleton (unique):   {s.get('singleton_clusters', '?'):>6,}")
    print(f"    Multi-entry clusters: {s.get('multi_clusters', '?'):>6,}")
    print(f"  Conflict clusters:      {s.get('conflict_clusters', '?'):>6,}")
    print(f"  Homonym names:          {s.get('total_homonym_names', '?'):>6,}")
    print(f"{'='*60}\n")

    # Show some conflicts
    conflicts = [
        (r, c) for r, c in identities['clusters'].items() if c['conflict']
    ]
    if conflicts:
        print("CONFLICT CLUSTERS (first 10):")
        for root, c in conflicts[:10]:
            print(f"  [{root}] {c['canonical_name_ar']}")
            print(f"    Members: {c['members']}")
            print(f"    Issue:   {c['conflict_notes']}")
            print()

    # Show some homonyms
    print("HOMONYM NAMES (first 10 — same name, different narrators):")
    for name, variants in list(identities['homonyms'].items())[:10]:
        print(f"  {name}")
        for v in variants:
            print(f"    → [{v['canonical_key']}] status={v['status']}  "
                  f"imams={v['narrates_from_imams']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Rijāl Disambiguation Pass")
    parser.add_argument("--stats", action="store_true",
                        help="Print stats from existing identities file and exit")
    args = parser.parse_args()

    if args.stats:
        if not IDENTITIES_FILE.exists():
            print("No identities file found. Run disambiguation first.")
            sys.exit(1)
        with open(IDENTITIES_FILE, 'r', encoding='utf-8') as f:
            identities = json.load(f)
        print_stats(identities)
        return

    if not DATABASE_FILE.exists():
        print(f"ERROR: {DATABASE_FILE} not found.")
        print("Run rijal_builder.py first to generate the database.")
        sys.exit(1)

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    print(f"Loading database from {DATABASE_FILE.name} ...")
    with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
        db = json.load(f)
    print(f"  {len(db):,} entries loaded\n")

    print("Building identity graph ...")
    identities = build_identities(db)

    # ── Write output ──────────────────────────────────────────────────────────
    with open(IDENTITIES_FILE, 'w', encoding='utf-8') as f:
        json.dump(identities, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved → {IDENTITIES_FILE.name}")

    # ── Annotate the database with canonical_key ───────────────────────────
    print("Annotating database entries with canonical_key ...")
    changed = 0
    for key, entry in db.items():
        canonical = identities['entry_to_canonical'].get(key, key)
        if entry.get('canonical_key') != canonical:
            entry['canonical_key'] = canonical
            changed += 1

    with open(DATABASE_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"  Updated {changed:,} entries with canonical_key")

    print_stats(identities)


if __name__ == "__main__":
    main()

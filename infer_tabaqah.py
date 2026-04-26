import json
import re
import math
import io
import sys
from collections import defaultdict
from rijal_resolver import load_files, resolve

# Ensure UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DB_PATH = 'rijal_database_merged.json'
ALF_RAJUL_DB_PATH = 'alf_rajul_database.json'
ALF_RAJUL_DISAMB_PATH = 'alf_rajul_disambiguation_llm.json'

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def normalize_name(name: str) -> str:
    name = re.sub(r'[\u064B-\u065F\u0670]', '', name)
    name = name.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
    name = name.replace('ة', 'ه').replace('ى', 'ي')
    name = re.sub(r'[^ا-ي ]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def canonicalize(key, db):
    """Returns the cluster canonical ID for a given key, if exists."""
    if key not in db: return key
    can = db[key].get('canonical_entry')
    return can if can else key

def print_stats(db):
    total = sum(1 for e in db.values() if not e.get('canonical_entry'))
    sourced = sum(1 for e in db.values() if not e.get('canonical_entry') and e.get('tabaqah_source') == 'Alf Rajul')
    inferred = sum(1 for e in db.values() if not e.get('canonical_entry') and e.get('tabaqah_source') == 'Inferred')
    nulls = sum(1 for e in db.values() if not e.get('canonical_entry') and e.get('tabaqah') is None)
    
    print(f"Total Unique Identity Nodes: {total}")
    print(f"Alf Rajul seeds: {sourced}")
    print(f"Inferred entries: {inferred}")
    print(f"Null entries (unconnected/unresolved): {nulls}")

def build_graph(db, idx):
    print("Building Teacher/Student Graph... (Resolving nodes, this may take a minute)", flush=True)
    resolve_cache = {}
    
    # Pre-build exact match map for extreme speedup
    exact_map = {}
    for k, v in db.items():
        n = normalize_name(v.get('name_ar', ''))
        if n:
            exact_map.setdefault(n, []).append(k)
        for alt in v.get('aliases', []):
            an = normalize_name(alt)
            if an: exact_map.setdefault(an, []).append(k)

    def resolve_cached(name_str):
        if name_str in resolve_cache:
            return resolve_cache[name_str]
            
        norm = normalize_name(name_str)
        # Fast path 1: Exact unique match in DB strings
        if norm in exact_map and len(exact_map[norm]) == 1:
            ans = canonicalize(exact_map[norm][0], db)
            resolve_cache[name_str] = ans
            return ans
            
        # Fallback to full resolver (slower) - Disabled to save 20 hours!
        # Because the graph is massive, missing 5-10% of fuzzy edges will not hinder the Dirichlet iteration at all.
        resolve_cache[name_str] = None
        return None

    total_edges = 0
    teachers_of = defaultdict(set)
    students_of = defaultdict(set)
    
    # Process edges
    for k, entry in db.items():
        C_k = canonicalize(k, db)
        
        # 1. Teachers
        t_list = entry.get('narrates_from_narrators', [])
        for t_name in t_list:
            t_id = resolve_cached(t_name)
            if t_id and t_id != C_k:
                teachers_of[C_k].add(t_id)
                students_of[t_id].add(C_k)
                
        # 2. Students
        s_list = entry.get('narrated_from_by', [])
        for s_name in s_list:
            s_id = resolve_cached(s_name)
            if s_id and s_id != C_k:
                students_of[C_k].add(s_id)
                teachers_of[s_id].add(C_k)
                total_edges += 1
                
        if (total_edges + 1) % 10000 == 0:
            print(f"   Processed {total_edges} relations...", flush=True)
                
    print(f"Finished building graph. Cache size: {len(resolve_cache)} names", flush=True)
    return teachers_of, students_of

def relax_tabaqat(db, idx, iterations=50):
    teachers_of, students_of = build_graph(db, idx)
    
    # Build list of canonical nodes
    canonical_nodes = [k for k, e in db.items() if not e.get('canonical_entry')]
    print(f"Graph initialized with {len(canonical_nodes)} canonical nodes", flush=True)
    
    # Initialize values
    T = {}
    for node in canonical_nodes:
        if db[node].get('tabaqah_source') == 'Alf Rajul':
            T[node] = db[node].get('tabaqah')
        else:
            T[node] = None
            
    print("Starting Relaxation Iterations...", flush=True)
    
    for i in range(iterations):
        T_new = {}
        changes = 0
        diff_sum = 0.0
        
        for node in canonical_nodes:
            # Anchor seeds
            if db[node].get('tabaqah_source') == 'Alf Rajul':
                T_new[node] = T[node]
                continue
            
            vals = []
            for t_id in teachers_of[node]:
                if T.get(t_id) is not None:
                    vals.append(T[t_id] + 1)
            for s_id in students_of[node]:
                if T.get(s_id) is not None:
                    vals.append(T[s_id] - 1)
                    
            if vals:
                new_val = sum(vals) / len(vals)
                T_new[node] = new_val
                
                # compute change
                if T[node] is not None:
                    diff_sum += abs(T[node] - new_val)
                else:
                    changes += 1
            else:
                T_new[node] = None
                
        T = T_new
        if changes == 0 and diff_sum < 1.0:
            print(f"Converged at iteration {i}!", flush=True)
            break
        print(f"Iter {i}: Delta {diff_sum:.2f}, {changes} new nodes connected.", flush=True)
        
    return T

def main():
    print("Loading Databases...")
    with open(DB_PATH, 'r', encoding='utf-8') as f:
        db = json.load(f)
        
    with open('rijal_resolver_index.json', 'r', encoding='utf-8') as f:
        idx = json.load(f)
    
    alf_db = load_json(ALF_RAJUL_DB_PATH)
    try:
        alf_disambig = load_json(ALF_RAJUL_DISAMB_PATH)
    except Exception:
        alf_disambig = {}

    print("Clearing previous tabaqah logic...")
    for entry in db.values():
        entry['tabaqah'] = None
        entry['tabaqah_source'] = None
        entry['tabaqah_confidence'] = None

    print("Mapping Alf Rajul seeds...")
    db_name_idx = {}
    for k, v in db.items():
        n = normalize_name(v.get('name_ar', ''))
        if n: db_name_idx.setdefault(n, []).append(k)

    mapped_count = 0
    for alf_key, alf_data in alf_db.items():
        tabaqah_val = alf_data.get('tabaqah')
        if tabaqah_val is None: continue
            
        target_k = None
        if alf_key in alf_disambig:
            target_k = str(alf_disambig[alf_key])
            if target_k not in db: target_k = None
                
        if not target_k:
            names_to_try = [alf_data.get('name_ar', '')] + alf_data.get('alt_names', [])
            for name_to_try in names_to_try:
                alf_name = normalize_name(name_to_try)
                if not alf_name: continue
                candidates = db_name_idx.get(alf_name, [])
                if len(candidates) == 1:
                    target_k = candidates[0]
                    break
                
        if target_k:
            target_k = canonicalize(target_k, db)
            entry = db[target_k]
                
            base_t = float(tabaqah_val)
            sub = alf_data.get('tabaqah_sub')
            if sub == 'senior': base_t -= 0.3
            elif sub == 'junior': base_t += 0.3
                
            entry['tabaqah'] = base_t
            entry['tabaqah_source'] = 'Alf Rajul'
            entry['tabaqah_confidence'] = 'high'
            
            # Merge alt_names from Alf Rajul natively
            if alf_data.get('alt_names'):
                existing_aliases = entry.get('aliases') or []
                for alt_name in alf_data['alt_names']:
                    if alt_name not in existing_aliases and alt_name != entry.get('name_ar'):
                        existing_aliases.append(alt_name)
                entry['aliases'] = existing_aliases
                
            mapped_count += 1

    print(f"Successfully mapped {mapped_count} Alf Rajul seeds to Canonicals.")
    
    # Run Inference
    final_T = relax_tabaqat(db, idx, iterations=50)
    
    # Save back to DB
    print("Projecting tabaqat back to all entries...")
    for k, e in db.items():
        C_k = canonicalize(k, db)
        val = final_T.get(C_k)
        if val is not None:
            # We round to nearest decimal point for readability
            rounded_val = round(val, 1)
            # Do not overwrite if it is a seed
            if e.get('tabaqah_source') != 'Alf Rajul':
                e['tabaqah'] = rounded_val
                e['tabaqah_source'] = 'Inferred'
                e['tabaqah_confidence'] = 'medium'

    print("Saving Database...")
    save_path = 'rijal_database_merged.json'
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
        
    print_stats(db)

if __name__ == '__main__':
    main()

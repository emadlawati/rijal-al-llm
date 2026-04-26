"""
Repair N3 numbering in rijal_entries.json
=========================================
Logic:
1. Identify entries with n3 == 0 or n3 > 15600.
2. For each bad entry, check prev and next n3.
3. If next == prev + 2, current = prev + 1.
4. If next == prev + 1, current = prev (duplicate).
5. Fallback: current = prev + 1.
6. Create backup first.
7. Log all changes.
"""

import json
import shutil
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ENTRIES_FILE = SCRIPT_DIR / "rijal_entries.json"
BACKUP_FILE = SCRIPT_DIR / "rijal_entries_backup.json"
LOG_FILE = SCRIPT_DIR / "repair_log.txt"

_AR = {'٠':0,'١':1,'٢':2,'٣':3,'٤':4,'٥':5,'٦':6,'٧':7,'٨':8,'٩':9}
_INT = {v:k for k,v in _AR.items()}

def ar_to_int(s):
    if not s: return 0
    r = 0
    for c in str(s):
        if c in _AR: r = r*10+_AR[c]
        elif c.isdigit(): r = r*10+int(c)
    return r

def int_to_ar(n):
    if n <= 0: return '٠'
    d = []
    temp_n = n
    while temp_n:
        d.append(_INT[temp_n % 10])
        temp_n //= 10
    return ''.join(reversed(d))

def repair():
    # 1. Backup
    print(f"Creating backup: {BACKUP_FILE.name}")
    shutil.copy2(ENTRIES_FILE, BACKUP_FILE)

    # 2. Load
    print(f"Loading {ENTRIES_FILE.name}")
    with open(ENTRIES_FILE, 'r', encoding='utf-8') as f:
        entries = json.load(f)

    changes = []
    bad_count = 0

    # 3. Process
    for i in range(len(entries)):
        n3_raw = entries[i].get('n3', '٠')
        n3_int = ar_to_int(n3_raw)

        if n3_int == 0 or n3_int > 15617: # 15617 is absolute max known
            bad_count += 1
            prev_n3 = ar_to_int(entries[i-1]['n3']) if i > 0 else 0
            # Need to look ahead for a VALID next_n3
            next_n3 = 0
            for j in range(i + 1, min(i + 10, len(entries))):
                val = ar_to_int(entries[j].get('n3', '٠'))
                if 0 < val <= 16500: # Found a valid neighbor
                    next_n3 = val
                    break
            
            # Inference logic
            inferred = 0
            reason = ""
            
            if prev_n3 > 0 and next_n3 > 0:
                if next_n3 == prev_n3 + 2:
                    inferred = prev_n3 + 1
                    reason = "Gap of 2 found"
                elif next_n3 == prev_n3 + 1:
                    inferred = prev_n3 # Duplicate
                    reason = "Sequential duplicate"
                elif next_n3 > prev_n3:
                    inferred = prev_n3 + 1
                    reason = f"Prev+1 (between {prev_n3} and {next_n3})"
                else:
                    inferred = prev_n3 + 1
                    reason = f"Prev+1 fallback (next={next_n3} < prev={prev_n3})"
            elif prev_n3 > 0:
                inferred = prev_n3 + 1
                reason = "Prev+1 only"
            elif next_n3 > 0:
                inferred = max(1, next_n3 - 1)
                reason = "Next-1 only"
            
            if inferred > 0:
                new_n3 = int_to_ar(inferred)
                changes.append({
                    'idx': i,
                    'old': n3_raw,
                    'old_int': n3_int,
                    'new': new_n3,
                    'new_int': inferred,
                    'reason': reason,
                    'text': entries[i]['text'][:60]
                })
                entries[i]['n3'] = new_n3
                entries[i]['_repaired_n3'] = True

    # 4. Save
    if changes:
        print(f"Applying {len(changes)} repairs...")
        with open(ENTRIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write(f"N3 REPAIR LOG\n{'='*70}\n")
            f.write(f"Bad entries found: {bad_count}\n")
            f.write(f"Repairs applied:  {len(changes)}\n\n")
            for c in changes:
                f.write(f"idx={c['idx']:>5}  '{c['old']}'({c['old_int']}) -> '{c['new']}'({c['new_int']})\n")
                f.write(f"           Reason: {c['reason']}\n")
                f.write(f"           Text  : {c['text']}\n\n")
        print(f"Done. Log written to {LOG_FILE.name}")
    else:
        print("No repairs needed.")

if __name__ == "__main__":
    repair()

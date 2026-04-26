#!/usr/bin/env python3
"""
Rijāl Benchmark
===============
Tests multiple models on extracting 10 entries.
Logs speed, JSON schema errors, and extraction density.

Usage:
    python benchmark_rijal.py
"""

import os
import sys
import json
import time
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BUILDER_SCRIPT = SCRIPT_DIR / "rijal_builder.py"
DB_FILE = SCRIPT_DIR / "rijal_database.json"

MODELS_TO_TEST = [
    "qwen3:14b",
    "qwen2.5:7b",
    "aya-expanse:8b"
]

TEST_COUNT = 10

def evaluate_extraction(db: dict, start_idx: int, count: int) -> dict:
    """Analyze extraction quality for the targeted entries."""
    filled_fields = 0
    total_fields = 0
    null_count = 0
    
    entries_found = 0
    for val in db.values():
        if start_idx <= val.get("_entry_idx", -1) < start_idx + count:
            entries_found += 1
            for k, v in val.items():
                if k.startswith("_"): continue
                total_fields += 1
                if v not in (None, "", [], "unspecified", False):
                    filled_fields += 1
                else:
                    null_count += 1
                    
    return {
        "entries_found": entries_found,
        "filled_fields": filled_fields,
        "total_fields": total_fields,
        "density": f"{(filled_fields / max(1, total_fields)) * 100:.1f}%"
    }

def main():
    print("=======================================")
    print(" RIJĀL MODEL BENCHMARK (~10 entries)")
    print("=======================================\n")
    
    # Back up existing database so we don't destroy real data
    if DB_FILE.exists():
        backup_file = DB_FILE.with_name("rijal_database.json.bak")
        DB_FILE.replace(backup_file)
        
    results = []

    for model in MODELS_TO_TEST:
        print(f"► Testing {model} ...")
        
        # Clear out test database before each run
        if DB_FILE.exists():
            DB_FILE.unlink()
            
        cmd = [
            sys.executable, str(BUILDER_SCRIPT),
            "--ollama",
            "--model", model,
            "--start", "0",
            "--count", str(TEST_COUNT)
        ]
        
        start_time = time.time()
        # Suppress output unless it fails (forcing UTF-8 for Windows console)
        env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", env=env)
        elapsed = time.time() - start_time
        
        if proc.returncode != 0:
            print(f"  [ERROR] Script failed for {model}. Check logs.")
            print(proc.stderr[:500])
            continue
            
        # Analyze database
        if DB_FILE.exists():
            with open(DB_FILE, "r", encoding="utf-8") as f:
                db = json.load(f)
        else:
            db = {}
            
        # Parse error count from standard error output
        stderr = proc.stderr
        failed_batches = stderr.count("FAILED: All")
        retries = stderr.count("Attempt")
        
        eval_data = evaluate_extraction(db, start_idx=0, count=TEST_COUNT)
        entries_found = eval_data['entries_found']
        speed = elapsed / max(1, entries_found) if entries_found > 0 else 0
        
        print(f"  ✓ Time:        {elapsed:.1f}s ({speed:.1f} sec/entry)")
        print(f"  ✓ Valid items: {entries_found}/{TEST_COUNT} entries extracted successfully")
        print(f"  ✓ Parsing errs:{retries} schema recovery retries needed")
        print(f"  ✓ Extraction:  {eval_data['density']} of fields contained rich data")
        print()
        
        results.append({
            "Model": model,
            "Total Time (s)": round(elapsed, 1),
            "Sec/Entry": round(speed, 1),
            "Extracted": f"{entries_found}/{TEST_COUNT}",
            "JSON Fails": retries,
            "Data Density": eval_data['density']
        })

    # Restore backup
    backup_file = DB_FILE.with_name("rijal_database.json.bak")
    if backup_file.exists():
        if DB_FILE.exists():
            DB_FILE.unlink()
        backup_file.replace(DB_FILE)
        
    # Print summary
    print("\n=======================================")
    print(" BENCHMARK SUMMARY")
    print("=======================================")
    print(f"{'Model':<16} | {'Sec/Entry':<10} | {'JSON Errors':<11} | {'Rich Data Density'}")
    print("-" * 65)
    for r in results:
        m = r['Model']
        s = r['Sec/Entry']
        e = r['JSON Fails']
        d = r['Data Density']
        print(f"{m:<16} | {s:<10.1f} | {e:<11} | {d}")
        
    print("\n(Note: High JSON Errors indicate the model struggles to output clean arrays.)")
    print("(Note: Rich Data Density measures how much actual data vs nulls it found.)")

if __name__ == "__main__":
    main()

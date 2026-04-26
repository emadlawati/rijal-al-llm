import json
import re
import os
import asyncio
import aiohttp
from pathlib import Path
from collections import defaultdict
from itertools import islice

# Configurations
ALF_DB_PATH = Path("alf_rajul_database.json")
MUJAM_DB_PATH = Path("rijal_database_merged.json")
OUTPUT_PATH = Path("alf_rajul_disambiguation_llm.json")

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"

CONCURRENCY_LIMIT = 20
TOP_K_CANDIDATES = 25  # We present 25 highly probable candidates to ensure no one is missed


def normalize_arabic(text: str) -> str:
    """Removes diacritics, normalizes alefs, ya/alef maksura, and ta-marbuta."""
    if not text:
        return ""
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text) # diacritics
    text = re.sub(r'[إأآا]', 'ا', text)
    text = re.sub(r'ة', 'ه', text)
    text = re.sub(r'ى', 'ي', text)
    # Remove common connective words to strengthen core matching
    text = re.sub(r'\b(بن|ابن|أبو|ابا|ابي|ال)\b', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def tokenize(text: str) -> set[str]:
    return set(normalize_arabic(text).split())

def build_search_index(mujam_db: dict) -> list:
    """Pre-builds token sets for rapid fuzzy searching."""
    index = []
    for k, v in mujam_db.items():
        names = [v.get('name_ar', '')] + v.get('aliases', []) + v.get('other_names', [])
        tokens = set()
        for n in names:
            tokens.update(tokenize(n))
        index.append({
            'key': k,
            'name': v.get('name_ar', ''),
            'names_list': [n for n in names if n],
            'normalized_names': [normalize_arabic(n) for n in names if n],
            'tokens': tokens,
            'metadata': {
                'tabaqah': v.get('tabaqah'),
                'books': v.get('books', [])
            }
        })
    return index

def get_top_candidates(alf_entry: dict, index: list, top_k: int = TOP_K_CANDIDATES) -> list:
    """Scores Mujam entries against the Alf Rajul entry to locate the best subset."""
    alf_names = [alf_entry.get('name_ar', '')] + alf_entry.get('alt_names', [])
    alf_tokens = set()
    for n in alf_names:
        alf_tokens.update(tokenize(n))
    
    scored = []
    for doc in index:
        # Score is intersection of unique name words
        score = len(alf_tokens.intersection(doc['tokens']))
        
        # Exact string match boost (if the raw sequence is literally inside the Mujam names)
        for an in alf_names:
            cn_an = normalize_arabic(an)
            if any(cn_an in norm_n for norm_n in doc['normalized_names']):
                score += 5
                
        if score > 0:
            scored.append((score, doc))
            
    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)
    return [doc for score, doc in scored[:top_k]]

def build_prompt(alf_entry: dict, candidates: list) -> list:
    """Builds the DeepSeek chat prompt for disambiguation."""
    sys_prompt = (
        "You are an expert in Islamic Rijal (Hadith Narrator) Databases. "
        "Your task is to perfectly identify an Alf Rajul narrator from a provided list of Mu'jam candidates.\n"
        "Output ONLY raw JSON format: {\"match_key\": \"candidate_key\"} or {\"match_key\": null} if no one matches. Do NOT wrap in markdown."
    )
    
    alf_bio = alf_entry.get('_raw', '').strip()
    user_prompt = f"Target Alf Rajul Narrator:\nName: {alf_entry.get('name_ar')}\nAlt Names: {alf_entry.get('alt_names', [])}\nBiography:\n{alf_bio}\n\n"
    user_prompt += "Candidates from Mu'jam DB:\n"
    for i, c in enumerate(candidates):
        user_prompt += f"Candidate key: '{c['key']}' | Name: {c['name']} | Aliases: {c['names_list']} | Tabaqah: {c['metadata']['tabaqah']} | Books: {c['metadata']['books']}\n"
        
    user_prompt += "\nWhich candidate key represents exactly the same person? Output JSON."
    
    return [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_prompt}
    ]

async def process_entry(session, alf_key, alf_entry, candidates, semaphore, retries=3):
    """Hits the DeepSeek API to find the Match."""
    if not candidates:
        return alf_key, None

    prompt = build_prompt(alf_entry, candidates)
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "deepseek-chat",
        "messages": prompt,
        "temperature": 0.0,
        "response_format": {"type": "json_object"}
    }
    
    # Simple strict exact-match fallback if there's only 1 flawless overlap and no API (for safety)
    # But we will reliably use the API here.
    
    async with semaphore:
        for attempt in range(retries):
            try:
                async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=45) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    content = data['choices'][0]['message']['content'].strip()
                    
                    try:
                        result = json.loads(content)
                        return alf_key, result.get("match_key")
                    except json.JSONDecodeError:
                        print(f"Failed to parse JSON for {alf_key}: {content}")
                        return alf_key, None
            except Exception as e:
                print(f"Error on {alf_key} (att {attempt+1}/{retries}): {e}")
                await asyncio.sleep(2)
        return alf_key, None

async def main():
    if not DEEPSEEK_API_KEY:
        print("Set DEEPSEEK_API_KEY environment variable.")
        return
        
    print("Loading databases...")
    alf_db = json.loads(ALF_DB_PATH.read_text(encoding='utf-8'))
    mujam_db = json.loads(MUJAM_DB_PATH.read_text(encoding='utf-8'))
    
    print("Building local fuzzy match index for Mu'jam Database...")
    mujam_idx = build_search_index(mujam_db)
    
    results = {}
    if OUTPUT_PATH.exists():
        results = json.loads(OUTPUT_PATH.read_text(encoding='utf-8'))
        print(f"Loaded {len(results)} existing mappings from cache.")
        
    tasks = []
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession() as session:
        for alf_key, alf_entry in alf_db.items():
            if alf_key in results:
                continue # Skip already processed in case of resume
                
            candidates = get_top_candidates(alf_entry, mujam_idx)
            tasks.append(process_entry(session, alf_key, alf_entry, candidates, semaphore))
            
        print(f"Queueing {len(tasks)} LLM matching tasks...")
        
        # Process dynamically and save periodically
        completed = 0
        for i, future in enumerate(asyncio.as_completed(tasks)):
            key, match_key = await future
            results[key] = match_key
            completed += 1
            if completed % 50 == 0:
                print(f"Progress: {completed} / {len(tasks)}")
                OUTPUT_PATH.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')

    OUTPUT_PATH.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    
    matched = sum(1 for v in results.values() if v is not None)
    print(f"\nDone! Successfully matched {matched}/{len(results)} narrators.")

if __name__ == '__main__':
    # Fix for Windows asyncio loop
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

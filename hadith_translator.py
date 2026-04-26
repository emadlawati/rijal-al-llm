#!/usr/bin/env python3
"""
Hadith Translation Engine
=========================
Uses 31,000+ real Arabic-English hadith translations from Thaqalayn as the
reference corpus. When translating new Arabic text, retrieves the most similar
existing translations and uses them as style/terminology examples.

The corpus IS the glossary. No hand-curated terms. The translators' actual
choices across 31,000 pairs define the translation style.

Requirements:
    pip install anthropic requests

Setup (one-time):
    python hadith_translator.py harvest    # ~10 min, fetches corpus
    python hadith_translator.py index      # ~1 min, builds search index

Usage:
    # Offline lookup — show similar existing translations (no API needed)
    python hadith_translator.py lookup
    python hadith_translator.py lookup --top-k 20

    # AI translation — uses corpus examples + Claude (needs API key)
    python hadith_translator.py translate
    python hadith_translator.py translate --file chapter.txt --output chapter_en.txt

    # Prefer a specific translator's style
    python hadith_translator.py lookup --prefer "Muhammad Sarwar"
    python hadith_translator.py translate --prefer "Muhammad Sarwar"

    # Show corpus statistics
    python hadith_translator.py stats

Environment:
    ANTHROPIC_API_KEY=sk-ant-...  (only needed for 'translate' command)
"""

import json
import os
import sys
import re
import time
import argparse
import logging
from pathlib import Path
from collections import Counter, defaultdict

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)

# ─── Paths ───
DIR = Path(__file__).parent
DATA = DIR / "translation_data"
CORPUS_FILE = DATA / "corpus.json"
INDEX_FILE = DATA / "index.json"
WEIGHTS_FILE = DATA / "translator_weights.json"

API_BASE = "https://www.thaqalayn-api.net/api/v2"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("ht")


# ═══════════════════════════════════════════════════
# TEXT PROCESSING
# ═══════════════════════════════════════════════════

def strip_tashkeel(text):
    """Remove Arabic diacritics."""
    return re.sub(r'[\u064B-\u065F\u0670]', '', text)


def normalize_ar(text):
    """Normalize Arabic for matching."""
    text = strip_tashkeel(text)
    text = text.replace('إ', 'ا').replace('أ', 'ا').replace('آ', 'ا').replace('ٱ', 'ا')
    text = text.replace('ة', 'ه').replace('ى', 'ي').replace('ـ', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_matn(pair):
    """Get the matn (content) text, preferring thaqalaynMatn over full arabicText.
    This strips the sanad so matching focuses on actual content."""
    matn = pair.get("matn_ar", "").strip()
    if matn and len(matn) > 20:
        return matn
    # Fallback: try to strip common sanad patterns from full text
    full = pair.get("ar", "").strip()
    # Crude sanad stripping: find the last occurrence of common transition phrases
    # that mark where the Imam starts speaking
    for marker in ['قال :', 'قال:', 'يقول :', 'يقول:', 'فقال :', 'فقال:',
                    'أنه قال', 'أنّه قال', 'عليه السلام قال', '(ع) قال',
                    '(ع) :', '(ص) :']:
        idx = full.rfind(marker)
        if idx > 0 and idx < len(full) * 0.6:  # marker in first 60% of text
            return full[idx:]
    return full


def split_sentences(text):
    """Split text into sentences (works for both Arabic and English)."""
    # Split on period, question mark, exclamation, or Arabic period
    parts = re.split(r'[.!?؟。]\s*', text)
    return [p.strip() for p in parts if len(p.strip()) > 15]


def make_ngrams(text, n=3):
    """Character n-grams for similarity."""
    text = normalize_ar(text) if any('\u0600' <= c <= '\u06FF' for c in text) else text.lower()
    text = re.sub(r'[^\w\u0600-\u06FF]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) < n:
        return set()
    return {text[i:i+n] for i in range(len(text) - n + 1)}


def similarity(a_ngrams, b_ngrams):
    """Jaccard similarity."""
    if not a_ngrams or not b_ngrams:
        return 0.0
    inter = len(a_ngrams & b_ngrams)
    return inter / (len(a_ngrams) + len(b_ngrams) - inter)


# ═══════════════════════════════════════════════════
# HARVEST — Fetch corpus from Thaqalayn API
# ═══════════════════════════════════════════════════

def cmd_harvest():
    DATA.mkdir(exist_ok=True)

    log.info("Fetching book list...")
    books = requests.get(f"{API_BASE}/allbooks", timeout=30).json()
    log.info(f"{len(books)} books found")

    corpus = []
    t_stats = Counter()

    for i, book in enumerate(books):
        bid = book["bookId"]
        bname = book.get("BookName", bid)
        translator = book.get("translator", "")
        vol = book.get("volume", 1)

        log.info(f"[{i+1}/{len(books)}] {bname} V{vol} ...")
        try:
            arr = requests.get(f"{API_BASE}/{bid}", timeout=120).json()
            if not isinstance(arr, list):
                continue

            n = 0
            for h in arr:
                ar = (h.get("arabicText") or "").strip()
                en = (h.get("englishText") or "").strip()
                if len(ar) < 15 or len(en) < 15:
                    continue

                corpus.append({
                    "ar": ar,
                    "en": en,
                    "matn_ar": (h.get("thaqalaynMatn") or "").strip(),
                    "sanad_ar": (h.get("thaqalaynSanad") or "").strip(),
                    "book": bname,
                    "book_id": bid,
                    "volume": vol,
                    "translator": translator,
                    "chapter": h.get("chapter", ""),
                    "category": h.get("category", ""),
                    "hadith_id": h.get("id"),
                    "url": h.get("URL", ""),
                })
                n += 1

            t_stats[translator] += n
            log.info(f"  → {n} pairs")
        except Exception as e:
            log.error(f"  FAILED: {e}")
        time.sleep(0.3)

    with open(CORPUS_FILE, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False)

    # Default weights
    weights = {t: 1.0 for t in t_stats if t}
    with open(WEIGHTS_FILE, "w", encoding="utf-8") as f:
        json.dump(weights, f, ensure_ascii=False, indent=2)

    log.info(f"\nHarvested {len(corpus)} pairs → {CORPUS_FILE.name}")
    log.info("Translator distribution:")
    for t, c in t_stats.most_common():
        log.info(f"  {t or '(unknown)'}: {c}")
    log.info(f"\nEdit {WEIGHTS_FILE.name} to boost preferred translators (higher = preferred)")


# ═══════════════════════════════════════════════════
# INDEX — Build search index
# ═══════════════════════════════════════════════════

def cmd_index():
    corpus = _load_corpus()
    log.info(f"Building index for {len(corpus)} pairs...")

    # Build TWO indices:
    # 1. Hadith-level (matn-focused) for finding similar hadiths
    # 2. Sentence-level for finding similar sentences/phrases
    
    hadith_index = []
    sentence_index = []

    for i, pair in enumerate(corpus):
        if i % 5000 == 0 and i > 0:
            log.info(f"  {i}/{len(corpus)}...")

        # Hadith-level: use matn (content without sanad)
        matn = extract_matn(pair)
        h_ngrams = list(make_ngrams(matn))
        hadith_index.append({
            "i": i,
            "ng": h_ngrams[:300],
        })

        # Sentence-level: split English into sentences, store with index back to pair
        en_sentences = split_sentences(pair["en"])
        ar_sentences = split_sentences(pair["ar"])

        for sent in en_sentences:
            if len(sent) > 20:
                sentence_index.append({
                    "i": i,
                    "en": sent,
                    "ng": list(make_ngrams(sent))[:150],
                })

        for sent in ar_sentences:
            if len(sent) > 15:
                sentence_index.append({
                    "i": i,
                    "ar": sent,
                    "ng": list(make_ngrams(sent))[:150],
                })

    index = {
        "hadith": hadith_index,
        "sentence": sentence_index,
    }

    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)

    log.info(f"Index saved: {len(hadith_index)} hadiths, {len(sentence_index)} sentences")


# ═══════════════════════════════════════════════════
# SEARCH — Find similar translations
# ═══════════════════════════════════════════════════

def search_similar(query_text, corpus, index_data, top_k=20, weights=None, level="hadith"):
    """Find most similar entries in the corpus.
    
    level="hadith" — match full hadiths (good for translating a complete hadith)
    level="sentence" — match individual sentences (good for specific phrases)
    """
    q_ngrams = make_ngrams(query_text)
    if not q_ngrams:
        return []

    entries = index_data.get(level, index_data.get("hadith", []))
    scores = []

    for entry in entries:
        e_ngrams = set(entry["ng"])
        sim = similarity(q_ngrams, e_ngrams)
        if sim > 0.03:
            idx = entry["i"]
            # Apply translator weight
            w = 1.0
            if weights and idx < len(corpus):
                t = corpus[idx].get("translator", "")
                w = weights.get(t, 1.0)
            scores.append((idx, sim * w, entry))

    scores.sort(key=lambda x: -x[1])

    # Deduplicate by corpus index (a hadith might match multiple sentences)
    seen = set()
    deduped = []
    for idx, score, entry in scores:
        if idx not in seen:
            seen.add(idx)
            deduped.append((idx, score, entry))
        if len(deduped) >= top_k:
            break

    return deduped


# ═══════════════════════════════════════════════════
# LOOKUP — Offline reference (no API)
# ═══════════════════════════════════════════════════

def cmd_lookup(args):
    corpus = _load_corpus()
    index_data = _load_index()
    weights = _load_weights(args.prefer)

    top_k = args.top_k
    log.info(f"Corpus: {len(corpus)} pairs. Showing top {top_k} matches.\n")

    print("=" * 65)
    print("  TRANSLATION MEMORY LOOKUP")
    print("  Paste Arabic text → see how similar text was translated")
    print("  Uses the actual 31,000 hadith translations as reference")
    print("  Type 'quit' to exit")
    print("=" * 65)

    while True:
        try:
            print("\nArabic text (paste, then Enter twice; or 'quit'):")
            lines = []
            while True:
                line = input()
                if line.strip().lower() == "quit":
                    return
                if line.strip() == "" and lines:
                    break
                lines.append(line)

            arabic = "\n".join(lines).strip()
            if not arabic:
                continue

            # Search at hadith level
            results = search_similar(arabic, corpus, index_data, top_k=top_k, weights=weights, level="hadith")

            # Also search at sentence level for more granular matches
            sent_results = search_similar(arabic, corpus, index_data, top_k=5, weights=weights, level="sentence")

            if not results and not sent_results:
                print("\nNo similar passages found.\n")
                continue

            print(f"\n{'═' * 65}")
            print(f"  {len(results)} MATCHING TRANSLATIONS")
            print(f"{'═' * 65}")

            for rank, (idx, sim, _) in enumerate(results, 1):
                p = corpus[idx]
                matn = extract_matn(p) or p["ar"]

                if sim >= 0.4:
                    icon = "★★"
                elif sim >= 0.2:
                    icon = "★ "
                else:
                    icon = "· "

                print(f"\n{icon} Match {rank}  [{sim:.0%} similar]")
                print(f"   Source: {p['book']}{' V'+str(p['volume']) if p.get('volume') else ''} | {p.get('translator','?')}")
                if p.get("chapter"):
                    print(f"   Chapter: {p.get('category','')}{' → ' if p.get('category') and p.get('chapter') else ''}{p['chapter']}")

                print(f"\n   ┌─ Arabic (matn):")
                for line in _wrap(matn, 90):
                    print(f"   │  {line}")

                print(f"   ├─ English translation:")
                for line in _wrap(p["en"], 90):
                    print(f"   │  {line}")

                if p.get("url"):
                    print(f"   └─ {p['url']}")
                else:
                    print(f"   └─ #{p.get('hadith_id','?')}")

            # Show sentence-level matches that aren't already shown
            shown_idxs = {idx for idx, _, _ in results}
            extra_sent = [(idx, sim, e) for idx, sim, e in sent_results if idx not in shown_idxs]
            if extra_sent:
                print(f"\n{'─' * 65}")
                print(f"  + {len(extra_sent)} additional sentence-level matches:")
                for idx, sim, entry in extra_sent[:5]:
                    p = corpus[idx]
                    snippet_en = entry.get("en", p["en"][:150])
                    print(f"    [{sim:.0%}] {p['book']} — {snippet_en[:120]}...")

            print()

        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            return


# ═══════════════════════════════════════════════════
# TRANSLATE — AI-assisted using corpus as reference
# ═══════════════════════════════════════════════════

def cmd_translate(args):
    try:
        import anthropic
    except ImportError:
        print("ERROR: pip install anthropic"); sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: export ANTHROPIC_API_KEY='sk-ant-...'"); sys.exit(1)

    corpus = _load_corpus()
    index_data = _load_index()
    weights = _load_weights(args.prefer)
    client = anthropic.Anthropic(api_key=api_key)

    if args.file:
        _translate_file(client, corpus, index_data, weights, args)
    else:
        _translate_interactive(client, corpus, index_data, weights, args)


def _build_translation_prompt(arabic, corpus, index_data, weights, n_examples=20):
    """Build the prompt with retrieved examples from the actual corpus."""
    results = search_similar(arabic, corpus, index_data, top_k=n_examples, weights=weights, level="hadith")

    examples = []
    for idx, sim, _ in results:
        p = corpus[idx]
        matn = extract_matn(p) or p["ar"]
        examples.append(
            f"[{sim:.0%} match | {p['book']} | tr: {p.get('translator','')}]\n"
            f"Arabic: {matn[:600]}\n"
            f"English: {p['en'][:600]}"
        )

    examples_block = "\n\n---\n\n".join(examples) if examples else "(no similar translations found)"

    system = f"""You are translating classical Shia Islamic hadith text from Arabic to English.

Below are {len(results)} existing translations from the Thaqalayn hadith corpus that are similar to the text you need to translate. These are REAL translations by established hadith translators. Your job is to match their style, terminology, and conventions as closely as possible.

REFERENCE TRANSLATIONS FROM CORPUS:

{examples_block}

RULES:
- Match the translation style, terminology, and register of the references above
- These references show you how the translators handle honorifics, technical terms, chain of narration phrasing, and Islamic vocabulary — follow their conventions
- Translate faithfully — do not add or omit content
- Do not add footnotes or commentary unless asked
- Output ONLY the English translation, nothing else"""

    return system, results


def _translate_interactive(client, corpus, index_data, weights, args):
    log.info(f"Corpus: {len(corpus)} reference translations loaded.\n")

    print("=" * 65)
    print("  AI-ASSISTED HADITH TRANSLATION")
    print("  Translates using 31,000 real hadith translations as reference")
    print("  Type 'quit' to exit")
    print("=" * 65)

    while True:
        try:
            print("\nArabic text (paste, then Enter twice; or 'quit'):")
            lines = []
            while True:
                line = input()
                if line.strip().lower() == "quit":
                    return
                if line.strip() == "" and lines:
                    break
                lines.append(line)

            arabic = "\n".join(lines).strip()
            if not arabic:
                continue

            print("\nFinding reference translations & translating...\n")
            system, refs = _build_translation_prompt(arabic, corpus, index_data, weights)

            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                system=system,
                messages=[{"role": "user", "content": f"Translate:\n\n{arabic}"}],
            )
            translation = "".join(b.text for b in msg.content if hasattr(b, "text"))

            print("─" * 65)
            print("TRANSLATION:")
            print("─" * 65)
            print(translation)
            print("─" * 65)

            if refs:
                print(f"\nBased on {len(refs)} reference translations:")
                for idx, sim, _ in refs[:5]:
                    p = corpus[idx]
                    print(f"  • {sim:.0%} — {p['book']} ({p.get('translator','')})")

            print()

        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            return


def _translate_file(client, corpus, index_data, weights, args):
    """Translate a file paragraph by paragraph."""
    output_path = args.output or (args.file.rsplit(".", 1)[0] + "_translated.txt")

    with open(args.file, "r", encoding="utf-8") as f:
        text = f.read()

    # Split into paragraphs
    paragraphs = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip() and len(p.strip()) > 10]
    log.info(f"{len(paragraphs)} paragraphs to translate")

    results = []
    for i, para in enumerate(paragraphs):
        log.info(f"Translating {i+1}/{len(paragraphs)}...")

        system, refs = _build_translation_prompt(para, corpus, index_data, weights, n_examples=15)

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=system,
            messages=[{"role": "user", "content": f"Translate:\n\n{para}"}],
        )
        translation = "".join(b.text for b in msg.content if hasattr(b, "text"))
        results.append((para, translation, refs))
        time.sleep(1.0)

    with open(output_path, "w", encoding="utf-8") as f:
        for i, (para, trans, refs) in enumerate(results):
            f.write(f"{'='*60}\n")
            f.write(f"Paragraph {i+1}\n")
            f.write(f"{'='*60}\n\n")
            f.write(f"[Arabic]\n{para}\n\n")
            f.write(f"[English]\n{trans}\n\n")
            if refs:
                f.write(f"[References: {', '.join(f'{corpus[idx][\"book\"]} ({sim:.0%})' for idx,sim,_ in refs[:3])}]\n")
            f.write("\n")

    log.info(f"Saved to {output_path}")


# ═══════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════

def cmd_stats():
    corpus = _load_corpus()

    t_counts = Counter(p.get("translator", "") for p in corpus)
    b_counts = Counter(p.get("book", "") for p in corpus)
    ar_lens = [len(p["ar"]) for p in corpus]
    en_lens = [len(p["en"]) for p in corpus]

    print(f"\n{'='*60}")
    print(f"  CORPUS STATISTICS")
    print(f"{'='*60}")
    print(f"  Total pairs: {len(corpus):,}")
    print(f"  Arabic text: avg {sum(ar_lens)//len(ar_lens)} chars, total {sum(ar_lens):,} chars")
    print(f"  English text: avg {sum(en_lens)//len(en_lens)} chars, total {sum(en_lens):,} chars")

    print(f"\n  BY TRANSLATOR:")
    for t, c in t_counts.most_common():
        print(f"    {t or '(unknown)':40s} {c:6,} pairs")

    print(f"\n  BY BOOK:")
    for b, c in b_counts.most_common():
        print(f"    {b:40s} {c:6,} pairs")

    # Check how many have separate matn
    with_matn = sum(1 for p in corpus if p.get("matn_ar", "").strip())
    print(f"\n  With separate matn field: {with_matn:,} ({with_matn/len(corpus)*100:.1f}%)")
    print(f"{'='*60}\n")


# ═══════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════

def _load_corpus():
    if not CORPUS_FILE.exists():
        print(f"ERROR: {CORPUS_FILE} not found. Run 'harvest' first."); sys.exit(1)
    with open(CORPUS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_index():
    if not INDEX_FILE.exists():
        print(f"ERROR: {INDEX_FILE} not found. Run 'index' first."); sys.exit(1)
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_weights(prefer=None):
    weights = {}
    if WEIGHTS_FILE.exists():
        with open(WEIGHTS_FILE, "r", encoding="utf-8") as f:
            weights = json.load(f)
    if prefer:
        for t in weights:
            if prefer.lower() in t.lower():
                weights[t] = 3.0
                log.info(f"Boosted: {t} (weight 3.0)")
    return weights


def _wrap(text, width=90):
    """Simple word-wrap."""
    words = text.split()
    lines, current = [], ""
    for w in words:
        if len(current) + len(w) + 1 > width:
            lines.append(current)
            current = w
        else:
            current = f"{current} {w}".strip()
    if current:
        lines.append(current)
    return lines or [""]


# ═══════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Hadith Translation Engine — powered by 31,000 real translations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  harvest    Fetch corpus from Thaqalayn API (one-time, ~10 min)
  index      Build search index (one-time, ~1 min)
  stats      Show corpus statistics
  lookup     Find similar existing translations (offline, no API)
  translate  AI-assisted translation using corpus as reference (needs API key)

Examples:
  python hadith_translator.py harvest
  python hadith_translator.py index
  python hadith_translator.py lookup --top-k 15
  python hadith_translator.py translate --prefer "Muhammad Sarwar"
  python hadith_translator.py translate --file chapter.txt
        """
    )
    p.add_argument("command", choices=["harvest", "index", "stats", "lookup", "translate"])
    p.add_argument("--prefer", type=str, default=None, help="Boost this translator's style (partial name match)")
    p.add_argument("--top-k", type=int, default=15, help="Number of matches to show in lookup (default: 15)")
    p.add_argument("--file", type=str, default=None, help="Input file for batch translation")
    p.add_argument("--output", type=str, default=None, help="Output file (default: input_translated.txt)")
    args = p.parse_args()

    DATA.mkdir(exist_ok=True)

    if args.command == "harvest":
        cmd_harvest()
    elif args.command == "index":
        cmd_index()
    elif args.command == "stats":
        cmd_stats()
    elif args.command == "lookup":
        cmd_lookup(args)
    elif args.command == "translate":
        cmd_translate(args)


if __name__ == "__main__":
    main()

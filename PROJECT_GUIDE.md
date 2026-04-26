# Thaqalayn Hadith Toolkit — Agent Guide

## Overview

This project is a suite of tools for Shia Islamic hadith scholarship, built around the [Thaqalayn API](https://www.thaqalayn-api.net/api/v2) which serves 31,000+ ahadith from 33 classical Shia books (Al-Kafi, Al-Tawhid, Man La Yahduruh al-Faqih, Nahj al-Balagha, etc.) with Arabic text, English translations, gradings, and metadata.

There are three main tools:

1. **Rijāl Database Builder** — Extracts structured narrator data from a 1,161-page PDF of *al-Mufīd min Muʿjam Rijāl al-Ḥadīth* (15,545 entries) using AI
2. **Hadith Topic Tagger** — Classifies all ahadith into 12 Islamic categories with sub-tags using AI
3. **Hadith Translation Engine** — Builds a translation memory from 31,000 parallel Arabic-English pairs and translates new Arabic hadith text matching existing translator style

All three tools support **Ollama (local/free)** and **Claude API (cloud/paid)**.

---

## Prerequisites

### Python packages

```bash
pip install requests anthropic
```

### Ollama (for local/free processing)

```bash
# Install Ollama: https://ollama.com/download
# Then pull models:

ollama pull qwen3:8b      # 5GB — good for tagger, lighter tasks
ollama pull qwen3:32b     # 20GB — better for rijal extraction & translation
                          # needs 32GB+ RAM

# Verify it's running:
ollama list
```

### Claude API (for cloud processing)

```bash
# Get API key from https://console.anthropic.com
# Then set it:
export ANTHROPIC_API_KEY="sk-ant-api03-..."
```

### File structure

All scripts and data files should be in the same directory:

```
project/
├── rijal_builder.py           # Rijal extraction pipeline
├── rijal_entries.json          # Pre-extracted 15,545 narrator entries from PDF
├── hadith_tagger.py            # Topic classification tool
├── hadith_translator.py        # Translation memory + AI translator
├── prepare_project_refs.py     # Extracts reference docs for Claude Project
├── claude_project_instructions.txt  # System prompt for Claude Project
│
├── rijal_database.json         # (generated) structured narrator database
├── rijal_progress.json         # (generated) processing progress tracker
├── rijal_errors.log            # (generated) error log
│
├── topic_tags/                 # (generated) tagged hadiths per book
│   ├── Al-Kafi-Volume-1-Kulayni.json
│   ├── Al-Tawhid-Saduq.json
│   ├── _merged.json            # all tags combined
│   └── ...
│
├── translation_data/           # (generated) translation corpus
│   ├── corpus.json             # 31,000+ Arabic-English pairs
│   ├── index.json              # trigram search index
│   └── translator_weights.json # translator preference weights
│
└── reference_docs/             # (generated) for Claude Project upload
    ├── ref_Al-Kafi_V1.txt
    └── ...
```

---

## Tool 1: Rijāl Database Builder

### What it does

Processes 15,545 narrator entries from *al-Mufīd min Muʿjam Rijāl al-Ḥadīth* by Muḥammad al-Jawāhirī (a summary of Sayyid al-Khoei's rijāl assessments). For each narrator, it extracts:

- Full Arabic name + English transliteration
- Father, grandfather, full nasab (lineage chain)
- Kunyah (teknonym like أبو الحسين), laqab (titles/nicknames), nisba (origin like الكوفي, القمي)
- Status ruling: thiqah (trustworthy), majhul (unknown), daif (weak), mamduh (praised), muwaththaq (reliable), hasan (good)
- Who gave the ruling (النجاشي, الشيخ, etc.)
- Which Imams they narrate from
- Which hadith books they appear in (الكافي, التهذيب, الفقيه, الاستبصار)
- Number of narrations
- Aliases and cross-references (متحد مع = identical to another entry)
- Whether they authored a book/asl
- Chain (tariq) status to them

### Input file

`rijal_entries.json` — pre-extracted from the PDF. Contains 15,545 entries, each with:

```json
{
  "idx": 0,
  "n1": "١",       // Najaf edition number
  "n2": "١",       // Beirut edition number
  "n3": "١",       // Tehran edition number (used as primary key)
  "text": "آدم أبو الحسين اللؤلؤي :روى رواية عن أبي عبد الله (ع) ..."
}
```

### Commands

```bash
# ─── With Ollama (recommended: qwen3:32b for Arabic accuracy) ───

# Test with 50 entries first
python rijal_builder.py --ollama --model qwen3:32b --count 50

# Process all (auto-resumes if stopped)
python rijal_builder.py --ollama --model qwen3:32b

# Process a specific range
python rijal_builder.py --ollama --model qwen3:32b --start 500 --count 200

# Adjust batch size (fewer = more accurate, slower)
python rijal_builder.py --ollama --model qwen3:32b --batch-size 5

# ─── With Claude API ───

python rijal_builder.py --count 50          # test run
python rijal_builder.py                     # process all

# ─── Check progress ───

python rijal_builder.py --stats
```

### Resume behavior

Progress is saved to `rijal_progress.json` after every batch. If the script is stopped (Ctrl+C, crash, machine sleep), running it again automatically resumes from the last completed batch. To start over, delete `rijal_progress.json` and `rijal_database.json`, or use the `--start 0` flag.

### Output

`rijal_database.json` — keyed by Tehran edition number. Each entry:

```json
{
  "name_ar": "آدم بن إسحاق بن آدم",
  "name_en": "Adam ibn Ishaq ibn Adam",
  "father": "إسحاق",
  "grandfather": "آدم",
  "nasab": "ibn Ishaq ibn Adam ibn Abdullah ibn Sa'd al-Ash'ari",
  "kunyah": null,
  "laqab": null,
  "nisba": "القمي",
  "status": "thiqah",
  "status_detail": "ثقة",
  "status_source": null,
  "narrates_from_imams": [],
  "companions_of": null,
  "books": [],
  "hadith_count": null,
  "aliases": ["آدم بن إسحاق"],
  "alias_entry_nums": ["٣"],
  "has_book": true,
  "tariq_status": "daif",
  "notes": "Path from both al-Shaykh and al-Saduq to him is weak",
  "_entry_idx": 3,
  "_num_najaf": "٤",
  "_num_beirut": "٤",
  "_num_tehran": "٤",
  "_raw": "٤آدم بن إسحاق بن آدم :بن عبد الله بن سعد الأشعري قمي -ثقة -له كتاب ..."
}
```

### Estimated processing time

- Ollama qwen3:32b: ~10-15 seconds per batch of 8, ~5-6 hours for all 15,545 entries
- Ollama qwen3:8b: ~5 seconds per batch, ~3 hours, but lower accuracy on complex entries
- Claude Sonnet: ~2 seconds per batch, ~1 hour, highest accuracy, costs ~$15-25 total

### Quality notes

- The 8b model may struggle with entries that have complex cross-references (متحد مع chains across multiple aliases) or ambiguous status rulings
- For best results, process with 32b or Claude, then spot-check the first 100 entries
- Entries that fail JSON parsing are logged to `rijal_errors.log` — re-run those ranges later

---

## Tool 2: Hadith Topic Tagger

### What it does

Fetches ahadith from the Thaqalayn API and classifies each into one of 12 categories with 2-3 specific sub-tags:

| Category | Description |
|----------|-------------|
| tawheed | Theology, God's attributes, divine unity |
| imamate | Leadership, successorship, Ahlul Bayt, authority |
| fiqh | Jurisprudence — salat, sawm, hajj, wudu, halal/haram |
| akhlaq | Ethics, character, vices and virtues |
| quran | Quranic interpretation, virtues of surahs |
| akhirah | Death, barzakh, qiyamah, heaven, hell |
| dua | Supplications, remembrance, istighfar |
| history | Prophets, events, companions, nations |
| social | Marriage, parenting, neighbors, rights |
| economics | Halal income, riba, charity, khums, zakat |
| knowledge | Seeking knowledge, scholars, intellect |
| health | Medicine, food, hygiene, diet |

### Commands

```bash
# ─── See available books ───

python hadith_tagger.py

# ─── Tag a specific book ───

# With Ollama (qwen3:8b is sufficient for classification)
python hadith_tagger.py --ollama --book Al-Tawhid-Saduq
python hadith_tagger.py --ollama --book Al-Kafi-Volume-1-Kulayni

# With Claude
python hadith_tagger.py --book Al-Tawhid-Saduq

# ─── Tag ALL books ───

python hadith_tagger.py --ollama --all

# ─── Check stats ───

python hadith_tagger.py --stats

# ─── Merge all tagged books into one file ───

python hadith_tagger.py --merge
```

### Resume behavior

Each book is saved as a separate JSON in `topic_tags/`. If a book has already been tagged, it is skipped. To re-tag a book, delete its JSON file from `topic_tags/`.

### Output

Per-book files in `topic_tags/`, e.g. `topic_tags/Al-Tawhid-Saduq.json`:

```json
{
  "book_id": "Al-Tawhid-Saduq",
  "book_name": "Al-Tawḥīd",
  "total_hadiths": 575,
  "tagged_count": 560,
  "errors": 1,
  "tags": {
    "42": {
      "cat": "tawheed",
      "sub": ["God's knowledge", "divine attributes"]
    },
    "43": {
      "cat": "tawheed",
      "sub": ["divine unity", "refutation of anthropomorphism"]
    }
  }
}
```

Merged file at `topic_tags/_merged.json` combines all books.

### Estimated processing time per book

- Al-Tawhid (575 hadiths): ~10 minutes with Ollama 8b, ~3 minutes with Claude
- Al-Kafi Volume 1 (1,449 hadiths): ~25 minutes with Ollama 8b, ~8 minutes with Claude
- All 33 books (~31,000 hadiths): ~8-10 hours with Ollama, ~2-3 hours with Claude

---

## Tool 3: Hadith Translation Engine

### What it does

Builds a translation memory from 31,000+ real Arabic-English hadith pairs harvested from the Thaqalayn API. When translating new Arabic text, it retrieves the most similar existing translations and uses them as style/terminology reference — either showing them for manual reference (lookup mode) or feeding them to an AI for style-consistent translation (translate mode).

There are no hand-curated glossaries. The 31,000 human translations ARE the entire reference. The AI sees how established translators (Muhammad Sarwar, Ali Peiravi, Bilal Muhammad, etc.) handled similar Arabic constructions and matches their style.

### Phase 1: Harvest corpus (one-time)

```bash
python hadith_translator.py harvest
```

Fetches all 33 books from the Thaqalayn API. Takes ~10 minutes. Creates `translation_data/corpus.json` (~30MB) and `translation_data/translator_weights.json`.

**Translator weights:** Edit `translator_weights.json` to boost preferred translators. Set a value higher than 1.0 to prefer that translator's style:

```json
{
  "Muhammad Sarwar": 3.0,
  "Dr. Ali Peiravi": 1.0,
  "Bilal Muhammad": 1.5
}
```

### Phase 2: Build index (one-time)

```bash
python hadith_translator.py index
```

Builds a trigram search index for fast similarity matching. Takes ~1 minute. Creates `translation_data/index.json`.

The index operates at two levels:
- **Hadith-level**: matches on matn (content) text, stripping the sanad (chain of narration) to focus on actual content similarity
- **Sentence-level**: matches individual sentences for fine-grained phrase lookup

### Phase 3: Use it

#### Lookup mode (offline, no AI, free)

```bash
python hadith_translator.py lookup
python hadith_translator.py lookup --top-k 20
python hadith_translator.py lookup --prefer "Muhammad Sarwar"
```

Paste Arabic text, get the most similar existing translations displayed with:
- Similarity score (★★ for 40%+, ★ for 20%+)
- Source book and translator name
- Full Arabic (matn) and English translation
- thaqalayn.net link
- Additional sentence-level matches

This is the primary tool for day-to-day translation work. You paste a passage, see how similar text was translated by professional translators, and use that as your reference.

#### Translate mode — Ollama (local, free)

```bash
python hadith_translator.py translate --ollama
python hadith_translator.py translate --ollama --model qwen3:32b
python hadith_translator.py translate --ollama --prefer "Muhammad Sarwar"
python hadith_translator.py translate --ollama --file chapter.txt
```

Retrieves 20 similar translations from the corpus, feeds them as style examples to the local model along with your Arabic text, and generates a full translation matching the corpus style.

#### Translate mode — Claude API (best quality)

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
python hadith_translator.py translate
python hadith_translator.py translate --prefer "Muhammad Sarwar"
python hadith_translator.py translate --file chapter.txt --output chapter_en.txt
```

Same retrieval, but uses Claude Sonnet for the translation. Higher quality, costs a few cents per passage.

#### Batch file translation

```bash
# With Ollama
python hadith_translator.py translate --ollama --file arabic_chapter.txt --output english_chapter.txt

# With Claude
python hadith_translator.py translate --file arabic_chapter.txt --output english_chapter.txt
```

Input file: Arabic text with paragraphs separated by blank lines. Each paragraph is translated separately with its own set of retrieved reference translations.

Output file: Each paragraph with original Arabic, English translation, and source references.

#### Corpus stats

```bash
python hadith_translator.py stats
```

Shows total pairs, average text length, per-translator breakdown, per-book breakdown.

### Claude Project alternative

For interactive translation sessions, you can also set up a Claude Project on claude.ai with reference translations uploaded as knowledge documents:

```bash
# After harvesting, extract reference docs
python prepare_project_refs.py --sample                    # ~3000 diverse pairs across all books
python prepare_project_refs.py --prefer "Muhammad Sarwar"  # prioritize his translations
python prepare_project_refs.py --books "Al-Kafi"           # only Kafi translations
```

Then:
1. Create a project on claude.ai → Projects → Create
2. Paste the contents of `claude_project_instructions.txt` as the project system prompt
3. Upload the `.txt` files from `reference_docs/` as project knowledge
4. Start translating — paste Arabic text in the project chat

This gives Claude the full reference material in context at all times (up to 200K tokens), which is better for interactive work where you want to ask follow-up questions about terminology choices.

---

## Thaqalayn API Reference

Base URL: `https://www.thaqalayn-api.net/api/v2`

| Endpoint | Description |
|----------|-------------|
| `/allbooks` | All books with metadata and hadith ID ranges |
| `/random` | Random hadith from any book |
| `/{bookId}/random` | Random hadith from a specific book |
| `/query?q={query}` | Full-text search across all books (Arabic and English) |
| `/query/{bookId}?q={query}` | Search within a specific book |
| `/{bookId}` | All hadiths for a book |
| `/{bookId}/{id}` | Specific hadith by numeric ID |

### Hadith JSON fields

```
id, bookId, book, category, categoryId, chapter, author, translator,
englishText, arabicText, majlisiGrading, URL, volume, frenchText,
mohseniGrading, behbudiGrading, chapterInCategoryId, thaqalaynSanad,
thaqalaynMatn, gradingsFull
```

Key fields:
- `thaqalaynSanad` — chain of narration (Arabic), separate from the matn
- `thaqalaynMatn` — hadith body text (Arabic), without the chain
- `majlisiGrading` — Allamah Majlisi's grading (Al-Kafi only)
- `behbudiGrading` — Shaykh Behbudi's grading (Al-Kafi only)
- `mohseniGrading` — Shaykh Mohseni's grading (some books)

### Book IDs

Al-Kafi volumes: `Al-Kafi-Volume-1-Kulayni` through `Al-Kafi-Volume-8-Kulayni`

Other major books: `Al-Tawhid-Saduq`, `Al-Amali-Saduq`, `Al-Amali-Mufid`, `Al-Khisal-Saduq`, `Man-La-Yahduruh-al-Faqih-Vol-1-Saduq` through Vol-5, `Nahj-al-Balagha-Radi`, `Uyun-akhbar-al-Rida-Volume-1-Saduq`, `Kitab-al-Ghayba-Numani`, `Kitab-al-Ghayba-Tusi`, `Kamil-al-Ziyarat-Qummi`, and others.

Run `python hadith_tagger.py` with no arguments to see the full list with hadith counts.

---

## Recommended Workflow

### First time setup

```bash
pip install requests anthropic
ollama pull qwen3:32b       # or qwen3:8b if RAM is limited

# Harvest translation corpus (one-time, ~10 min)
python hadith_translator.py harvest
python hadith_translator.py index
```

### Processing priority

1. **Rijal database** — start with `--count 50` to verify quality, then let it run
2. **Topic tagger** — start with a small book like `Al-Tawhid-Saduq` or `Fadail-al-Shia-Saduq` (45 hadiths) to verify, then `--all`
3. **Translation** — use `lookup` mode immediately for ongoing translation work; use `translate` mode for batch processing

### Model recommendations

| Task | Minimum model | Recommended model | Notes |
|------|--------------|-------------------|-------|
| Rijal extraction | qwen3:8b | qwen3:32b or Claude | Complex Arabic + JSON extraction |
| Topic tagging | qwen3:8b | qwen3:8b | Simple classification, 8b is sufficient |
| Translation | qwen3:8b | qwen3:32b or Claude | Translation quality scales with model size |
| Lookup | none | none | No AI needed, runs offline |

---

## Troubleshooting

### Ollama connection refused

```bash
# Make sure Ollama is running
ollama serve

# Check available models
ollama list
```

### JSON parse errors in rijal builder

The AI sometimes returns malformed JSON, especially with the 8b model. These entries are logged to `rijal_errors.log`. Re-run those specific ranges with a larger model:

```bash
# Check which entries failed
cat rijal_errors.log

# Re-process a specific range with better model
python rijal_builder.py --ollama --model qwen3:32b --start 150 --count 10
```

### Thaqalayn API timeout

Some books are large (Al-Kafi V6 has 2,509 hadiths). The scripts use 120-second timeouts. If you get timeouts, your internet connection may be slow. The API is free and unprotected — don't hammer it with parallel requests.

### Out of memory with Ollama

If `qwen3:32b` crashes, your machine doesn't have enough RAM. Fall back to `qwen3:8b`:

```bash
python rijal_builder.py --ollama --model qwen3:8b --count 50
```

Or reduce context window by lowering batch size:

```bash
python rijal_builder.py --ollama --model qwen3:32b --batch-size 4
```

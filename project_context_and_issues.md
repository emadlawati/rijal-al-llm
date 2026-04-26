# Project Context: Shia Isnad Analyzer & Rijal Resolver

This document provides a comprehensive overview of the current state of the Shia Isnad Analyzer project, intended for use as context for external AI models or developers.

## 1. Project Objective
The goal is to automate the authentication of Shia hadith chains (isnad) by resolving narrator identities against the *Mu'jam Rijal al-Hadith* and applying standard Shia grading logic (Sahih, Hasan, Muwathaq, Da'if).

## 2. Technical Stack
- **Languages**: Python 3.11+.
- **Data Sources**:
    - `rijal_database.json`: 15,000+ extracted narrator entries from Mu'jam Rijal al-Hadith.
    - `allBooks.json`: A large dataset of hadiths with historical gradings (Majlisi, Behbudi).
    - `rijal_resolver_index.json`: A searchable tokenized index for fast name matching.
- **Environment**: Windows (handled with UTF-8 stdout reconfiguration).

## 3. Core Components

### A. Rijal Resolver (`rijal_resolver.py`)
- **Identity Resolution**: Tokenizes Arabic names and searches the index.
- **Context-Aware Scoring**: Uses `narrates_from` and `narrated_by` to disambiguate common names.
- **Robustness Fixes**: 
    - Implemented a **Reliability Bonus** (`+2.0` score) for narrators marked as *Thiqah*.
    - Implemented a **Famousness Bonus** based on `hadith_count`.
    - These fixes prevent the system from accidentally picking a "Weak" duplicate entry when a famous, reliable narrator shares the same name.

### B. Isnad Analyzer (`isnad_analyzer.py`)
- **Parsing**: Splits raw Arabic strings by connectors like `عن`, `منهم`, `،`.
- **Normalization**: Automatically strips diacritics (tashkeel) and honorifics Like `(عليه السلام)` or `(ع)`.
- **Virtual Narrators**: Handles non-individual links like **"عدة من أصحابنا"** (Iddah min ashabina) and common Imams, treating them as `Thiqah`.
- **Grading Logic**: Follows the "weakest link" rule.

### C. Comparison Engine (`compare_majlisi.py`)
- Benchmarks the analyzer against Allamah Majlisi's gradings in `allBooks.json`.

## 4. Current Progress & Known Issues

### Progress
- **Resolution Accuracy**: High for individual names when provided clearly.
- **Database Consistency**: Improved via scoring biases.

### Key Issues (The "Bottlenecks")
1. **Isnad Extraction Heuristic**:
    - *Problem*: In `allBooks.json`, the isnad and the hadith text (matn) are stored together in one field.
    - *Current Solution*: A heuristic looking for the transition at the word `قال` (said).
    - *Failure Mode*: It often truncates the isnad too early or includes matn, leading to "Undetermined" grades. 
2. **Imam Identification**: 
    - While several Imams are in `VIRTUAL_NARRATORS`, a comprehensive list is needed to ensure every Sahih chain terminates correctly.
3. **Complex Connectors**: 
    - Some chains use archaic or complex phrases (e.g., `بإسناده عن`) that the current regex-based parser may struggle with.

## 5. Troubleshooting History
- **Initial Match Rate**: 0% matches against Majlisi's Sahih hadiths.
- **Diagnosis**: Found that `Iddah min ashabina` was failing resolution, and common narrators like *Muhammad ibn Yahya* were resolving to weak duplicates because the scoring was too neutral.
- **After Fixes**: Match rate improved to **1%**, with remaining failures being almost exclusively due to the **isnad extraction heuristic** (truncation before the end of the chain).

## 6. How to Run
```bash
# Analyze a specific chain
python isnad_analyzer.py --isnad "محمد بن يحيى، عن أحمد بن محمد، عن الوشاء، عن أبان بن عثمان، عن زرارة"

# Run the benchmark
python compare_majlisi.py 100
```

## 7. Context for Opus/External Review
When reviewing this code, focus on:
- Improving the `extract_isnad` logic to better distinguish narrator chains from message text in raw Arabic blocks.
- Suggesting a more robust tokenizer for complex Arabic hadith connectors.
- Enhancing the resolving logic to handle "skipped" links in abbreviated chains (e.g., `عن عدة...`).

#!/usr/bin/env python3
"""
Isnad Extractor — Robust boundary detection between isnad and matn.
===============================================================

Problem: Classical hadith texts store isnad + matn in one field.
The boundary between chain and text is often ambiguous.

Strategy (backward Imam-search):
1. Normalize the full text
2. Find ALL Imam references (using an exhaustive marker list)
3. The rightmost Imam reference is the chain endpoint
4. Extract everything before it as the isnad candidate
5. Validate: run through parse_isnad_string; last element should be an Imam
6. If validation fails, try the next candidate endpoint
"""

import re
from typing import List, Optional
from rijal_resolver import normalize_ar

# ── Exhaustive Imam marker list (post-normalize_ar form) ────────────────────
# All alef variants collapsed, no diacritics, no tatweel.
# We build both a regex for fast search AND a set for exact matching.

_IMAM_FORMS = [
    # Prophet
    "النبي", "رسول الله", "النبي محمد", "محمد رسول الله",
    # Imam Ali (1)
    "امير المومنين", "علي بن ابي طالب", "ابي الحسن امير المومنين",
    "ابو الحسن امير المومنين", "ابا الحسن امير المومنين",
    # Imam Hasan (2)
    "الحسن بن علي بن ابي طالب", "ابي محمد الحسن", "ابو محمد الحسن", "ابا محمد الحسن",
    # Imam Husayn (3)
    "الحسين بن علي بن ابي طالب", "ابي عبدالله الحسين", "ابو عبدالله الحسين", "ابا عبدالله الحسين",
    "الحسين بن علي",
    # Imam Sajjad (4)
    "علي بن الحسين", "زين العابدين", "السجاد",
    "ابي محمد علي بن الحسين", "ابو محمد علي بن الحسين", "ابا محمد علي بن الحسين",
    # Imam Baqir (5)
    "ابي جعفر", "ابو جعفر", "ابا جعفر",
    "الباقر", "محمد بن علي الباقر",
    "ابي جعفر الاول", "ابو جعفر الاول", "ابا جعفر الاول",
    # Imam Sadiq (6)
    "ابي عبدالله", "ابو عبدالله", "ابا عبدالله",
    "ابي عبد الله", "ابو عبد الله", "ابا عبد الله",
    "الصادق", "جعفر بن محمد", "جعفر الصادق",
    "ابي عبدالله جعفر بن محمد", "ابو عبدالله جعفر بن محمد", "ابا عبدالله جعفر بن محمد",
    # Imam Kadhim (7)
    "ابي الحسن", "ابو الحسن", "ابا الحسن",
    "الكاظم", "موسى بن جعفر", "موسى الكاظم",
    "ابي الحسن موسى", "ابو الحسن موسى", "ابا الحسن موسى",
    "ابي الحسن الاول", "ابو الحسن الاول", "ابا الحسن الاول",
    # Imam Reza (8)
    "الرضا", "علي بن موسى", "علي بن موسى الرضا",
    "ابي الحسن الرضا", "ابو الحسن الرضا", "ابا الحسن الرضا",
    "ابي الحسن الثاني", "ابو الحسن الثاني", "ابا الحسن الثاني",
    # Imam Jawad (9)
    "الجواد", "محمد بن علي الجواد", "محمد الجواد",
    "ابي جعفر الثاني", "ابو جعفر الثاني", "ابا جعفر الثاني",
    "ابي محمد الجواد", "ابو محمد الجواد", "ابا محمد الجواد",
    # Imam Hadi (10)
    "الهادي", "علي بن محمد الهادي", "علي الهادي",
    "ابي الحسن الثالث", "ابو الحسن الثالث", "ابا الحسن الثالث",
    "ابي الحسن علي بن محمد", "ابو الحسن علي بن محمد", "ابا الحسن علي بن محمد",
    # Imam Askari (11)
    "العسكري", "صاحب العسكر", "الحسن العسكري",
    "الحسن بن علي العسكري",
    "ابي محمد العسكري", "ابو محمد العسكري", "ابا محمد العسكري",
    # Imam Mahdi (12)
    "المهدي", "القائم", "صاحب الزمان",
    "ابي القاسم", "ابو القاسم", "ابا القاسم",
    "الحجة بن الحسن", "الحجة المهدي",
    # Titles
    "العبد الصالح", "الفقيه",
]

# Build regex that matches any Imam form as a whole word
# Sort by length descending so longer forms match first
_IMAM_FORMS_SORTED = sorted(set(_IMAM_FORMS), key=len, reverse=True)
_IMAM_PATTERN = re.compile(
    r'(?:' + '|'.join(re.escape(f) for f in _IMAM_FORMS_SORTED) + r')',
    re.UNICODE
)

# Honorific patterns that may follow an Imam name
_HONORIFIC = re.compile(
    r'(?:\s*[\(\[]?\s*(?:عليه|عليها|عليهما|عليهم)\s+(?:السلام|السلم)\s*[\)\]]?'
    r'|\s*[\(\[]?\s*[عصج]{1,3}\s*[\)\]]?'
    r'|\s*عليهم\s+السلام)?',
    re.UNICODE
)

# Matn-start indicators (weak signals — used only as fallback)
_MATN_MARKERS = [
    r'\bقال\b', r'\bيقول\b', r'\bفقال\b', r'\bفقلت\b', r'\bقلت\b',
    r'\bسالت\b', r'\bمسال[ةه]\b', r'\bفي\s+قول\s+الله\b',
    r'\bفي\s+رجل\b', r'\bذلك\s+ابن\b',
]
_MATN_PATTERN = re.compile('|'.join(_MATN_MARKERS), re.UNICODE)


def _find_imam_positions(norm_text: str) -> List[int]:
    """Return list of end positions of all Imam references in the text."""
    positions = []
    for m in _IMAM_PATTERN.finditer(norm_text):
        end = m.end()
        # Extend past optional honorific
        h = _HONORIFIC.match(norm_text, end)
        if h:
            end = h.end()
        positions.append(end)
    return positions


def _score_isnad_quality(candidate: str) -> float:
    """
    Score how 'isnad-like' a candidate string is.
    Higher = more likely to be a valid isnad.
    """
    score = 0.0
    # Connector density
    connectors = len(re.findall(r'\b(?:عن|حدثنا|اخبرنا|حدثني|اخبرني|سمعت)\b', candidate))
    score += connectors * 2.0
    # Name density (ibn/ben indicates names)
    ibn_count = len(re.findall(r'\b(?:بن|ابن|ابو|ابي|ابا)\b', candidate))
    score += ibn_count * 1.5
    # Penalize matn keywords
    matn_hits = len(_MATN_PATTERN.findall(candidate))
    score -= matn_hits * 5.0
    # Penalize very short
    if len(candidate) < 30:
        score -= 10.0
    # Penalize very long (likely includes matn)
    if len(candidate) > 500:
        score -= 5.0
    return score


def extract_isnad_robust(full_text: str, analyzer=None) -> str:
    """
    Extract the isnad from full hadith text using backward Imam-search.

    Args:
        full_text: The raw Arabic text (isnad + matn combined).
        analyzer: Optional IsnadAnalyzer instance for validation.

    Returns:
        Normalized isnad string ready for parse_isnad_string().
    """
    norm = normalize_ar(full_text)

    # Strip Kulayni header (same logic as parse_isnad_string)
    norm = re.sub(r'^(?:اخبرنا|حدثنا)\s+ابو جعفر محمد بن يعقوب\s*(?:الكليني\s*)?قال حدثني\s+', '', norm)
    norm = re.sub(r'^(?:حدثنا|اخبرنا|قال حدثنا|روى|وحدثني|وحدثنا)\s+', '', norm)

    # ── Strategy 1: Find rightmost Imam, extract everything before ──────────
    imam_positions = _find_imam_positions(norm)

    if imam_positions:
        # Try positions from rightmost to leftmost
        candidates = []
        for pos in reversed(imam_positions):
            candidate = norm[:pos].strip()
            candidates.append(candidate)

        # If we have an analyzer, validate each candidate
        if analyzer is not None:
            for candidate in candidates:
                names = analyzer.parse_isnad_string(candidate)
                if names:
                    # Check if last resolved element is an Imam
                    last_name = names[-1]
                    last_norm = normalize_ar(last_name)
                    from isnad_analyzer import VIRTUAL_NARRATORS
                    vm = VIRTUAL_NARRATORS.get(last_norm)
                    if vm:
                        ck = vm.get('canonical_key', '')
                        if ck.startswith('IMAM_'):
                            return candidate
            # None validated — fall through to scoring
        else:
            # No analyzer: just return the rightmost candidate
            return candidates[0] if candidates else norm[:400].strip()

        # Validation failed for all — score and pick best
        if candidates:
            scored = [(c, _score_isnad_quality(c)) for c in candidates]
            scored.sort(key=lambda x: -x[1])
            return scored[0][0]

    # ── Strategy 2: No Imam found — use matn-marker heuristic ───────────────
    # Find all قال/يقول and pick the one where the text before is most isnad-like
    matn_matches = list(_MATN_PATTERN.finditer(norm))
    if matn_matches:
        candidates = []
        for m in matn_matches:
            candidate = norm[:m.start()].strip()
            candidates.append(candidate)
        # Score and pick best
        scored = [(c, _score_isnad_quality(c)) for c in candidates]
        scored.sort(key=lambda x: -x[1])
        return scored[0][0]

    # ── Strategy 3: Fallback ────────────────────────────────────────────────
    return norm[:400].strip()


# ── Compatibility shim for compare_majlisi.py ───────────────────────────────

def extract_isnad(full_text: str) -> str:
    """Drop-in replacement for compare_majlisi.extract_isnad()."""
    return extract_isnad_robust(full_text)


if __name__ == '__main__':
    # Simple smoke test
    test_text = (
        "1 - أحمد بن محمد، عن الحسين بن سعيد، عن ابن أبي عمير، "
        "عن عمر بن اذينة، عن أبي عبد الله (ع) قال: «الحلال بين...»"
    )
    result = extract_isnad(test_text)
    print("Input:", test_text[:100])
    print("Extracted:", result)

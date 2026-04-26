#!/usr/bin/env python3
"""
Isnad Parser — Recursive descent parser for hadith chain syntax.
=========================================================

Replaces regex-heavy parsing with a formal grammar approach.
Much easier to extend, test, and debug.

Grammar:
    Chain       → Segment (Connector Segment)*
    Segment     → Name | GroupNarrator | FatherRef | RelayRef
    Connector   → "عن" | "حدثني" | "اخبرني" | "حدثنا" | "اخبرنا"
                  | "سمعت" | "بإسناده عن" | "ورواه عن" | ...
    Name        → Word (Word)*
    Word        → Arabic text excluding connectors and punctuation
    GroupNarrator → "عدة من أصحابنا" | "جماعة من أصحابنا" | ...
    FatherRef   → "ابيه" | "ابوه" | "عن ابيه"
    RelayRef    → "عنه" | "عنها" | "عمن رواه" | "عمن ذكره"
"""

import re
from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto
from rijal_resolver import normalize_ar


class TokenType(Enum):
    CONNECTOR = auto()    # عن, حدثني, etc.
    NAME = auto()         # A word that's part of a name
    GROUP = auto()        # عدة من أصحابنا etc.
    FATHER = auto()       # ابيه, ابوه
    RELAY = auto()        # عنه, عمن رواه
    PUNCT = auto()        # comma, و, etc.
    HONORIFIC = auto()    # (ع), عليه السلام
    UNKNOWN = auto()      # Unclassified


@dataclass
class Token:
    text: str
    type: TokenType
    pos: int  # position in original normalized text


@dataclass
class Segment:
    """A single narrator or reference in the chain."""
    text: str
    kind: str  # 'name', 'group', 'father', 'relay', 'imam'


# ── Connectors (in normalized form) ─────────────────────────────────────────
_CONNECTORS = {
    'عن', 'من', 'منهم', 'حدثني', 'حدثنا', 'اخبرني', 'اخبرنا',
    'سمعت', 'سمعته', 'روى', 'رواه', 'اخبرنا', 'حدثنا',
    'وحدثني', 'وحدثنا', 'بإسناده', 'باسناده', 'بسنده',
    'وبهذا الاسناد', 'وبهذا الإسناد',
}

# Two-word connectors
_CONNECTORS_2 = {
    ('باسناده', 'عن'), ('بإسناده', 'عن'), ('بسنده', 'عن'),
    ('ورواه', 'عن'), ('وروى', 'عن'),
    ('و', 'بهذا'), ('وبهذا', 'الاسناد'), ('وبهذا', 'الإسناد'),
    ('قال', 'حدثني'), ('قال', 'اخبرني'), ('قال', 'سمعت'),
}

# Group narrators — include both original and normalize_ar forms
_GROUP_NARRATORS_RAW = {
    'عدة من أصحابنا', 'جماعة من أصحابنا', 'غير واحد من أصحابنا',
    'بعض أصحابنا', 'بعض اصحابنا', 'بعض اصحابه',
    'غير واحد', 'جماعة', 'غيره', 'وغيره',
    'احدهما',
}
_GROUP_NARRATORS = _GROUP_NARRATORS_RAW | {normalize_ar(g) for g in _GROUP_NARRATORS_RAW}

_FATHER_REFS_RAW = {'ابيه', 'ابوه', 'عن ابيه', 'عن ابوه'}
_FATHER_REFS = _FATHER_REFS_RAW | {normalize_ar(f) for f in _FATHER_REFS_RAW}

_RELAY_REFS = {'عنه', 'عنها', 'عمن رواه', 'عمن ذكره', 'وعنه', 'وعنها'}

_HONORIFIC_PATTERNS = [
    re.compile(r'\( ?(?:عليه|عليها|عليهما|عليهم) (?:السلام|السلم) ?\)'),
    re.compile(r'\( ?[عصج]{1,3} ?\)'),
    re.compile(r'عليهم? السلام'),
    re.compile(r'صلوات الله عليه'),
]


class IsnadTokenizer:
    """Tokenizes normalized Arabic isnad text into parser tokens."""

    def __init__(self, text: str):
        self.text = normalize_ar(text)
        self.tokens: List[Token] = []
        self._tokenize()

    def _tokenize(self):
        # Remove common prefixes first
        text = self.text
        text = re.sub(r'^[\d٠-٩]+[-\s]\s*', '', text)
        text = re.sub(r'^(?:اخبرنا|حدثنا|قال حدثنا|روى|وحدثني|وحدثنا)\s+', '', text)
        text = re.sub(
            r'^(?:اخبرنا|حدثنا)\s+ابو جعفر محمد بن يعقوب\s*(?:الكليني\s*)?قال حدثني\s+',
            '', text
        )

        # Pre-process: split by whitespace but also handle commas attached to words
        raw_words = text.replace('،', ' ، ').replace(',', ' , ').split()
        words = [w for w in raw_words if w]

        i = 0
        while i < len(words):
            word = words[i]
            pos = self.text.find(word, sum(len(w) + 1 for w in words[:i]))

            # Skip empty
            if not word:
                i += 1
                continue

            # Punctuation / conjunctions — handle FIRST
            if word in ('،', ',', 'و'):
                self.tokens.append(Token(word, TokenType.PUNCT, pos))
                i += 1
                continue

            # Two-word connector check
            if i + 1 < len(words):
                pair = (word, words[i + 1])
                if pair in _CONNECTORS_2 or (pair[0] in ('باسناده', 'بإسناده', 'بسنده') and pair[1] == 'عن'):
                    self.tokens.append(Token(f"{word} {words[i + 1]}", TokenType.CONNECTOR, pos))
                    i += 2
                    continue

            # Single-word connector
            if word in _CONNECTORS or word.lstrip('و') in _CONNECTORS:
                self.tokens.append(Token(word, TokenType.CONNECTOR, pos))
                i += 1
                continue

            # Group narrators (check multi-word) — BEFORE single-word checks
            matched_group = False
            for g in sorted(_GROUP_NARRATORS, key=len, reverse=True):
                g_words = g.split()
                if i + len(g_words) <= len(words):
                    if words[i:i + len(g_words)] == g_words:
                        self.tokens.append(Token(g, TokenType.GROUP, pos))
                        i += len(g_words)
                        matched_group = True
                        break
            if matched_group:
                continue

            # Father reference (also check multi-word)
            matched_father = False
            for fref in sorted(_FATHER_REFS, key=len, reverse=True):
                f_words = fref.split()
                if i + len(f_words) <= len(words):
                    if words[i:i + len(f_words)] == f_words:
                        self.tokens.append(Token(fref, TokenType.FATHER, pos))
                        i += len(f_words)
                        matched_father = True
                        break
            if matched_father:
                continue

            # Relay reference
            if word in _RELAY_REFS:
                self.tokens.append(Token(word, TokenType.RELAY, pos))
                i += 1
                continue

            # Honorific check (may be attached to word or standalone)
            is_honorific = False
            for pat in _HONORIFIC_PATTERNS:
                if pat.match(word):
                    self.tokens.append(Token(word, TokenType.HONORIFIC, pos))
                    i += 1
                    is_honorific = True
                    break
            if is_honorific:
                continue

            # Default: it's a name word
            self.tokens.append(Token(word, TokenType.NAME, pos))
            i += 1

    def __iter__(self):
        return iter(self.tokens)

    def __len__(self):
        return len(self.tokens)

    def peek(self, offset: int = 0) -> Optional[Token]:
        idx = 0 + offset
        return self.tokens[idx] if 0 <= idx < len(self.tokens) else None


class IsnadParser:
    """
    Recursive descent parser for hadith chains.

    Parses a token stream into a list of Segments (narrators).
    """

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
        self.segments: List[Segment] = []

    def _current(self) -> Optional[Token]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _advance(self) -> Optional[Token]:
        tok = self._current()
        self.pos += 1
        return tok

    def _skip_punct(self):
        while self._current() and self._current().type == TokenType.PUNCT:
            self._advance()

    def _is_connector(self, tok: Optional[Token]) -> bool:
        return tok is not None and tok.type == TokenType.CONNECTOR

    def parse(self) -> List[Segment]:
        """Parse the full token stream into segments."""
        self.segments = []
        self._skip_punct()

        while self.pos < len(self.tokens):
            segment = self._parse_segment()
            if segment:
                self.segments.append(segment)
            self._skip_punct()

            # Consume connector between segments
            if self._is_connector(self._current()):
                self._advance()
                self._skip_punct()

        return self.segments

    def _parse_segment(self) -> Optional[Segment]:
        """Parse a single segment (name, group, father, relay)."""
        tok = self._current()
        if not tok:
            return None

        if tok.type == TokenType.GROUP:
            self._advance()
            return Segment(tok.text, 'group')

        if tok.type == TokenType.FATHER:
            self._advance()
            return Segment(tok.text, 'father')

        if tok.type == TokenType.RELAY:
            self._advance()
            return Segment(tok.text, 'relay')

        if tok.type == TokenType.NAME:
            return self._parse_name_segment()

        # Skip unknown/honorific tokens
        if tok.type in (TokenType.HONORIFIC, TokenType.UNKNOWN):
            self._advance()
            return None

        if tok.type == TokenType.CONNECTOR:
            # Lone connector — skip
            self._advance()
            return None

        return None

    def _parse_name_segment(self) -> Optional[Segment]:
        """Parse a sequence of NAME tokens into a single name."""
        parts = []
        while self._current() and self._current().type == TokenType.NAME:
            parts.append(self._current().text)
            self._advance()
            # Absorb attached honorifics
            while self._current() and self._current().type == TokenType.HONORIFIC:
                parts.append(self._current().text)
                self._advance()

        if not parts:
            return None

        name_text = ' '.join(parts)
        # Clean trailing punctuation
        name_text = re.sub(r'[،,]+$', '', name_text).strip()
        return Segment(name_text, 'name')


def parse_isnad_new(isnad_str: str) -> List[str]:
    """
    New parsing entry point. Returns list of name strings
    compatible with IsnadAnalyzer.parse_isnad_string().
    """
    tokenizer = IsnadTokenizer(isnad_str)
    parser = IsnadParser(tokenizer.tokens)
    segments = parser.parse()
    return [seg.text for seg in segments if seg.text]


# ── Compatibility: old-style flat name extraction ───────────────────────────

def parse_isnad_compatible(isnad_str: str) -> List[str]:
    """
    Hybrid approach: use the recursive parser, but fall back to
    the legacy regex parser if the new one returns too few names.
    This allows gradual migration with safety net.
    """
    new_names = parse_isnad_new(isnad_str)

    # If new parser found < 2 names, try old parser
    if len(new_names) < 2:
        from isnad_analyzer import IsnadAnalyzer
        legacy = IsnadAnalyzer().parse_isnad_string(isnad_str)
        if len(legacy) >= len(new_names):
            return legacy

    return new_names


if __name__ == '__main__':
    tests = [
        "أحمد بن محمد، عن الحسين بن سعيد، عن ابن أبي عمير، عن عمر بن اذينة، عن أبي عبد الله (ع) قال",
        "عدة من أصحابنا، عن أحمد بن محمد، عن ابن أبي عمير",
        "1 - محمد بن يحيى، عن أحمد بن محمد، عن الوشاء، عن أبان بن عثمان، عن زرارة",
        "بإسناده عن أحمد بن محمد، عن أبيه، عن أبي عبد الله (ع)",
    ]

    for t in tests:
        print("Input:", t[:80])
        print("Tokens:", [f"{tok.text}({tok.type.name})" for tok in IsnadTokenizer(t)])
        print("Parsed:", parse_isnad_new(t))
        print()

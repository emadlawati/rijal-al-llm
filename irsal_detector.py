#!/usr/bin/env python3
"""
Irsal (إرسال) Detector
======================

Detects gaps in hadith chains (irsal / mursal / mu'dal) by identifying
anonymous narrators and indirect transmission markers.

In classical hadith terminology:
- **Mursal** (مرسل): chain has a gap — a Tabi'i narrates directly from the
  Prophet without mentioning a Companion, or a narrator is anonymous.
- **Mu'dal** (مُعْضَل): two or more consecutive narrators are missing.
- **Munqati'** (منقطع): one narrator is missing at any point.

Common irsal patterns in Shia hadith:
    عن رجل           — "from a man" (anonymous)
    عن شيخ           — "from a shaykh" (anonymous)
    عن فلان          — "from so-and-so" (placeholder)
    عن فلان بن فلان  — "from so-and-so son of so-and-so"
    عن فلانة         — "from so-and-so" (female)
    عن امرأة         — "from a woman" (anonymous)
    عن رجل من أصحابنا — "from a man of our companions"
    عن شيخ له        — "from a shaykh of his"
    عن بعض شيوخه     — "from some of his shaykhs"
    عن جماعة         — "from a group" (when not the standard iddah forms)

Detection strategy:
1. After parsing the chain, scan each segment for irsal patterns.
2. Classify the severity based on position and pattern type.
3. Report with classical terminology and reasoning.
"""

import sys
import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum, auto

from rijal_resolver import normalize_ar


class IrsalType(Enum):
    """Classical categories of chain gaps."""
    MURSAL = auto()      # One anonymous/missing link
    MUDAL = auto()       # Two+ consecutive missing links
    MUNQATI = auto()     # One missing link (general)
    MUBHAM = auto()      # Ambiguous narrator (not necessarily a gap)


@dataclass
class IrsalFinding:
    """A single irsal detection in a chain."""
    index: int           # position in the chain
    segment: str         # the raw text that triggered
    irsal_type: IrsalType
    pattern: str         # which pattern matched
    description: str     # human-readable explanation
    severity: str        # 'critical', 'major', 'minor'


# ── Pattern definitions ─────────────────────────────────────────────────────

# Anonymous / placeholder narrator patterns (normalized form)
_IRSAL_PATTERNS: List[Tuple[str, IrsalType, str, str]] = [
    # (regex pattern, type, description, severity)
    (r'^\s*عمن\s+رواه\s*$',       IrsalType.MURSAL, 'عمن رواه — anonymous relay', 'critical'),
    (r'^\s*عمن\s+ذكره\s*$',       IrsalType.MURSAL, 'عمن ذكره — anonymous relay', 'critical'),
    (r'^\s*عن\s+رجل\b',           IrsalType.MURSAL, 'عن رجل — anonymous man', 'critical'),
    (r'^\s*عن\s+رجل\s+من',       IrsalType.MURSAL, 'عن رجل من... — anonymous man from group', 'critical'),
    (r'^\s*عن\s+شيخ\b',           IrsalType.MURSAL, 'عن شيخ — anonymous shaykh', 'critical'),
    (r'^\s*عن\s+شيخ\s+له\b',     IrsalType.MURSAL, 'عن شيخ له — anonymous shaykh of his', 'critical'),
    (r'^\s*عن\s+بعض\s+شيوخه\b',  IrsalType.MURSAL, 'عن بعض شيوخه — some of his shaykhs (unspecified)', 'major'),
    (r'^\s*عن\s+فلان\b',          IrsalType.MURSAL, 'عن فلان — placeholder name (so-and-so)', 'critical'),
    (r'^\s*عن\s+فلان\s+بن\s+فلان', IrsalType.MURSAL, 'عن فلان بن فلان — placeholder name', 'critical'),
    (r'^\s*عن\s+فلانة\b',         IrsalType.MURSAL, 'عن فلانة — placeholder name (female)', 'critical'),
    (r'^\s*عن\s+امرأة\b',         IrsalType.MURSAL, 'عن امرأة — anonymous woman', 'critical'),
    (r'^\s*عن\s+جماعة\s*$',       IrsalType.MURSAL, 'عن جماعة — vague group (not iddah)', 'major'),
    (r'^\s*عن\s+قوم\b',           IrsalType.MURSAL, 'عن قوم — vague group', 'major'),
    (r'^\s*عن\s+ناس\b',           IrsalType.MURSAL, 'عن ناس — vague people', 'major'),
    (r'^\s*عن\s+اثنين\b',         IrsalType.MURSAL, 'عن اثنين — two unnamed people', 'major'),
    (r'^\s*عن\s+ثلاثة\b',         IrsalType.MURSAL, 'عن ثلاثة — three unnamed people', 'major'),
    (r'^\s*عن\s+رجال\b',          IrsalType.MURSAL, 'عن رجال — anonymous men (plural)', 'critical'),
    (r'^\s*عن\s+اخر\b',           IrsalType.MUBHAM, 'عن آخر — "another" (ambiguous)', 'minor'),
    (r'^\s*عن\s+غير\s+واحد\b',   IrsalType.MUBHAM, 'عن غير واحد — "more than one" (ambiguous)', 'minor'),
]

# Pre-compile all patterns
_COMPILED_PATTERNS = [
    (re.compile(pat, re.UNICODE), itype, desc, sev)
    for pat, itype, desc, sev in _IRSAL_PATTERNS
]


class IrsalDetector:
    """Detects gaps and anonymous narrators in hadith chains."""

    def __init__(self):
        self.findings: List[IrsalFinding] = []

    def detect(self, chain_segments: List[str]) -> List[IrsalFinding]:
        """
        Analyze a parsed chain for irsal patterns.

        Args:
            chain_segments: List of name strings from parse_isnad_string()

        Returns:
            List of IrsalFinding objects, sorted by index.
        """
        self.findings = []
        consecutive_anonymous = 0
        last_anon_index = -2

        for i, seg in enumerate(chain_segments):
            norm = normalize_ar(seg)
            finding = self._check_segment(i, norm, seg)

            if finding:
                self.findings.append(finding)

                # Track consecutive anonymous links for mu'dal detection
                if i == last_anon_index + 1:
                    consecutive_anonymous += 1
                else:
                    consecutive_anonymous = 1
                last_anon_index = i

                # If two+ consecutive anonymous links → upgrade to mu'dal
                if consecutive_anonymous >= 2:
                    # Upgrade previous finding to MUDAL if it was MURSAL
                    for f in self.findings:
                        if f.irsal_type == IrsalType.MURSAL and f.index >= i - 1:
                            f.irsal_type = IrsalType.MUDAL
                            f.description = f.description.replace('Mursal', 'Mu\'dal')
                            f.description += ' [UPGRADED: consecutive anonymous links]'
                            f.severity = 'critical'

        return sorted(self.findings, key=lambda f: f.index)

    def _check_segment(self, index: int, norm: str, raw: str) -> Optional[IrsalFinding]:
        """Check a single segment against all irsal patterns."""
        for compiled, itype, desc, sev in _COMPILED_PATTERNS:
            if compiled.search(norm):
                return IrsalFinding(
                    index=index,
                    segment=raw,
                    irsal_type=itype,
                    pattern=compiled.pattern,
                    description=desc,
                    severity=sev,
                )
        return None

    def classify_chain(self, chain_segments: List[str]) -> Dict:
        """
        Full classification of a chain's irsal status.

        Returns a dict with:
            - has_irsal: bool
            - irsal_type: str (most severe finding)
            - findings: list of dicts
            - recommendation: str
        """
        findings = self.detect(chain_segments)

        if not findings:
            return {
                'has_irsal': False,
                'irsal_type': None,
                'findings': [],
                'recommendation': 'No irsal detected.',
            }

        # Determine most severe
        severity_order = {'critical': 3, 'major': 2, 'minor': 1}
        most_severe = max(findings, key=lambda f: severity_order.get(f.severity, 0))

        type_names = {
            IrsalType.MURSAL: 'Mursal (مرسل)',
            IrsalType.MUDAL: "Mu'dal (مُعْضَل)",
            IrsalType.MUNQATI: 'Munqati\' (منقطع)',
            IrsalType.MUBHAM: 'Mubham (مبهَم)',
        }

        # Build recommendation
        if most_severe.irsal_type == IrsalType.MUDAL:
            rec = "Chain is MU'DAL — two or more consecutive anonymous links. This significantly weakens the chain."
        elif most_severe.irsal_type == IrsalType.MURSAL:
            rec = "Chain is MURSAL — contains anonymous narrator(s). Grade should be downgraded unless the anonymous narrator is known to be thiqah by context."
        else:
            rec = "Chain contains ambiguous reference(s). Verify whether the ambiguity is resolvable from context."

        return {
            'has_irsal': True,
            'irsal_type': type_names.get(most_severe.irsal_type, 'Unknown'),
            'findings': [
                {
                    'index': f.index,
                    'segment': f.segment,
                    'type': type_names.get(f.irsal_type, 'Unknown'),
                    'description': f.description,
                    'severity': f.severity,
                }
                for f in findings
            ],
            'recommendation': rec,
        }


def detect_irsal_in_chain(chain_segments: List[str]) -> Dict:
    """Convenience function: detect irsal in a parsed chain."""
    detector = IrsalDetector()
    return detector.classify_chain(chain_segments)


# ── Integration with IsnadAnalyzer ──────────────────────────────────────────

def enrich_analysis_with_irsal(analysis: Dict, chain_segments: List[str]):
    """
    Add irsal detection to an existing analysis dict (from IsnadAnalyzer.analyze()).
    Modifies analysis in-place by adding 'irsal' key.
    """
    irsal_result = detect_irsal_in_chain(chain_segments)
    analysis['irsal'] = irsal_result

    # If irsal is critical, downgrade the grade
    if irsal_result['has_irsal']:
        findings = irsal_result.get('findings', [])
        critical_count = sum(1 for f in findings if f['severity'] == 'critical')
        if critical_count > 0:
            current_grade = analysis.get('final_status', {}).get('grade', '')
            if 'Sahih' in current_grade:
                analysis['final_status']['grade'] = "Da'if (Weak — Irsal/Mursal)"
                analysis['final_status']['irsal_override'] = True


# ── Self-test ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

    test_chains = [
        ["أحمد بن محمد", "عن رجل", "عن أبي عبد الله"],
        ["محمد بن يحيى", "عن شيخ", "عن رجل", "عن أبي عبد الله"],
        ["عدة من أصحابنا", "عن أحمد بن محمد", "عن أبي عبد الله"],
        ["أحمد بن محمد", "عمن رواه", "عن أبي عبد الله"],
        ["أحمد بن محمد", "عن فلان بن فلان", "عن أبي عبد الله"],
        ["أحمد بن محمد", "الحسين بن سعيد", "عن أبي عبد الله"],
    ]

    detector = IrsalDetector()
    for chain in test_chains:
        result = detector.classify_chain(chain)
        print(f"Chain: {chain}")
        print(f"  Irsal: {result['has_irsal']} | Type: {result['irsal_type']}")
        if result['findings']:
            for f in result['findings']:
                print(f"    [{f['index']}] {f['description']} ({f['severity']})")
        print(f"  → {result['recommendation']}")
        print()

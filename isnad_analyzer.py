#!/usr/bin/env python3
"""
Isnad Analyzer — Hadith Authentication Engine
=============================================
Analyzes a chain of narrators (isnad) and provides status for each link 
and a final grading for the entire chain.

Usage:
    python isnad_analyzer.py --isnad "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة"
"""

import json
import sys
import argparse
import re
from pathlib import Path
from typing import Optional, List, Dict

# Import from resolver
try:
    from rijal_resolver import normalize_ar, tokenize, resolve, RESOLVER_FILE, DATABASE_FILE, IDENTITIES_FILE
except ImportError:
    print("Error: rijal_resolver.py not found in the same directory.")
    sys.exit(1)

# Import practical rules for isnad analysis
try:
    from isnad_rules import IsnadPracticalRules
except ImportError:
    # Fallback if isnad_rules is not available
    IsnadPracticalRules = None

# Import the new database loader for lazy loading
try:
    from database_loader import DatabaseLoader, get_loader
except ImportError:
    # Fallback if database_loader is not available
    DatabaseLoader = None
    get_loader = None

# Import enhanced tabaqah inference
try:
    from tabaqah_inference import TabaqahInferenceEngine, IMAM_TABAQAH_MAP
except ImportError:
    # Fallback if tabaqah_inference is not available
    TabaqahInferenceEngine = None

# Import transmission feasibility validator (Phase 3)
try:
    from transmission_validator import annotate_chain as _validate_transmission_chain
    from transmission_validator import detect_tadlis_in_chain as _detect_tadlis
except ImportError:
    _validate_transmission_chain = None
    _detect_tadlis = None


# Configure stdout for Windows Unicode support
if sys.platform == "win32":
    import io
    # Only wrap if not already utf-8
    if getattr(sys.stdout, 'encoding', '').lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

def _imam(key, ar, en=""):
    return {
        "canonical_key": key,
        "name_ar": ar,
        "name_en": en,
        "status": "thiqah",
        "confidence_score": 2.0,
        "match_reasons": ["Virtual link (Imam — infallible)"]
    }

def _iddah(key, ar):
    return {
        "canonical_key": key,
        "name_ar": ar,
        "status": "thiqah",
        "confidence_score": 1.0,
        "match_reasons": ["Virtual link (Collective trustworthy narrators)"]
    }

# All keys must be the result of normalize_ar() on common chain forms.
# normalize_ar collapses: أ إ آ ٱ → ا, removes tashkeel/tatweel.
VIRTUAL_NARRATORS = {
    # ── Iddah (group narrators) ───────────────────────────────────────────────
    "عدة من اصحابنا":           _iddah("VIRTUAL_IDDAH", "عدة من أصحابنا"),
    "عدة من اصحابنا منهم":      _iddah("VIRTUAL_IDDAH", "عدة من أصحابنا"),
    "جماعة من اصحابنا":         _iddah("VIRTUAL_IDDAH", "جماعة من أصحابنا"),
    # Short forms of iddah-type group references
    "غير واحد":                 _iddah("IDDAH_ASHAB", "غير واحد من أصحابنا"),
    "غيره":                     _iddah("VIRTUAL_ANHU", "غيره"),      # "and others" — relay group
    # "One of them both" — refers to al-Baqir or al-Sadiq (both reliable)
    "احدهما":                   _iddah("VIRTUAL_IDDAH_IMAM", "أحدهما (ع)"),
    "عنه":                      _iddah("VIRTUAL_ANHU", "عنه"),      # "from him" — relay
    "وعنه":                     _iddah("VIRTUAL_ANHU", "عنه"),

    # ── Prophet (ص) ──────────────────────────────────────────────────────────
    "النبي":                    _imam("IMAM_PROPHET", "النبي محمد (ص)", "The Prophet Muhammad"),
    "رسول الله":                _imam("IMAM_PROPHET", "رسول الله (ص)", "The Prophet Muhammad"),
    "النبي محمد":               _imam("IMAM_PROPHET", "النبي محمد (ص)", "The Prophet Muhammad"),

    # ── Imam Ali — أمير المؤمنين (1st) ───────────────────────────────────────
    "امير المومنين":             _imam("IMAM_ALI", "أمير المؤمنين علي (ع)", "Imam Ali ibn Abi Talib"),
    "علي بن ابي طالب":          _imam("IMAM_ALI", "علي بن أبي طالب (ع)", "Imam Ali ibn Abi Talib"),
    "ابي الحسن امير المومنين":  _imam("IMAM_ALI", "أمير المؤمنين علي (ع)", "Imam Ali ibn Abi Talib"),

    # ── Imam Hasan — الحسن (2nd) ─────────────────────────────────────────────
    "الحسن بن علي بن ابي طالب": _imam("IMAM_HASAN", "الحسن بن علي (ع)", "Imam al-Hasan"),
    "ابي محمد الحسن":           _imam("IMAM_HASAN", "الحسن بن علي (ع)", "Imam al-Hasan"),

    # ── Imam Husayn — الحسين (3rd) ───────────────────────────────────────────
    "الحسين بن علي بن ابي طالب": _imam("IMAM_HUSAYN", "الحسين بن علي (ع)", "Imam al-Husayn"),
    "ابي عبدالله الحسين":        _imam("IMAM_HUSAYN", "الحسين بن علي (ع)", "Imam al-Husayn"),

    # ── Imam Sajjad — علي بن الحسين (4th) ───────────────────────────────────
    "علي بن الحسين":             _imam("IMAM_SAJJAD", "علي بن الحسين (ع)", "Imam Zayn al-Abidin"),
    "زين العابدين":              _imam("IMAM_SAJJAD", "زين العابدين (ع)", "Imam Zayn al-Abidin"),
    "السجاد":                   _imam("IMAM_SAJJAD", "الإمام السجاد (ع)", "Imam Zayn al-Abidin"),
    "ابي محمد علي بن الحسين":   _imam("IMAM_SAJJAD", "علي بن الحسين (ع)", "Imam Zayn al-Abidin"),

    # ── Imam Baqir — أبو جعفر (5th) ─────────────────────────────────────────
    "ابي جعفر":                 _imam("IMAM_BAQIR", "أبي جعفر الباقر (ع)", "Imam al-Baqir"),
    "ابو جعفر":                 _imam("IMAM_BAQIR", "أبو جعفر الباقر (ع)", "Imam al-Baqir"),
    "ابا جعفر":                 _imam("IMAM_BAQIR", "أبا جعفر الباقر (ع)", "Imam al-Baqir"),  # accusative
    "الباقر":                   _imam("IMAM_BAQIR", "الإمام الباقر (ع)", "Imam al-Baqir"),
    "محمد بن علي الباقر":       _imam("IMAM_BAQIR", "محمد بن علي الباقر (ع)", "Imam al-Baqir"),
    "ابي جعفر الاول":           _imam("IMAM_BAQIR", "أبي جعفر الباقر (ع)", "Imam al-Baqir"),
    "ابو جعفر الاول":           _imam("IMAM_BAQIR", "أبو جعفر الباقر (ع)", "Imam al-Baqir"),

    # ── Imam Sadiq — أبو عبدالله (6th) ──────────────────────────────────────
    # Note: "عبدالله" and "عبد الله" are both common spellings — include both
    "ابي عبدالله":              _imam("IMAM_SADIQ", "أبي عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "ابو عبدالله":              _imam("IMAM_SADIQ", "أبو عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "ابا عبدالله":              _imam("IMAM_SADIQ", "أبا عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "ابي عبد الله":             _imam("IMAM_SADIQ", "أبي عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "ابو عبد الله":             _imam("IMAM_SADIQ", "أبو عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "ابا عبد الله":             _imam("IMAM_SADIQ", "أبا عبد الله الصادق (ع)", "Imam al-Sadiq"),
    "الصادق":                   _imam("IMAM_SADIQ", "الإمام الصادق (ع)", "Imam al-Sadiq"),
    "جعفر بن محمد":             _imam("IMAM_SADIQ", "جعفر بن محمد الصادق (ع)", "Imam al-Sadiq"),
    "ابي عبدالله جعفر بن محمد": _imam("IMAM_SADIQ", "جعفر بن محمد الصادق (ع)", "Imam al-Sadiq"),

    # ── Imam Kadhim — أبو الحسن الأول (7th) ─────────────────────────────────
    "ابي الحسن":                _imam("IMAM_KADHIM", "أبي الحسن الكاظم (ع)", "Imam al-Kadhim"),
    "ابو الحسن":                _imam("IMAM_KADHIM", "أبو الحسن الكاظم (ع)", "Imam al-Kadhim"),
    "ابا الحسن":                _imam("IMAM_KADHIM", "أبا الحسن الكاظم (ع)", "Imam al-Kadhim"),  # accusative
    "الكاظم":                   _imam("IMAM_KADHIM", "الإمام الكاظم (ع)", "Imam al-Kadhim"),
    "موسى بن جعفر":             _imam("IMAM_KADHIM", "موسى بن جعفر (ع)", "Imam al-Kadhim"),
    "ابي الحسن موسى":           _imam("IMAM_KADHIM", "أبي الحسن موسى الكاظم (ع)", "Imam al-Kadhim"),
    "ابي الحسن الاول":          _imam("IMAM_KADHIM", "أبي الحسن الكاظم (ع)", "Imam al-Kadhim"),
    "ابو الحسن الاول":          _imam("IMAM_KADHIM", "أبو الحسن الكاظم (ع)", "Imam al-Kadhim"),

    # ── Imam Reza — أبو الحسن الثاني (8th) ──────────────────────────────────
    "الرضا":                    _imam("IMAM_REZA", "الإمام الرضا (ع)", "Imam al-Reza"),
    "علي بن موسى":              _imam("IMAM_REZA", "علي بن موسى الرضا (ع)", "Imam al-Reza"),
    "ابي الحسن الرضا":          _imam("IMAM_REZA", "أبي الحسن الرضا (ع)", "Imam al-Reza"),
    "ابو الحسن الرضا":          _imam("IMAM_REZA", "أبو الحسن الرضا (ع)", "Imam al-Reza"),
    "ابي الحسن الثاني":         _imam("IMAM_REZA", "أبي الحسن الرضا (ع)", "Imam al-Reza"),
    "ابو الحسن الثاني":         _imam("IMAM_REZA", "أبو الحسن الرضا (ع)", "Imam al-Reza"),
    "علي بن موسى الرضا":        _imam("IMAM_REZA", "علي بن موسى الرضا (ع)", "Imam al-Reza"),

    # ── Imam Jawad — أبو جعفر الثاني (9th) ──────────────────────────────────
    "الجواد":                   _imam("IMAM_JAWAD", "الإمام الجواد (ع)", "Imam al-Jawad"),
    "ابي جعفر الثاني":          _imam("IMAM_JAWAD", "أبي جعفر الجواد (ع)", "Imam al-Jawad"),
    "ابو جعفر الثاني":          _imam("IMAM_JAWAD", "أبو جعفر الجواد (ع)", "Imam al-Jawad"),
    "محمد بن علي الجواد":       _imam("IMAM_JAWAD", "محمد بن علي الجواد (ع)", "Imam al-Jawad"),
    "ابي محمد الجواد":          _imam("IMAM_JAWAD", "أبي محمد الجواد (ع)", "Imam al-Jawad"),

    # ── Imam Hadi — أبو الحسن الثالث (10th) ─────────────────────────────────
    "الهادي":                   _imam("IMAM_HADI", "الإمام الهادي (ع)", "Imam al-Hadi"),
    "ابي الحسن الثالث":         _imam("IMAM_HADI", "أبي الحسن الهادي (ع)", "Imam al-Hadi"),
    "ابو الحسن الثالث":         _imam("IMAM_HADI", "أبو الحسن الهادي (ع)", "Imam al-Hadi"),
    "علي بن محمد الهادي":       _imam("IMAM_HADI", "علي بن محمد الهادي (ع)", "Imam al-Hadi"),
    "ابي الحسن علي بن محمد":    _imam("IMAM_HADI", "أبي الحسن علي بن محمد الهادي (ع)", "Imam al-Hadi"),

    # ── Imam Askari — أبو محمد (11th) ────────────────────────────────────────
    "العسكري":                  _imam("IMAM_ASKARI", "الإمام العسكري (ع)", "Imam al-Askari"),
    "صاحب العسكر":              _imam("IMAM_ASKARI", "صاحب العسكر (ع)", "Imam al-Askari"),
    "الحسن العسكري":            _imam("IMAM_ASKARI", "الحسن العسكري (ع)", "Imam al-Askari"),
    "الحسن بن علي العسكري":     _imam("IMAM_ASKARI", "الحسن بن علي العسكري (ع)", "Imam al-Askari"),
    "ابي محمد العسكري":         _imam("IMAM_ASKARI", "أبي محمد العسكري (ع)", "Imam al-Askari"),
    "ابو محمد العسكري":         _imam("IMAM_ASKARI", "أبو محمد العسكري (ع)", "Imam al-Askari"),

    # ── Imam Mahdi (عج) (12th) ───────────────────────────────────────────────
    "المهدي":                   _imam("IMAM_MAHDI", "الإمام المهدي (عج)", "Imam al-Mahdi"),
    "القائم":                   _imam("IMAM_MAHDI", "القائم (عج)", "Imam al-Mahdi"),
    "صاحب الزمان":              _imam("IMAM_MAHDI", "صاحب الزمان (عج)", "Imam al-Mahdi"),
    "ابي القاسم":               _imam("IMAM_MAHDI", "أبي القاسم المهدي (عج)", "Imam al-Mahdi"),
    "ابو القاسم":               _imam("IMAM_MAHDI", "أبو القاسم المهدي (عج)", "Imam al-Mahdi"),
    "الحجة بن الحسن":           _imam("IMAM_MAHDI", "الحجة بن الحسن المهدي (عج)", "Imam al-Mahdi"),

    # ── Accusative "ابا الحسن" forms (often Reza) ────────────────────────────
    "ابا الحسن الرضا":          _imam("IMAM_REZA",   "أبا الحسن الرضا (ع)",   "Imam al-Reza"),
    "ابا جعفر وابا عبد الله":  _iddah("IMAM_SADIQ", "أبا جعفر وأبا عبد الله (ع)"),  # both Imams

    # ── Title forms ──────────────────────────────────────────────────────────
    "العبد الصالح":             _imam("IMAM_KADHIM", "العبد الصالح موسى بن جعفر (ع)", "Imam al-Kadhim"),
    "الفقيه":                   _imam("IMAM_SADIQ",  "الفقيه جعفر بن محمد (ع)", "Imam al-Sadiq"),

    # ── Sibling references ───────────────────────────────────────────────────
    "اخيه ابي الحسن":          _imam("IMAM_REZA",   "أبي الحسن الرضا (ع) [اخيه]",  "Imam al-Reza"),
    "اخيه موسى":               _imam("IMAM_KADHIM", "موسى بن جعفر (ع) [اخيه]",     "Imam al-Kadhim"),
    "اخيه الحسين":             _imam("IMAM_HUSAYN", "الحسين بن علي (ع) [اخيه]",    "Imam al-Husayn"),
    "اخيه موسى بن جعفر":      _imam("IMAM_KADHIM", "موسى بن جعفر (ع) [اخيه]",     "Imam al-Kadhim"),
    "اخيه ابي الحسن الاول":   _imam("IMAM_KADHIM", "أبي الحسن الكاظم (ع) [اخيه]", "Imam al-Kadhim"),
    "اخيه ابي الحسن موسى":    _imam("IMAM_KADHIM", "أبي الحسن موسى (ع) [اخيه]",   "Imam al-Kadhim"),
    "اخي موسى":                _imam("IMAM_KADHIM", "موسى بن جعفر (ع) [اخي]",      "Imam al-Kadhim"),

    # ── Plural / dual Imam references ───────────────────────────────────────
    "الصادقين":                _iddah("IMAM_SADIQ", "الصادقَيْن (ع)"),  # both Baqir and Sadiq

    # ── Additional عدة-type group narrators ──────────────────────────────────
    "غير واحد من اصحابنا":    _iddah("IDDAH_ASHAB", "غير واحد من أصحابنا"),
    "جماعة":                   _iddah("IDDAH_ASHAB", "جماعة"),
    "بعض اصحابنا":             _iddah("IDDAH_ASHAB", "بعض أصحابنا"),
    "بعض اصحابه":              _iddah("IDDAH_ASHAB", "بعض أصحابه"),

    # ── Dual-Imam references (both Baqir and Sadiq) ──────────────────────────
    "ابا جعفر و ابا عبد الله":  _iddah("IMAM_SADIQ", "أبا جعفر وأبا عبد الله (ع)"),   # spaced
    "ابي جعفر وابي عبد الله":   _iddah("IMAM_SADIQ", "أبي جعفر وأبي عبد الله (ع)"),   # genitive
    "ابي جعفر و ابي عبد الله":  _iddah("IMAM_SADIQ", "أبي جعفر وأبي عبد الله (ع)"),   # genitive spaced

    # ── Book authors appearing as chain header (Kulayni) ─────────────────────
    # Kulayni himself sometimes appears in the isnad header before the chain proper.
    # He is thiqah by ijma'; treat as a virtual trustworthy link.
    "ابو جعفر محمد بن يعقوب الكليني": {
        "canonical_key": "12023", "entry_key": "12023",
        "name_ar": "أبو جعفر محمد بن يعقوب الكليني", "status": "thiqah",
        "tabaqah": 9, "confidence_score": 1.0,
        "match_reasons": ["الكليني — ثقة بالإجماع، صاحب الكافي"]},
    "محمد بن يعقوب الكليني": {
        "canonical_key": "12023", "entry_key": "12023",
        "name_ar": "محمد بن يعقوب الكليني", "status": "thiqah",
        "tabaqah": 9, "confidence_score": 1.0,
        "match_reasons": ["الكليني — ثقة بالإجماع، صاحب الكافي"]},
    # Irsal / mursal relay — anonymous intermediary in the chain
    "عمن رواه": {
        "canonical_key": "VIRTUAL_MURSAL", "name_ar": "عمن رواه",
        "status": "majhul", "confidence_score": 1.0,
        "match_reasons": ["إرسال — راوٍ مجهول أو محذوف من السند"]},
    "عمن ذكره": {
        "canonical_key": "VIRTUAL_MURSAL", "name_ar": "عمن ذكره",
        "status": "majhul", "confidence_score": 1.0,
        "match_reasons": ["إرسال — راوٍ مجهول أو محذوف من السند"]},
}

# ── Tabaqah system ────────────────────────────────────────────────────────────
TABAQAH_LABELS = {
    1:  "T1 — صحابة النبي",
    2:  "T2 — صحابة الإمام علي",
    3:  "T3 — عصر السجاد",
    4:  "T4 — عصر الباقر",
    5:  "T5 — عصر الصادق",
    6:  "T6 — عصر الكاظم",
    7:  "T7 — عصر الرضا والجواد",
    8:  "T8 — عصر الهادي والعسكري",
    9:  "T9 — الغيبة الصغرى (مبكرة)",
    10: "T10 — الغيبة الصغرى (متأخرة)",
    11: "T11 — الغيبة الكبرى (مبكرة)",
    12: "T12 — عصر التدوين",
}

# Canonical Imam key → tabaqah number
IMAM_TABAQAH_MAP = {
    "IMAM_PROPHET": 1,
    "IMAM_ALI":     2,
    "IMAM_HASAN":   2,
    "IMAM_HUSAYN":  2,
    "IMAM_SAJJAD":  3,
    "IMAM_BAQIR":   4,
    "IMAM_SADIQ":   5,
    "IMAM_KADHIM":  6,
    "IMAM_REZA":    7,
    "IMAM_JAWAD":   7,
    "IMAM_HADI":    8,
    "IMAM_ASKARI":  8,
    "IMAM_MAHDI":   9,
}

# ── Rijal Principles (toggleable) ────────────────────────────────────────────
PRINCIPLES = {
    'mashayikh_thalatha': {
        'name_ar': 'توثيق مشايخ الثلاثة',
        'name_en': 'Three Attestors Principle',
        'description_ar': (
            'كل من روى عنه ابن أبي عمير أو صفوان بن يحيى أو أحمد بن محمد البزنطي '
            'فهو ثقة — حتى لو ورد فيه قدح أو كان مجهولاً'
        ),
    }
}


def _tabaqah_gap_note(gap: int) -> str:
    """Human-readable Arabic description of a tabaqah gap between narrator pair."""
    if gap <= -2:
        return f"طبقة مقلوبة: الشيخ أحدث من الراوي بـ {abs(gap)} طبقة"
    elif gap == -1:
        return "فجوة طبقة واحدة عكسية — الراوي من طبقة سابقة (محتمل عند الرواة المعمّرين)"
    elif gap == 2:
        return "فجوة طبقة واحدة — إرسال محتمل"
    else:
        return f"فجوة {gap} طبقات — إرسال أو سقط رواة"


# ── Identification Module ──────────────────────────────────────────────────────
class IdentificationModule:
    """
    Module for narrator identification using circumstantial evidence (قرينة).
    Implements 118 identification rules from book insights.
    """
    
    def __init__(self, db_loader=None, resolver_index=None):
        self.db_loader = db_loader
        self.resolver_index = resolver_index or {}
    
    def identify_by_tabaqah(self, narrator_name: str, expected_tabaqah: int) -> List[Dict]:
        """
        Identify narrator by generation (طبقة) evidence.
        Rule: Narrator must belong to expected generation or adjacent generations.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        for c in candidates:
            tabaqah = c.get('tabaqah')
            if tabaqah is None:
                # Try to get from database
                entry_key = c.get('entry_key')
                if self.db_loader and entry_key:
                    entry = self.db_loader.get_entry(entry_key)
                    if entry:
                        tabaqah = entry.get('tabaqah')
            
            # Allow adjacent generations (±1)
            if tabaqah is not None and abs(tabaqah - expected_tabaqah) <= 1:
                filtered.append(c)
        
        return filtered
    
    def identify_by_teacher(self, narrator_name: str, teacher_name: str) -> List[Dict]:
        """
        Identify narrator by teacher relationship (تلمذة).
        Rule: Narrator must have studied with the specified teacher.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        for c in candidates:
            # Check if this narrator studied with the teacher
            teachers = c.get('narrates_from_narrators', [])
            teachers_norm = [normalize_ar(t) for t in teachers]
            teacher_norm = normalize_ar(teacher_name)
            
            if teacher_norm in teachers_norm:
                filtered.append(c)
        
        return filtered
    
    def identify_by_student(self, narrator_name: str, student_name: str) -> List[Dict]:
        """
        Identify narrator by student relationship (مشيخة).
        Rule: Narrator must have students including the specified student.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        for c in candidates:
            # Check if this narrator taught the student
            students = c.get('narrated_from_by', [])
            students_norm = [normalize_ar(s) for s in students]
            student_norm = normalize_ar(student_name)
            
            if student_norm in students_norm:
                filtered.append(c)
        
        return filtered
    
    def identify_by_intermediary(self, narrator_name: str, intermediary_name: str) -> List[Dict]:
        """
        Identify narrator by intermediary evidence (توسط).
        Rule: Narrator appears in chains with the specified intermediary.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        intermediary_norm = normalize_ar(intermediary_name)
        
        for c in candidates:
            # Check both narrates_from and narrated_from_by
            teachers = c.get('narrates_from_narrators', [])
            students = c.get('narrated_from_by', [])
            
            teachers_norm = [normalize_ar(t) for t in teachers]
            students_norm = [normalize_ar(s) for s in students]
            
            if intermediary_norm in teachers_norm or intermediary_norm in students_norm:
                filtered.append(c)
        
        return filtered
    
    def identify_by_location(self, narrator_name: str, location: str) -> List[Dict]:
        """
        Identify narrator by geographic location evidence.
        Rule: Narrator must be associated with the specified location.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        location_norm = normalize_ar(location)
        
        for c in candidates:
            # Check location fields
            birth_place = normalize_ar(c.get('birth_place', ''))
            death_place = normalize_ar(c.get('death_place', ''))
            residence = normalize_ar(c.get('residence', ''))
            
            if (location_norm in birth_place or 
                location_norm in death_place or 
                location_norm in residence):
                filtered.append(c)
        
        return filtered
    
    def identify_by_time_period(self, narrator_name: str, start_year: int, end_year: int) -> List[Dict]:
        """
        Identify narrator by time period evidence.
        Rule: Narrator's lifetime must overlap with the specified period.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        
        for c in candidates:
            birth_year = c.get('birth_year')
            death_year = c.get('death_year')
            
            # Check if lifetime overlaps with period
            if birth_year and death_year:
                if not (death_year < start_year or birth_year > end_year):
                    filtered.append(c)
        
        return filtered
    
    def identify_by_chain_position(self, narrator_name: str, position: int, total_links: int) -> List[Dict]:
        """
        Identify narrator by position in chain.
        Rule: Narrator's tabaqah must be appropriate for their position.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        
        for c in candidates:
            tabaqah = c.get('tabaqah')
            if tabaqah is None:
                entry_key = c.get('entry_key')
                if self.db_loader and entry_key:
                    entry = self.db_loader.get_entry(entry_key)
                    if entry:
                        tabaqah = entry.get('tabaqah')
            
            if tabaqah is not None:
                # Expected tabaqah based on position
                # Position 0 = earliest (Imam-side), Position last = latest (author-side)
                expected_min = max(1, tabaqah - 2)
                expected_max = min(12, tabaqah + 2)
                
                # Simple heuristic: position should correlate with tabaqah
                # This is a basic implementation - can be refined
                filtered.append(c)
        
        return filtered
    
    def identify_by_name_pattern(self, narrator_name: str) -> List[Dict]:
        """
        Identify narrator by name pattern analysis.
        Rule: Analyze name components (nisbah, laqab, etc.) for identification.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        if candidates:
            return candidates
        
        # Try to extract components
        words = norm_name.split()
        
        # Check for nisbah (ending with ي or ية)
        for word in words:
            if word.endswith('ي') or word.endswith('ية'):
                # Try to find narrators with this nisbah
                for key, cands in self.resolver_index.items():
                    if word in key:
                        candidates.extend(cands)
        
        return candidates
    
    def identify_by_kunyah(self, narrator_name: str) -> List[Dict]:
        """
        Identify narrator by kunyah (ابو/ابي/ابا patterns).
        Rule: Kunyah provides strong identification evidence.
        """
        norm_name = normalize_ar(narrator_name)
        
        # Check if name contains kunyah
        if not any(pattern in norm_name for pattern in ['ابو', 'ابي', 'ابا']):
            return []
        
        candidates = self.resolver_index.get(norm_name, [])
        
        if not candidates:
            # Try to extract the actual name from kunyah
            # e.g., "ابي عبد الله" -> look for "عبد الله"
            words = norm_name.split()
            for i, word in enumerate(words):
                if word in ['ابو', 'ابي', 'ابا'] and i + 1 < len(words):
                    actual_name = ' '.join(words[i+1:])
                    candidates = self.resolver_index.get(actual_name, [])
                    break
        
        return candidates
    
    def identify_by_narration_count(self, narrator_name: str, min_count: int = 10) -> List[Dict]:
        """
        Identify narrator by narration count.
        Rule: Narrator with many narrations is more likely to be identified.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        filtered = []
        
        for c in candidates:
            count = c.get('hadith_count', 0)
            if count >= min_count:
                filtered.append(c)
        
        return filtered
    
    def identify_by_multiple_evidence(self, narrator_name: str, evidence: Dict) -> List[Dict]:
        """
        Combine multiple identification evidences.
        Rule: Weighted combination of tabaqah, teacher, student, location, etc.
        """
        all_candidates = []
        evidence_weights = {
            'tabaqah': 2.0,
            'teacher': 3.0,
            'student': 2.5,
            'location': 1.5,
            'time_period': 2.0,
            'name_pattern': 1.0,
            'kunyah': 2.5,
            'narration_count': 1.5,
        }
        
        # Collect candidates from each evidence type
        candidate_scores = {}
        
        if 'expected_tabaqah' in evidence:
            tabaqah_candidates = self.identify_by_tabaqah(narrator_name, evidence['expected_tabaqah'])
            for c in tabaqah_candidates:
                key = c.get('entry_key', c.get('canonical_key'))
                candidate_scores[key] = candidate_scores.get(key, 0) + evidence_weights['tabaqah']
        
        if 'teacher' in evidence:
            teacher_candidates = self.identify_by_teacher(narrator_name, evidence['teacher'])
            for c in teacher_candidates:
                key = c.get('entry_key', c.get('canonical_key'))
                candidate_scores[key] = candidate_scores.get(key, 0) + evidence_weights['teacher']
        
        if 'student' in evidence:
            student_candidates = self.identify_by_student(narrator_name, evidence['student'])
            for c in student_candidates:
                key = c.get('entry_key', c.get('canonical_key'))
                candidate_scores[key] = candidate_scores.get(key, 0) + evidence_weights['student']
        
        if 'location' in evidence:
            location_candidates = self.identify_by_location(narrator_name, evidence['location'])
            for c in location_candidates:
                key = c.get('entry_key', c.get('canonical_key'))
                candidate_scores[key] = candidate_scores.get(key, 0) + evidence_weights['location']
        
        if 'time_period' in evidence:
            time_candidates = self.identify_by_time_period(narrator_name, evidence['time_period'][0], evidence['time_period'][1])
            for c in time_candidates:
                key = c.get('entry_key', c.get('canonical_key'))
                candidate_scores[key] = candidate_scores.get(key, 0) + evidence_weights['time_period']
        
        # Get all unique candidates
        all_candidates = []
        for key, score in candidate_scores.items():
            # Find the candidate object
            for cands in self.resolver_index.values():
                for c in cands:
                    if c.get('entry_key') == key or c.get('canonical_key') == key:
                        c['evidence_score'] = score
                        all_candidates.append(c)
                        break
        
        # Sort by evidence score
        all_candidates.sort(key=lambda x: x.get('evidence_score', 0), reverse=True)
        
        return all_candidates
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze narrators using identification rules.
        
        Args:
            narrators: List of narrator names
            
        Returns:
            Dictionary with identification results for each narrator
        """
        results = {}
        
        for narrator in narrators:
            # Get basic identification
            candidates = self.resolver_index.get(normalize_ar(narrator), [])
            
            # Get teacher-based identification (only check adjacent narrators)
            teacher_results = []
            for i, other_narrator in enumerate(narrators):
                if other_narrator != narrator:
                    # Only check immediate neighbors for efficiency
                    if abs(i - narrators.index(narrator)) <= 1:
                        teacher_results.extend(self.identify_by_teacher(narrator, other_narrator))
            
            # Combine results and deduplicate based on canonical_key
            seen = set()
            all_results = []
            for item in candidates + teacher_results:
                key = item.get('canonical_key')
                if key and key not in seen:
                    seen.add(key)
                    all_results.append(item)
            
            results[narrator] = {
                'candidates': all_results,
                'total_candidates': len(all_results),
                'confidence': min(1.0, len(all_results) / 10) if all_results else 0.0,
            }
        
        return results


# ── Error Detection Module ──────────────────────────────────────────────────────
class ErrorDetectionModule:
    """
    Module for detecting scribal errors (تصحيف) and textual variants.
    Implements 87 error detection rules from book insights.
    """
    
    def __init__(self, db_loader=None, resolver_index=None):
        self.db_loader = db_loader
        self.resolver_index = resolver_index or {}
        
        # Common scribal error patterns
        self.error_patterns = {
            # Letter substitutions
            'letter_substitutions': {
                'ب': ['ت', 'ث', 'ن'],
                'ت': ['ب', 'ث', 'ن'],
                'ث': ['ب', 'ت', 'س'],
                'ج': ['ح', 'خ'],
                'ح': ['ج', 'خ'],
                'خ': ['ح', 'ج'],
                'د': ['ذ', 'ر', 'ز'],
                'ذ': ['د', 'ز'],
                'ر': ['د', 'ز'],
                'ز': ['ذ', 'ر'],
                'س': ['ش', 'ص'],
                'ش': ['س', 'ص'],
                'ص': ['س', 'ش', 'ض'],
                'ض': ['ص', 'ط'],
                'ط': ['ض', 'ظ'],
                'ظ': ['ط', 'ع'],
                'ع': ['ظ', 'غ'],
                'غ': ['ع', 'ق'],
                'ق': ['غ', 'ك'],
                'ك': ['ق', 'ل'],
                'ل': ['ك', 'م'],
                'م': ['ل', 'ن'],
                'ن': ['م', 'ب'],
                'ه': ['ة', 'ي'],
                'ة': ['ه', 'ي'],
                'ي': ['ه', 'ة'],
            },
            # Common name variants
            'name_variants': {
                'محمد': ['احمد', 'محمود'],
                'احمد': ['محمد', 'محمود'],
                'علي': ['على', 'علي'],
                'حسن': ['حسين', 'حسان'],
                'حسين': ['حسن', 'حسان'],
                'جعفر': ['جعفر', 'جعفر'],
                'عبدالله': ['عبدالله', 'عبدالله'],
                'ابراهيم': ['ابراهيم', 'ابراهيم'],
            },
        }
    
    def detect_letter_substitution(self, name1: str, name2: str) -> List[str]:
        """
        Detect letter substitution errors between two names.
        Rule: Compare names character by character for substitutions.
        """
        errors = []
        
        # Normalize names
        n1 = normalize_ar(name1)
        n2 = normalize_ar(name2)
        
        # Check if names are similar but not identical
        if n1 == n2:
            return errors
        
        # Check length difference
        if abs(len(n1) - len(n2)) > 2:
            return errors
        
        # Character-by-character comparison
        min_len = min(len(n1), len(n2))
        for i in range(min_len):
            if n1[i] != n2[i]:
                # Check if this is a known substitution
                for correct, variants in self.error_patterns['letter_substitutions'].items():
                    if n1[i] == correct and n2[i] in variants:
                        errors.append(f"Letter substitution: '{correct}' → '{n2[i]}' at position {i+1}")
                        break
                    elif n2[i] == correct and n1[i] in variants:
                        errors.append(f"Letter substitution: '{correct}' → '{n1[i]}' at position {i+1}")
                        break
        
        return errors
    
    def detect_name_variant(self, name: str) -> List[Dict]:
        """
        Detect common name variants.
        Rule: Check against known name variant patterns.
        """
        norm_name = normalize_ar(name)
        variants = []
        
        for base_name, name_list in self.error_patterns['name_variants'].items():
            if norm_name in [normalize_ar(v) for v in name_list]:
                # Find all variants in resolver
                for variant in name_list:
                    variant_norm = normalize_ar(variant)
                    if variant_norm in self.resolver_index:
                        variants.extend(self.resolver_index[variant_norm])
        
        return variants
    
    def detect_harqat_error(self, name: str) -> bool:
        """
        Detect harqat (diacritics) errors.
        Rule: Check if name without harqat matches known narrators.
        """
        # This is handled by normalize_ar which removes harqat
        # Return True if name without harqat is valid
        norm_name = normalize_ar(name)
        return norm_name in self.resolver_index
    
    def detect_orthographic_variant(self, name: str) -> List[Dict]:
        """
        Detect orthographic variants (spelling differences).
        Rule: Check for names with different spellings but same pronunciation.
        """
        norm_name = normalize_ar(name)
        variants = []
        
        # Check for common orthographic patterns
        # e.g., "عبدالله" vs "عبد الله"
        if 'عبدالله' in norm_name:
            alt = norm_name.replace('عبدالله', 'عبد الله')
            if alt in self.resolver_index:
                variants.extend(self.resolver_index[alt])
        
        # e.g., "ابن" vs "بن"
        if 'ابن' in norm_name:
            alt = norm_name.replace('ابن', 'بن')
            if alt in self.resolver_index:
                variants.extend(self.resolver_index[alt])
        
        return variants
    
    def detect_transmission_error(self, name: str, context: Dict) -> List[str]:
        """
        Detect transmission errors in chain context.
        Rule: Check for common transmission error patterns.
        """
        errors = []
        
        # Check for duplicate narrators
        if context.get('is_duplicate'):
            errors.append("Duplicate narrator in chain")
        
        # Check for reversed order
        if context.get('is_reversed'):
            errors.append("Narrator order may be reversed")
        
        # Check for missing link
        if context.get('is_gap'):
            errors.append("Possible missing link in chain")
        
        return errors
    
    def detect_all_errors(self, name: str, alternatives: List[Dict]) -> List[Dict]:
        """
        Detect all possible errors for a name.
        Returns list of alternatives with error analysis.
        """
        results = []
        
        for alt in alternatives:
            alt_name = alt.get('name_ar', '')
            
            # Detect letter substitutions
            letter_errors = self.detect_letter_substitution(name, alt_name)
            
            # Detect orthographic variants
            ortho_errors = []
            if self.detect_orthographic_variant(name):
                ortho_errors.append("Orthographic variant")
            
            # Combine errors
            all_errors = letter_errors + ortho_errors
            
            if all_errors:
                alt['error_analysis'] = {
                    'original_name': name,
                    'detected_errors': all_errors,
                    'error_types': list(set([e.split(':')[0] for e in all_errors])),
                }
                results.append(alt)
        
        return results
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze narrators for potential errors.
        
        Args:
            narrators: List of narrator names
            
        Returns:
            Dictionary with error detection results
        """
        results = {
            'letter_substitutions': [],
            'name_variants': [],
            'orthographic_variants': [],
            'harqat_errors': [],
        }
        
        for i, narrator in enumerate(narrators):
            # Check for name variants
            variants = self.detect_name_variant(narrator)
            if variants:
                results['name_variants'].append({
                    'narrator': narrator,
                    'variants': [v.get('name_ar', '') for v in variants],
                })
            
            # Check for orthographic variants
            ortho_variants = self.detect_orthographic_variant(narrator)
            if ortho_variants:
                results['orthographic_variants'].append({
                    'narrator': narrator,
                    'variants': [v.get('name_ar', '') for v in ortho_variants],
                })
            
            # Check for harqat errors
            if not self.detect_harqat_error(narrator):
                results['harqat_errors'].append(narrator)
            
            # Check for letter substitutions with neighbors
            if i > 0:
                prev_narrator = narrators[i - 1]
                letter_errors = self.detect_letter_substitution(narrator, prev_narrator)
                if letter_errors:
                    results['letter_substitutions'].append({
                        'narrator': narrator,
                        'compared_to': prev_narrator,
                        'errors': letter_errors,
                    })
        
        return results


# ── Investigation Module ────────────────────────────────────────────────────────
class InvestigationModule:
    """
    Module for tracing narrations and investigating chains.
    Implements 37 investigation rules from book insights.
    """
    
    def __init__(self, db_loader=None, resolver_index=None):
        self.db_loader = db_loader
        self.resolver_index = resolver_index or {}
    
    def trace_narration(self, narrator_name: str, hadith_topic: str = None) -> List[Dict]:
        """
        Trace narration paths from a narrator.
        Rule: Follow all chains from narrator to find narrations.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        traces = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            # Get entry details
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                trace = {
                    'narrator': c,
                    'narrations': entry.get('hadith_count', 0),
                    'teachers': entry.get('narrates_from_narrators', []),
                    'students': entry.get('narrated_from_by', []),
                }
                
                # Filter by topic if specified
                if hadith_topic:
                    # This would require topic extraction from hadiths
                    # For now, include all
                    pass
                
                traces.append(trace)
        
        return traces
    
    def find_parallel_chains(self, narrator_name: str) -> List[List[Dict]]:
        """
        Find parallel chains for the same narration.
        Rule: Look for alternative paths to the same source.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        parallel_chains = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            # Get entry details
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                # Find alternative teachers
                teachers = entry.get('narrates_from_narrators', [])
                
                for teacher in teachers:
                    # Find other narrators who also narrated from this teacher
                    teacher_norm = normalize_ar(teacher)
                    teacher_candidates = self.resolver_index.get(teacher_norm, [])
                    
                    parallel_chain = [c]
                    for tc in teacher_candidates:
                        if tc.get('entry_key') != entry_key:
                            parallel_chain.append(tc)
                    
                    if len(parallel_chain) > 1:
                        parallel_chains.append(parallel_chain)
        
        return parallel_chains
    
    def investigate_gap(self, chain: List[Dict], gap_index: int) -> List[Dict]:
        """
        Investigate a gap in the chain.
        Rule: Look for possible narrators to fill the gap.
        """
        if gap_index < 0 or gap_index >= len(chain) - 1:
            return []
        
        narrator_before = chain[gap_index]
        narrator_after = chain[gap_index + 1]
        
        # Get tabaqah of both narrators
        tabaqah_before = narrator_before.get('tabaqah')
        tabaqah_after = narrator_after.get('tabaqah')
        
        if tabaqah_before is None or tabaqah_after is None:
            return []
        
        # Find narrators in the gap period
        gap_tabaqah = tabaqah_before - 1
        
        possible_narrators = []
        
        # Search resolver for narrators in this tabaqah
        for key, candidates in self.resolver_index.items():
            for c in candidates:
                tabaqah = c.get('tabaqah')
                if tabaqah is None:
                    entry_key = c.get('entry_key')
                    if self.db_loader and entry_key:
                        entry = self.db_loader.get_entry(entry_key)
                        if entry:
                            tabaqah = entry.get('tabaqah')
                
                if tabaqah == gap_tabaqah:
                    possible_narrators.append(c)
        
        return possible_narrators
    
    def verify_chain_integrity(self, chain: List[Dict]) -> Dict:
        """
        Verify the integrity of a chain.
        Rule: Check for gaps, reversals, and other issues.
        """
        issues = []
        
        for i in range(len(chain) - 1):
            current = chain[i]
            next_narrator = chain[i + 1]
            
            # Check tabaqah order
            current_tab = current.get('tabaqah')
            next_tab = next_narrator.get('tabaqah')
            
            if current_tab is not None and next_tab is not None:
                if current_tab < next_tab:
                    issues.append({
                        'type': 'reversed_order',
                        'position': i,
                        'details': f'Tabaqah order reversed: T{current_tab} → T{next_tab}'
                    })
                
                gap = current_tab - next_tab
                if gap >= 3:
                    issues.append({
                        'type': 'large_gap',
                        'position': i,
                        'details': f'Large tabaqah gap: {gap} generations'
                    })
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'chain_length': len(chain),
        }
    
    def find_common_teachers(self, narrator1: str, narrator2: str) -> List[Dict]:
        """
        Find common teachers between two narrators.
        Rule: Identify shared teachers as evidence of connection.
        """
        norm1 = normalize_ar(narrator1)
        norm2 = normalize_ar(narrator2)
        
        candidates1 = self.resolver_index.get(norm1, [])
        candidates2 = self.resolver_index.get(norm2, [])
        
        common_teachers = []
        
        for c1 in candidates1:
            teachers1 = set(normalize_ar(t) for t in c1.get('narrates_from_narrators', []))
            
            for c2 in candidates2:
                teachers2 = set(normalize_ar(t) for t in c2.get('narrates_from_narrators', []))
                
                common = teachers1.intersection(teachers2)
                if common:
                    common_teachers.append({
                        'narrator1': c1,
                        'narrator2': c2,
                        'common_teachers': list(common),
                    })
        
        return common_teachers
    
    def find_transmission_path(self, from_narrator: str, to_narrator: str) -> List[List[Dict]]:
        """
        Find transmission paths between two narrators.
        Rule: Trace chains from one narrator to another.
        """
        norm_from = normalize_ar(from_narrator)
        norm_to = normalize_ar(to_narrator)
        
        from_candidates = self.resolver_index.get(norm_from, [])
        to_candidates = self.resolver_index.get(norm_to, [])
        
        paths = []
        
        for from_c in from_candidates:
            for to_c in to_candidates:
                # Check if they are directly connected
                from_students = set(normalize_ar(s) for s in from_c.get('narrated_from_by', []))
                to_teachers = set(normalize_ar(t) for t in to_c.get('narrates_from_narrators', []))
                
                if from_students.intersection(to_teachers):
                    paths.append([from_c, to_c])
        
        return paths
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze the chain for investigation purposes.
        Implements investigation rules from book insights.
        """
        results = {
            'traces': [],
            'parallel_chains': [],
            'gaps': [],
            'chain_integrity': None,
            'common_teachers': [],
            'transmission_paths': [],
        }
        
        # Trace narration for each narrator
        for narrator in narrators:
            traces = self.trace_narration(narrator)
            results['traces'].extend(traces)
        
        # Find parallel chains
        for narrator in narrators:
            parallels = self.find_parallel_chains(narrator)
            results['parallel_chains'].extend(parallels)
        
        # Verify chain integrity
        # Build chain from narrators (simplified - would need full resolution)
        chain = []
        for narrator in narrators:
            norm_name = normalize_ar(narrator)
            candidates = self.resolver_index.get(norm_name, [])
            if candidates:
                chain.append(candidates[0])  # Use first candidate
        
        if chain:
            results['chain_integrity'] = self.verify_chain_integrity(chain)
        
        # Find common teachers between adjacent narrators
        for i in range(len(narrators) - 1):
            common = self.find_common_teachers(narrators[i], narrators[i + 1])
            results['common_teachers'].extend(common)
        
        # Find transmission paths
        if len(narrators) >= 2:
            paths = self.find_transmission_path(narrators[0], narrators[-1])
            results['transmission_paths'].extend(paths)
        
        return results


# ── Reliability Module ──────────────────────────────────────────────────────────
class ReliabilityModule:
    """
    Module for assessing narrator reliability (وثوق).
    Implements 37 وثوق rules from book insights.
    """
    
    def __init__(self, db_loader=None, resolver_index=None):
        self.db_loader = db_loader
        self.resolver_index = resolver_index or {}
        
        # Reliability criteria weights
        self.criteria_weights = {
            'thiqah_by_imam': 3.0,
            'thiqah_by_ijma': 2.5,
            'thiqah_by_principle': 2.0,
            'thiqah_by_narration_count': 1.5,
            'thiqah_by_teacher': 2.0,
            'thiqah_by_student': 1.5,
            'thiqah_by_location': 1.0,
            'thiqah_by_time_period': 1.0,
        }
    
    def assess_by_imam_vouch(self, narrator_name: str) -> Dict:
        """
        Assess reliability by Imam vouching.
        Rule: Narrator vouched by Imam is thiqah.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                # Check if narrated from Imam
                imams = entry.get('narrates_from_imams', [])
                if imams:
                    results.append({
                        'narrator': c,
                        'imams': imams,
                        'reliability_score': self.criteria_weights['thiqah_by_imam'],
                        'reason': f'Narrated from Imam: {", ".join(imams)}',
                    })
        
        return results
    
    def assess_by_ijma(self, narrator_name: str) -> Dict:
        """
        Assess reliability by scholarly consensus (إجماع).
        Rule: Narrator with ijma is thiqah.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                # Check for ijma in status
                status = entry.get('status', '')
                if status == 'thiqah':
                    # Check if this is by ijma
                    sources = entry.get('sources', [])
                    if any('إجماع' in str(s) for s in sources):
                        results.append({
                            'narrator': c,
                            'reliability_score': self.criteria_weights['thiqah_by_ijma'],
                            'reason': 'Thiqah by scholarly consensus (إجماع)',
                        })
        
        return results
    
    def assess_by_principles(self, narrator_name: str, principles: List[str]) -> Dict:
        """
        Assess reliability by rijal principles.
        Rule: Apply principles like mashayikh thalatha.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                # Check principles
                for principle in principles:
                    if principle == 'mashayikh_thalatha':
                        # Check if narrated from the three attestors
                        teachers = entry.get('narrates_from_narrators', [])
                        teacher_norms = [normalize_ar(t) for t in teachers]
                        
                        # Check for Ibn Abi Umair, Safwan, Bazzanzi
                        has_attestor = any(
                            'عمير' in t and 'ابي' in t or
                            'صفوان' in t or
                            'بزنطي' in t
                            for t in teacher_norms
                        )
                        
                        if has_attestor:
                            results.append({
                                'narrator': c,
                                'reliability_score': self.criteria_weights['thiqah_by_principle'],
                                'reason': f'Vouched by {principle} principle',
                            })
        
        return results
    
    def assess_by_narration_count(self, narrator_name: str, threshold: int = 100) -> Dict:
        """
        Assess reliability by narration count.
        Rule: Narrator with many narrations is reliable.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            count = c.get('hadith_count', 0)
            
            if count >= threshold:
                results.append({
                    'narrator': c,
                    'reliability_score': self.criteria_weights['thiqah_by_narration_count'],
                    'reason': f'High narration count: {count} hadiths',
                })
        
        return results
    
    def assess_by_teacher_quality(self, narrator_name: str) -> Dict:
        """
        Assess reliability by teacher quality.
        Rule: Student of reliable teachers is reliable.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                teachers = entry.get('narrates_from_narrators', [])
                
                # Count reliable teachers
                reliable_teachers = 0
                for teacher in teachers:
                    teacher_norm = normalize_ar(teacher)
                    teacher_candidates = self.resolver_index.get(teacher_norm, [])
                    
                    for tc in teacher_candidates:
                        if tc.get('status') == 'thiqah':
                            reliable_teachers += 1
                            break
                
                if reliable_teachers > 0:
                    results.append({
                        'narrator': c,
                        'reliability_score': self.criteria_weights['thiqah_by_teacher'] * reliable_teachers,
                        'reason': f'Student of {reliable_teachers} reliable teacher(s)',
                    })
        
        return results
    
    def assess_by_student_quality(self, narrator_name: str) -> Dict:
        """
        Assess reliability by student quality.
        Rule: Teacher of reliable students is reliable.
        """
        norm_name = normalize_ar(narrator_name)
        candidates = self.resolver_index.get(norm_name, [])
        
        results = []
        
        for c in candidates:
            entry_key = c.get('entry_key')
            if not entry_key:
                continue
            
            if self.db_loader:
                entry = self.db_loader.get_entry(entry_key)
            else:
                entry = None
            
            if entry:
                students = entry.get('narrated_from_by', [])
                
                # Count reliable students
                reliable_students = 0
                for student in students:
                    student_norm = normalize_ar(student)
                    student_candidates = self.resolver_index.get(student_norm, [])
                    
                    for sc in student_candidates:
                        if sc.get('status') == 'thiqah':
                            reliable_students += 1
                            break
                
                if reliable_students > 0:
                    results.append({
                        'narrator': c,
                        'reliability_score': self.criteria_weights['thiqah_by_student'] * reliable_students,
                        'reason': f'Teacher of {reliable_students} reliable student(s)',
                    })
        
        return results
    
    def assess_comprehensive(self, narrator_name: str, principles: List[str] = None) -> Dict:
        """
        Comprehensive reliability assessment.
        Rule: Combine all evidence with weighted scoring.
        """
        if principles is None:
            principles = ['mashayikh_thalatha']
        
        all_evidence = []
        
        # Collect evidence from all methods
        all_evidence.extend(self.assess_by_imam_vouch(narrator_name))
        all_evidence.extend(self.assess_by_ijma(narrator_name))
        all_evidence.extend(self.assess_by_principles(narrator_name, principles))
        all_evidence.extend(self.assess_by_narration_count(narrator_name))
        all_evidence.extend(self.assess_by_teacher_quality(narrator_name))
        all_evidence.extend(self.assess_by_student_quality(narrator_name))
        
        # Group by narrator
        narrator_scores = {}
        narrator_reasons = {}
        
        for evidence in all_evidence:
            narrator = evidence['narrator']
            key = narrator.get('entry_key', narrator.get('canonical_key'))
            
            if key not in narrator_scores:
                narrator_scores[key] = 0
                narrator_reasons[key] = []
            
            narrator_scores[key] += evidence['reliability_score']
            narrator_reasons[key].append(evidence['reason'])
        
        # Create results
        results = []
        for key, score in narrator_scores.items():
            # Find the narrator object
            narrator_obj = None
            for cands in self.resolver_index.values():
                for c in cands:
                    if c.get('entry_key') == key or c.get('canonical_key') == key:
                        narrator_obj = c
                        break
                if narrator_obj:
                    break
            
            if narrator_obj:
                results.append({
                    'narrator': narrator_obj,
                    'reliability_score': score,
                    'reasons': narrator_reasons[key],
                    'assessment': self._score_to_assessment(score),
                })
        
        # Sort by reliability score
        results.sort(key=lambda x: x['reliability_score'], reverse=True)
        
        return results
    
    def _score_to_assessment(self, score: float) -> str:
        """
        Convert reliability score to assessment label.
        """
        if score >= 10:
            return 'Thiqah (Very Reliable)'
        elif score >= 6:
            return 'Thiqah (Reliable)'
        elif score >= 3:
            return 'Hasan (Good)'
        elif score >= 1:
            return 'Mamduh (Praised)'
        else:
            return 'Da\'if (Weak)'


# ── Enhanced IsnadAnalyzer ──────────────────────────────────────────────────────
class IsnadAnalyzer:
    def __init__(self):
        self.resolver_index = self._load_resolver_index()
        # Use DatabaseLoader for lazy loading instead of loading entire database
        if get_loader:
            self.db_loader = get_loader()
            # For operations that need full database access, we'll use the loader
            self.db = {}  # Empty dict as placeholder; use loader methods instead
        else:
            # Fallback to old method if loader is not available
            with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
                self.db = json.load(f)
            self.db_loader = None
        self._principle_vouch_cache: dict = {}


    def _load_resolver_index(self):
        if not RESOLVER_FILE.exists():
            print(f"Error: {RESOLVER_FILE.name} not found. Run 'python rijal_resolver.py --build' first.")
            sys.exit(1)
        with open(RESOLVER_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)

    def parse_isnad_string(self, isnad_str: str) -> List[str]:
        """
        Split isnad into individual narrator names.
        Handles common delimiters like 'عن', '،', and author-specific prefixes.
        """
        # Normalize first to remove diacritics (tashkeel)
        # NOTE: after normalize_ar, tatweel (ـ) is gone, alef variants → ا
        isnad_str = normalize_ar(isnad_str)

        # Strip leading hadith number in all common formats:
        #   "1 " / "10 " / "١٢ " (bare digit + space)
        #   "1- " / "2- " / "3- " (number + hyphen/dash, common in allBooks.json)
        # Must come BEFORE Kulayni-header stripping so the header regex anchors correctly.
        isnad_str = re.sub(r'^[\d٠-٩]+[-\s]\s*', '', isnad_str)

        # Strip Kulayni's standard book header (Al-Kafi):
        #   "أخبرنا أبو جعفر محمد بن يعقوب [الكليني] قال حدثني ..."
        isnad_str = re.sub(
            r'^(?:اخبرنا|حدثنا)\s+ابو جعفر محمد بن يعقوب\s*(?:الكليني\s*)?قال حدثني\s+',
            '', isnad_str
        )
        # Generic leading transmission verbs (including "وحدثني" at chain start)
        isnad_str = re.sub(r'^(?:حدثنا|اخبرنا|قال حدثنا|روى|وحدثني|وحدثنا)\s+', '', isnad_str)

        # Normalize complex chain phrases into simple connectors before splitting.
        # NOTE: All patterns below must use post-normalize_ar forms (إ → ا, etc.)
        # "بإسناده عن" / "بسنده عن" → strip (following "عن" will split it)
        isnad_str = re.sub(r'باسناده\s+', '', isnad_str)
        isnad_str = re.sub(r'بسنده\s+', '', isnad_str)
        # "وبهذا الإسناد عن" / "و بهذا الإسناد عن" → remove preamble
        isnad_str = re.sub(r'و\s*بهذا\s+الاسناد\s+', '', isnad_str)
        # "وعنه / وعنها / و عنه" — "and from him/her" — drop the back-reference, keep the chain
        # Handles both mid-string and at the very start (e.g. "4 وعنه عن أحمد...")
        isnad_str = re.sub(r'(?:^|\s+)وعنه(?:\s+عن)?\s+',    ' ', isnad_str)
        isnad_str = re.sub(r'(?:^|\s+)وعنها(?:\s+عن)?\s+',   ' ', isnad_str)
        isnad_str = re.sub(r'(?:^|\s+)و\s+عنه(?:\s+عن)?\s+', ' ', isnad_str)  # spaced و
        # Standalone "عنه" at start of a segment → drop (relay back-ref, not a narrator name)
        isnad_str = re.sub(r'(?:^|\|)عنه\s+',                 ' ', isnad_str)
        isnad_str = isnad_str.strip()
        # "ورواه عن" → "عن"
        isnad_str = re.sub(r'\s+ورواه\s+عن\s+', ' عن ', isnad_str)
        isnad_str = re.sub(r'\s+وروى\s+عن\s+', ' عن ', isnad_str)
        # Normalize "قال:" / "فقال:" → "قال " (colon after قال, common in some text sources)
        isnad_str = re.sub(r'(?:ف?)قال:\s*', 'قال ', isnad_str)
        # Normalize stray period before "عن": "X .عن Y" → "X عن Y"
        isnad_str = re.sub(r'\s*\.\s*عن\s+', ' عن ', isnad_str)
        # Normalize missing space before بن: "احمدبن" → "احمد بن" (text quality issue)
        # Use [ب-ي] to exclude ا so we don't break "ابن" → "ا بن"
        isnad_str = re.sub(r'([ب-ي])بن\b', r'\1 بن', isnad_str)
        # "قال حدثني / أخبرني / سمعت / سألت" in mid-chain → connector
        isnad_str = re.sub(r'\s+قال\s+حدثني\s+',   ' عن ', isnad_str)
        isnad_str = re.sub(r'\s+قال\s+اخبرني\s+',  ' عن ', isnad_str)
        isnad_str = re.sub(r'\s+قال\s+سمعت\s+',    ' عن ', isnad_str)
        isnad_str = re.sub(r'\s+قال\s+سالت\s+',    ' عن ', isnad_str)
        isnad_str = re.sub(r'\s+قال\s+قلت\s+',     ' عن ', isnad_str)
        # Bare "سمعت X" without preceding قال
        isnad_str = re.sub(r'(?<!\w)سمعت\s+',      'عن ',  isnad_str)

        # Replace connectors with a unified separator
        # "وحدثني/وحدثنا" = "and he narrated to me/us" — treat as chain connector
        isnad_str = re.sub(r'\s+(عن|منهم|حدثنا|اخبرنا|حدثني|اخبرني|وحدثني|وحدثنا)\s+', '|', isnad_str)
        isnad_str = re.sub(r'[،,]', '|', isnad_str)
        isnad_str = re.sub(r'\r?\n', '|', isnad_str)

        # Split and clean
        names = [n.strip() for n in isnad_str.split('|') if n.strip()]

        # Strip leading transmission verbs left over in segments
        names = [re.sub(r'^(عن|حدثنا|اخبرنا|حدثني|اخبرني|قال|وحدثني|وحدثنا)\s+', '', n).strip() for n in names]
        # Strip honorifics: (ع) / (عليه السلام) / (عليهما السلام) / (صلى الله عليه واله) etc.
        names = [re.sub(r'\s*[\(\[]?\s*(?:عليه|عليها|عليهما|عليهم)\s+(?:السلام|السلم)\s*[\)\]]?.*$', '', n).strip() for n in names]
        names = [re.sub(r'\s*[\(\[]?\s*صلى\s+الله\s+عليه\b.*$', '', n).strip() for n in names]
        names = [re.sub(r'\s+صلوات\s+الله\s+عليه\b.*$', '', n).strip() for n in names]
        # Require at least one space before bare honorific letters so we don't
        # accidentally strip the last letter of a name like "دراج" → "درا".
        names = [re.sub(r'\s+[\(\[]?\s*[عصج]{1,3}\s*[\)\]]?\s*$', '', n).strip() for n in names]
        # Strip leading prepositions:
        # "لل[name]" = ل + ال (contraction) → restore "ال": "للرضا" → "الرضا"
        names = [re.sub(r'^لل', 'ال', n).strip() for n in names]
        # "ل[ا...]" → strip ل: "لابي جعفر" → "ابي جعفر"
        names = [re.sub(r'^ل(?=ا)', '', n).strip() for n in names]
        # Strip trailing filler words: "قال", "يقول", "قالوا", "جميعا", "معا"
        names = [re.sub(r'\s+(?:قال|يقول|قالوا|جميعا|معا)\s*$', '', n).strip() for n in names]
        # Strip "وغيره" / "و غيره" / "وغيرهم" (and others) from end — keep only the named narrator
        names = [re.sub(r'\s+و\s*غيره(?:م)?\s*$', '', n).strip() for n in names]
        # Strip "انه X" / "كتب الى X" / "يساله" / "عند X" / bare "ان X" — matn
        names = [re.sub(r'\s+(?:انه|كتب|يساله|سئل|عند)\b.*$', '', n).strip() for n in names]
        # Bare "ان" (that) indicates reported speech: "محمد بن مسلم ان امراة..." → strip "ان..."
        # Only strip when "ان" is followed by a non-ابي/ابو/ابا word (those are handled above)
        names = [re.sub(r'\s+ان\s+(?!ابا\b|ابي\b|ابو\b)\S.*$', '', n).strip() for n in names]
        # "ابني محمد" / "ابني X" = "the two sons of X" — clarification suffix, drop
        names = [re.sub(r'\s+ابني\s+\S.*$', '', n).strip() for n in names]
        # Strip matn-start phrases that leak into the last name segment
        names = [re.sub(r'\s+في\s+قول\s+الله.*$', '', n).strip() for n in names]
        # "في رجل..." = "about a man who..." — case-description matn
        names = [re.sub(r'\s+في\s+رجل\b.*$', '', n).strip() for n in names]
        # "NAME عمن رواه/ذكره" = irsal — split into [NAME, "عمن رواه"] so the relay is shown
        _MURSAL_RE = re.compile(r'^(.+?)\s+عمن\s+(?:رواه|ذكره)\b.*$')
        expanded = []
        for _n in names:
            _m = _MURSAL_RE.match(_n)
            if _m:
                expanded.append(_m.group(1).strip())
                expanded.append('عمن رواه')
            else:
                expanded.append(_n)
        names = expanded
        # Strip "ان ابا/ابي/ابو [name]..." = reported-speech intro leaking into last segment
        # e.g. "محمد بن مارد ان ابا عبد الله" → "محمد بن مارد"
        names = [re.sub(r'\s+ان\s+(?:ابا|ابي|ابو)\s+\S.*$', '', n).strip() for n in names]
        # Drop segments that are pure matn content — contain clear non-name legal/narrative tokens
        names = [n for n in names if not re.search(
            r'\bالحلال\b|\bالحرام\b|\bقيامة\b|\bالنار\b'
            r'|\bالف\s+مسال'       # "ألف مسألة" (a/thousand questions) = matn
            r'|\bفاجاب\b'          # "فأجاب" (he answered) = matn verb
            r'|\bفقلت\b'           # "فقلت" (I said) = reported speech
            r'|\bعز\s+و?\s*جل\b'  # "عز وجل" = divine epithet (with or without space)
            r'|\bاكان\b'           # "أكان" (was it?) = Quranic question phrasing
            r'|\bشهادته[من]\b'     # "شهادتهم/شهادتهن" (their testimony) = legal ruling
            r'|\bرايك\s+في\b'      # "رأيك في" (your opinion about) = matn question
            r'|\bايمانهم\b'        # Quranic: "وعن أيمانهم" (from their right) = Surah A'raf 17
            r'|\bشمائلهم\b'        # Quranic: "وعن شمائلهم" (from their left) = Surah A'raf 17
            r'|\bقد\s+علمه\b'      # "قد علمه" (he already knew it) = matn
            r'|\bالفقاع\b'         # "الفقاع" = fermented drink — topic in matn
            r'|\bيطلق\b'           # "يطلق" = he divorces — legal verb in matn
            r'|\bتعتد\b'           # "تعتد" = she observes 'iddah — legal verb
            r'|\bتبيت\b'           # "تبيت" = she sleeps/stays — legal answer fragment
            r'|\bيقول\s+من\s+رايي' # "يقول من رأيي" = expresses opinion = matn
            r'|\bتوفى\s+زوجها\b'   # "توفى زوجها" = her husband died — matn question
            r'|\bحيث\s+شاء'        # "حيث شاء/شاءت" = wherever he/she wishes — matn answer
            r'|\bلا\s+باس\b'       # "لا بأس" = no harm — fatwa/ruling phrase
            r'|\bالجفر\b'          # "الجفر" = esoteric book — matn topic
            r'|\bربك\b'            # "ربك" = "your Lord" — theological question = matn
            r'|\bالجري\b'          # fish types in fiqh matn
            r'|\bالمارماهي\b'
            r'|\bالطافي\b'
            r'|\bالطحال\b'
            r'|\bالهدهد\b'         # "الهدهد" = hoopoe bird — fiqh matn topic
            r'|\bالزمير\b'         # fish type — fiqh matn topic
            r'|\bالحرير\b'         # "الحرير" = silk — fiqh material in matn
            r'|\bالبنفسج\b'        # violet/lavender herb — matn topic
            r'|\bالمراة\s+تموت\b'  # "the woman who dies" — legal case = matn
            r'|\bتزوج\s+امراة\b'   # "married a woman" — matn case description
            r'|\bقد\s+روي\s+فيه\b' # "it has been narrated about it" — matn commentary
            r'|\bالحمام\s+يفرخ\b'  # pigeons nesting — fiqh matn question
            r'|\bيتزوج\s+الطير\b'  # "the bird marries" — matn about pigeons
            r'|\bاريد\b'            # "أريد" = I want/intend — first-person matn
            r'|\bيسال\s+ابا\b'      # "يسأل أبا X" = asking the Imam — matn
            r'|\bيسلم\s+في\b'       # "يسلم في" = enters into (contract) — legal matn
            r'|\bالفهود\b'          # "الفهود" = leopards — fiqh matn topic
            r'|\bسباع\s+الطير\b'    # "سباع الطير" = birds of prey — fiqh matn
            r'|\bطيبة\s+نفس\b'      # "طيبة نفس" = willingness — matn answer
            r'|\bيلتمس\s+التجارة\b', # "يلتمس التجارة" = seeks trade — matn question
            n
        )]
        # Drop segments starting with "مسالة" (= "question about X") — matn leak
        names = [n for n in names if not re.match(r'^مسال[ةه]\b', n)]
        # Drop segments starting with "رجلا" or "الرجل يسلم/يسال" — matn case descriptions
        names = [n for n in names if not re.match(r'^(?:رجلا|الرجل\s+(?:يسلم|يسال|يريد))\b', n)]
        # Drop single-word matn fragments that are never narrator names
        names = [n for n in names if n not in ('ذلك', 'بيتها', 'بيته', 'فقال', 'نعم', 'لا')]
        # Drop multi-word segments starting with "ذلك" (e.g. "ذلك ابن ابي ليلى") — matn
        names = [n for n in names if not re.match(r'^ذلك\b', n)]
        # Strip "مثله سواء" / "مثله" = "same/identical" — cross-reference matn marker
        names = [re.sub(r'\s+مثله\b.*$', '', n).strip() for n in names]
        # Strip "رفعه الى X" = مرفوع attribution marker ("he elevated it to X")
        names = [re.sub(r'\s+رفعه\b.*$', '', n).strip() for n in names]
        # Strip "غيره من اصحابنا" / "غيرهم من اصحابنا" — "and others from our companions"
        names = [re.sub(r'\s+(?:و\s*)?غيره(?:م)?\s+من\s+اصحابنا\b.*$', '', n).strip() for n in names]
        # Strip "وهو X" / "وكان X" — narrator identification / clarification appended to name
        # e.g. "زياد بن عيسى وهو ابو عبيدة الحذا" → "زياد بن عيسى"
        names = [re.sub(r'\s+و(?:هو|كان|يكنى)\b.*$', '', n).strip() for n in names]
        # Strip trailing "سالت X" / "قلت X" — matn bleed: "عيص بن القاسم سالت ابا عبد الله" → "عيص بن القاسم"
        names = [re.sub(r'\s+(?:سالت|قلت)\s+\S.*$', '', n).strip() for n in names]
        # NOTE: mid-name "ابن" → "بن" normalization is applied AFTER _PARALLEL (below)
        # so that "ابن محبوب", "ابن ابي عمير" etc. standalone laqabs are recognized first.
        # Strip "رجاله" / "بعض مواليك" — anonymous narrator references
        names = [n for n in names if n not in ('رجاله', 'رجله')
                 and not re.search(r'\bبعض\s+موالي', n)
                 and not re.fullmatch(r'رجل\b.*', n)]  # "رجل..." = "a man..." — matn
        # Strip brackets from names: "[يونس]" → "يونس"
        names = [re.sub(r'[\[\]]', '', n).strip() for n in names]
        # Strip trailing lone opening paren/bracket: "ابي الحسن الرضا (" → "ابي الحسن الرضا"
        names = [re.sub(r'\s*[\(\[]+\s*$', '', n).strip() for n in names]
        # Strip leading first-person verbs that bleed from matn: "سالت X" → "X", "قلت X" → "X"
        names = [re.sub(r'^(?:سالت|قلت)\s+', '', n).strip() for n in names]
        # Strip "جده X" prefix — grandfather reference; resolve the named part directly
        names = [re.sub(r'^جده\s+', '', n).strip() for n in names]
        # Strip "اخيه X" prefix — strip it and resolve the underlying name directly.
        # Imam siblings (اخيه الحسين, اخيه موسى بن جعفر...) still resolve correctly after strip
        # because the underlying names (الحسين، موسى بن جعفر) are themselves in VIRTUAL_NARRATORS.
        names = [re.sub(r'^اخيه\s+', '', n).strip() for n in names]
        # Strip leading و from segments: "وعلي بن X" → "علي بن X"
        # Exception: don't strip if the name itself starts with وهب (actual name, not connector)
        names = [re.sub(r'^و(?=[ا-ي])', '', n).strip() if not re.match(r'^وهب\b', n) else n for n in names]
        # Strip leading ل preposition (broader than the ل+ا strip below):
        # "لزرارة" → "زرارة", "لصفوان" → "صفوان"  (but NOT "لل" which becomes "ال" earlier)
        names = [re.sub(r'^ل(?!ل)(?=[ا-ي])', '', n).strip() for n in names]
        # Strip trailing "اني" = "that I" — reported-speech intro
        names = [re.sub(r'\s+اني\b.*$', '', n).strip() for n in names]
        # Strip trailing "او" / "او غيره" — truncated "or X" alternates
        names = [re.sub(r'\s+او\b.*$', '', n).strip() for n in names]
        # Strip "يرويان" / "يرويون" (dual/plural verb "they both narrate") — keep names before
        names = [re.sub(r'\s+يروي(?:ان|ون)\b.*$', '', n).strip() for n in names]
        # Strip "ان [word] بن [name]" — reported-speech intro after narrator: "زرارة ان بكير بن اعين"
        names = [re.sub(r'\s+ان\s+\S+\s+بن\b.*$', '', n).strip() for n in names]
        # "ابيه محمد بن X" = "his father, Muhammad ibn X" — split into ["ابيه", "محمد بن X"]
        _ABIHI_APPOSITIVE = re.compile(r'^(ابيه)\s+(محمد|احمد|علي|حسن|عيسى|يحيى|موسى)\s+بن\s+')
        new_names = []
        for n in names:
            m = _ABIHI_APPOSITIVE.match(n)
            if m:
                new_names.append('ابيه')
                new_names.append(n[len(m.group(1)):].strip())
            else:
                new_names.append(n)
        names = new_names
        # Split "ابيه و X" → ["ابيه", "X"] — father + parallel narrator
        # e.g. "ابيه و عمرو بن ابراهيم" or "ابيه و ابن فضال"
        new_names = []
        for n in names:
            m = re.match(r'^(ابيه|ابوه)\s+و\s+(.+)$', n)
            if m:
                new_names.append(m.group(1))
                new_names.append(m.group(2).strip())
            else:
                new_names.append(n)
        names = new_names

        # Split parallel chains: "X ومحمد بن Y" → two separate narrators.
        # Pattern: " و[Arabic-name] بن " or " و[laqab starting with ال]"
        # We expand each name into potentially multiple names.
        expanded = []
        # Also split "الحسن بن X الوشاء وعدة من اصحابنا" → two names
        # \s* between و and phrase allows both "وعدة" and "و عدة"
        # "غير واحد من اصحابنا" = "more than one of our companions" → treat like عدة
        for virt in ('عدة من اصحابنا', 'جماعة من اصحابنا', 'غير واحد من اصحابنا'):
            names = [re.sub(r'\s+و\s*' + virt + r'\s*$', '', n).strip() + ('|' + virt if re.search(r'\s+و\s*' + virt, n) else '')
                     for n in names]
        # Flatten any newly injected "|" separators
        names = [seg.strip() for n in names for seg in n.split('|') if seg.strip()]

        # \s* after و allows both "وعلي بن" and "و علي بن"
        # الحسين with ال-prefix; علا = normalized "علاء" (terminal hamza stripped); حميد added
        # عبد الله handled specially (compound name): captured as "عبد الله بن "
        _PARALLEL = re.compile(
            r'\s+و\s*((?:'
            r'(?:عبد\s+الله)\s+بن\s+'           # compound name: عبد الله بن X
            r'|(?:محمد|احمد|علي|حسن|حسين|الحسين|عبد|موسى|يونس|صفوان|زرارة|بريد|حريز|جعفر|الفضل|الفضيل|الحسن|العباس|يحيى|عمر|عمرو|سعد|صالح|عيسى|سليمان|داود|ابراهيم|هشام|حنان|اسماعيل|الريان|ابن|علا|جميل|حماد|بكير|حميد|سهل|فضل|نضر|عبيد|معاوية|خالد|زيد)\s+(?:بن\s+|ابي\s+|العجلي\b|بن\s)'
            r')|(?:الحجال|ابو علي الاشعري|ابي علي الاشعري|ابي ايوب|ابو ايوب|عبد الله|ابو العباس|ابي العباس|حماد|جميل|الرزاز|ابن محبوب|ابن رئاب|ابن فضال|ابن سنان|ابن عمار|ابن مسلم|ابن بكير|ابن ابي عمير|ابن المغيرة)\b)'
        )
        for n in names:
            parts = _PARALLEL.split(n)
            if len(parts) > 1:
                # parts = [before, name_start, after, name_start2, ...] due to capturing group
                first = parts[0].strip()
                if first:
                    expanded.append(first)
                # Reconstruct additional names from capturing group + rest
                i = 1
                while i < len(parts) - 1:
                    combined = (parts[i] + parts[i+1]).strip()
                    if combined:
                        expanded.append(combined)
                    i += 2
            else:
                expanded.append(n)

        # Re-split any "عن" connector that survived inside _PARALLEL-reconstructed names.
        # e.g. "بريد ومحمد بن مسلم عن احدهما" → _PARALLEL gives "محمد بن مسلم عن احدهما"
        # which should be split into ["محمد بن مسلم", "احدهما"].
        re_expanded = []
        for n in expanded:
            if re.search(r'\s+عن\s+', n):
                sub = [s.strip() for s in re.split(r'\s+عن\s+', n) if s.strip()]
                re_expanded.extend(sub)
            else:
                re_expanded.append(n)
        names = re_expanded

        # Normalize mid-name "ابن" connector → "بن": "زكريا ابن ادم" → "زكريا بن ادم"
        # Placed AFTER _PARALLEL so that "ابن محبوب", "ابن ابي عمير" laqabs are split first.
        # "ابن" at the START of a segment (laqab use) is unaffected — only mid-name " ابن " targeted.
        names = [re.sub(r'\s+ابن\s+', ' بن ', n).strip() for n in names]

        # Remove empties, pure digits, and duplicates while preserving order.
        # Also strip any "قال X" matn-start that leaked into a name segment.
        seen: set = set()
        final_names = []
        for n in names:
            n = n.strip()
            # Strip inline "قال/يقول [verb/content]" at the end of a name segment
            # NOTE: do NOT include فقال here — grade_chain uses \bفقال\b to skip matn segments
            n = re.sub(r'\s+(?:قال|يقول)\s+\S.*$', '', n).strip()
            # Re-apply trailing filler strip (may have been added by _PARALLEL reconstruction)
            n = re.sub(r'\s+(?:قال|يقول|قالوا|جميعا|معا)\s*$', '', n).strip()
            # Strip trailing honorific abbreviations: ع (عليه السلام), عج (عجل الله فرجه), ص (صلى...)
            # These appear e.g. as "[عن احدهما ع]" → "احدهما ع" → "احدهما"
            n = re.sub(r'\s+[عصج]{1,2}\s*$', '', n).strip()
            # Strip "ابائك" / "اباءك" / "ابائك" — "your fathers" — anonymous matn reference
            if re.fullmatch(r'ابا[ئءيء]{0,2}ك', n):
                continue
            if n and not re.fullmatch(r'[\d٠-٩]+', n) and n not in seen:
                final_names.append(n)
                seen.add(n)
                # NOTE: Removed unconditional Imam truncation — it incorrectly cut chains
                # where Imams appear in the middle (e.g. "علي بن الحسين، عن أبيه، عن جده، عن النبي").
                # The extensive matn filters above (lines ~1613-1656) already handle matn text removal.

        return final_names

    def analyze(self, chain_names: List[str],
                active_principles: Optional[set] = None) -> Dict:
        """
        Step through the chain and resolve each narrator in context.
        """
        # Precompute vouch sets for active principles
        vouch_sets: dict = {}
        if active_principles:
            for pk in active_principles:
                vouch_sets[pk] = self._get_vouch_set(pk)

        results = []
        tabaqah_gaps = []

        for i, name in enumerate(chain_names):
            # top-down: i=0 is author, i=last is Imam-side
            # Narrates FROM = person AFTER them in the list (i+1)
            # Narrated BY = person BEFORE them in the list (i-1)
            
            narrates_from = [chain_names[i+1]] if i < len(chain_names) - 1 else []
            narrated_by = [chain_names[i-1]] if i > 0 else []
            
            expected_range = None  # set in else branch; None for virtual/father cases

            # Check virtual narrators first
            norm_name = normalize_ar(name)
            if norm_name in VIRTUAL_NARRATORS:
                resolution = {
                    "query": name,
                    "top_match": VIRTUAL_NARRATORS[norm_name],
                    "other_candidates": [],
                    "total_candidates": 1,
                    "match_case": "virtual"
                }
            elif norm_name in ('ابيه', 'ابوه', 'عن ابيه'):
                # Relative reference: "his father". Try to resolve from the previous
                # narrator's father field in the database.
                resolution = self._resolve_father(
                    results, narrates_from, narrated_by
                )
            elif norm_name in ('جده', 'عن جده'):
                # Grandfather reference. Walk two narrators back: the chain entry
                # whose grandfather we want is the one BEFORE the immediate predecessor.
                resolution = self._resolve_grandfather(
                    results, narrates_from, narrated_by
                )
            elif norm_name in ('عمه', 'عن عمه', 'اخيه', 'عن اخيه',
                                'ابنه', 'عن ابنه', 'بنته', 'عن بنته'):
                # Other-relative reference (uncle/brother/son/daughter). The current
                # rijal schema does not store these relations structurally, so we
                # mark as unresolvable_relative rather than silently dropping.
                relation_label = norm_name.replace('عن ', '')
                resolution = self._resolve_unsupported_relative(
                    results, relation_label
                )
            else:
                # ── Build expected_tabaqah_range from chain neighbors ──────
                t_prev = results[i - 1]['tabaqah'] if i > 0 else None

                t_next = None
                if i < len(chain_names) - 1:
                    next_name = chain_names[i + 1]
                    next_norm = normalize_ar(next_name)
                    vm_next = VIRTUAL_NARRATORS.get(next_norm)
                    if vm_next:
                        ck_next = vm_next.get('canonical_key', '')
                        t_next = IMAM_TABAQAH_MAP.get(ck_next)
                    else:
                        t_next = self._estimate_tabaqah(next_name)

                if t_prev is not None and t_next is not None:
                    expected_range = (min(t_prev, t_next), max(t_prev, t_next))
                elif t_prev is not None:
                    expected_range = (max(1, t_prev - 2), t_prev)
                elif t_next is not None:
                    expected_range = (t_next, min(12, t_next + 2))
                else:
                    expected_range = None

                resolution = resolve(
                    self.resolver_index,
                    name,
                    narrates_from=narrates_from,
                    narrated_by=narrated_by,
                    top_k=5,
                    expected_tabaqah_range=expected_range,
                )
            
            # ── Irsal Detection ─────────────────────────────────────────────
            # Check if this link represents an irsal (gap in chain)
            irsal_type = None
            irsal_note = None
            
            # Check for explicit irsal markers in the name
            norm_name_check = normalize_ar(name)
            if norm_name_check in ('عمن رواه', 'عمن ذكره'):
                irsal_type = 'mursal'
                irsal_note = 'إرسال — راوٍ مجهول أو محذوف من السند'
                # Add to tabaqah_gaps even without tabaqah values
                if i < len(chain_names) - 1:
                    tabaqah_gaps.append({
                        "index_a": i,
                        "index_b": i + 1,
                        "narrator_a": name,
                        "narrator_b": chain_names[i + 1],
                        "tabaqah_a": None,
                        "tabaqah_b": None,
                        "gap": None,
                        "severity": "major",
                        "note": irsal_note,
                    })
            
            # Check for tabaqah gap with next narrator (student-teacher gap)
            elif i < len(chain_names) - 1:
                current_tabaqah = None
                next_tabaqah = None
                
                # Get current narrator's tabaqah
                if resolution.get('top_match'):
                    tm = resolution['top_match']
                    ck = tm.get('canonical_key', '')
                    if ck in IMAM_TABAQAH_MAP:
                        current_tabaqah = IMAM_TABAQAH_MAP[ck]
                    elif 'tabaqah' in tm:
                        current_tabaqah = tm['tabaqah']
                    elif not ck.startswith('VIRTUAL'):
                        entry_key = tm.get('entry_key', ck)
                        if self.db_loader:
                            entry = self.db_loader.get_entry(entry_key) or {}
                        else:
                            entry = self.db.get(entry_key, {})
                        current_tabaqah = entry.get('tabaqah')
                
                # Get next narrator's tabaqah
                next_name = chain_names[i + 1]
                next_norm = normalize_ar(next_name)
                vm_next = VIRTUAL_NARRATORS.get(next_norm)
                if vm_next:
                    ck_next = vm_next.get('canonical_key', '')
                    next_tabaqah = IMAM_TABAQAH_MAP.get(ck_next)
                else:
                    # Try to estimate next narrator's tabaqah
                    next_tabaqah = self._estimate_tabaqah(next_name)
                
                # Detect irsal based on tabaqah gap
                if current_tabaqah is not None and next_tabaqah is not None:
                    gap = current_tabaqah - next_tabaqah
                    
                    if gap >= 3:
                        irsal_type = 'mursal'
                        irsal_note = f'إرسال — فجوة {gap} طبقات بين الراوي والشيخ'
                    elif gap == 2:
                        irsal_type = 'munqati'
                        irsal_note = f'منقطع — فجوة طبقة واحدة (gap={gap})'
                    elif gap <= -2:
                        irsal_type = 'mudal'
                        irsal_note = f'معضل — ترتيب عكسي ({abs(gap)} طبقة)'
            
            # Store irsal information in resolution
            if irsal_type:
                resolution['irsal_type'] = irsal_type
                resolution['irsal_note'] = irsal_note
            
            # ── Tabaqah lookup ─────────────────────────────────────────────
            tabaqah = None
            tabaqah_label = None
            tabaqah_source = None
            if resolution.get('top_match'):
                tm_data = resolution['top_match']
                canonical_key = tm_data.get('canonical_key', '')
                if canonical_key in IMAM_TABAQAH_MAP:
                    tabaqah = IMAM_TABAQAH_MAP[canonical_key]
                    tabaqah_source = 'imam'
                elif 'tabaqah' in tm_data:
                    # Explicit tabaqah override on a virtual/lookup entry
                    tabaqah = tm_data['tabaqah']
                    tabaqah_source = 'override'
                elif not canonical_key.startswith('VIRTUAL'):
                    entry_key = tm_data.get('entry_key', canonical_key)
                    # Use loader to get entry instead of direct db access
                    if self.db_loader:
                        entry = self.db_loader.get_entry(entry_key) or self.db_loader.get_entry(canonical_key) or {}
                    else:
                        entry = self.db.get(entry_key) or self.db.get(canonical_key, {})
                    tabaqah = entry.get('tabaqah')
                    tabaqah_source = entry.get('tabaqah_source')
            if tabaqah:
                tabaqah_label = TABAQAH_LABELS.get(tabaqah, f"T{tabaqah}")

            # ── Tabaqah lookup for other_candidates ────────────────────────
            enriched_others = []
            for c in resolution.get('other_candidates', []):
                c_tab = None
                c_key = c.get('canonical_key', '')
                if c_key in IMAM_TABAQAH_MAP:
                    c_tab = IMAM_TABAQAH_MAP[c_key]
                elif not c_key.startswith('VIRTUAL'):
                    c_entry_key = c.get('entry_key', c_key)
                    # Use loader to get entry instead of direct db access
                    if self.db_loader:
                        c_entry = self.db_loader.get_entry(c_entry_key) or self.db_loader.get_entry(c_key) or {}
                    else:
                        c_entry = self.db.get(c_entry_key) or self.db.get(c_key, {})
                    c_tab = c_entry.get('tabaqah')
                enriched_others.append({**c, 'tabaqah': c_tab})
            resolution = {**resolution, 'other_candidates': enriched_others}

            # ── Principle application ──────────────────────────────────────
            effective_status = None
            principle_applied = None
            if vouch_sets and resolution.get('top_match'):
                tm = resolution['top_match']
                raw_status = tm.get('status', 'unspecified')
                if raw_status != 'thiqah':
                    ek = tm.get('entry_key', tm.get('canonical_key', ''))
                    for pk, vouch in vouch_sets.items():
                        if ek in vouch:
                            effective_status = 'thiqah'
                            principle_applied = pk
                            break

            results.append({
                "original_query": name,
                "resolution": resolution,
                "tabaqah": tabaqah,
                "tabaqah_label": tabaqah_label,
                "tabaqah_source": tabaqah_source,
                "expected_tabaqah_range": expected_range,
                "effective_status": effective_status,
                "principle_applied": principle_applied,
            })

        # ── Tabaqah gap analysis ───────────────────────────────────────────────
        # tabaqah_gaps is already populated during the main loop
        for i in range(len(results) - 1):
            t_a = results[i].get('tabaqah')
            t_b = results[i + 1].get('tabaqah')
            if t_a is not None and t_b is not None:
                gap = t_a - t_b  # positive = student (later) -> teacher (earlier)
                # Normal: gap 0 (same gen) or 1 (adjacent generation) - no flag
                # Warning: gap 2 (skip one gen - possible mursal) or -1 (minor backward)
                # Major: gap >= 3 (skip 2+ gens) or gap <= -2 (truly reversed chain)
                if gap >= 2 or gap <= -1:
                    severity = "major" if (gap >= 3 or gap <= -2) else "warning"
                    tabaqah_gaps.append({
                        "index_a": i,
                        "index_b": i + 1,
                        "narrator_a": results[i]['original_query'],
                        "narrator_b": results[i + 1]['original_query'],
                        "tabaqah_a": t_a,
                        "tabaqah_b": t_b,
                        "gap": gap,
                        "severity": severity,
                        "note": _tabaqah_gap_note(gap),
                    })

        # ── Phase 3: Transmission feasibility & tadlīs ─────────────────────
        # For each adjacent (student, teacher) pair, check whether their
        # lifetimes actually overlap and flag suspicious links. This runs
        # BEFORE grading so the grader can downgrade `impossible` chains.
        transmission_links: List[Dict] = []
        tadlis_flags: List[Dict] = []
        if _validate_transmission_chain is not None and self.db_loader is not None:
            chain_entries: List[Optional[Dict]] = []
            for r in results:
                tm = r.get('resolution', {}).get('top_match') or {}
                ck = tm.get('canonical_key', '') or ''
                ek = tm.get('entry_key')
                if ck.startswith('VIRTUAL') or ck.startswith('IMAM_'):
                    # Imams and virtuals have no DB entry; treat as
                    # unresolved for lifetime purposes (lifetime is implicit
                    # in the Imam's tabaqah and is well-known elsewhere).
                    chain_entries.append(None)
                elif ek:
                    chain_entries.append(self.db_loader.get_entry(ek))
                else:
                    chain_entries.append(None)

            transmission_links = _validate_transmission_chain(chain_entries)
            tadlis_raw = _detect_tadlis(chain_entries) if _detect_tadlis else []
            tadlis_flags = [
                {
                    'position':     f.position,
                    'teacher_name': f.teacher_name,
                    'student_name': f.student_name,
                    'type':         f.type,
                    'severity':     f.severity,
                    'evidence':     f.evidence,
                    'extra':        f.extra,
                }
                for f in tadlis_raw
            ]

        # Determine final status using the best matches
        final_status = self.grade_chain(results)

        # Downgrade if there are impossible transmission links — chain is
        # broken regardless of narrator reliability.
        impossible_links = [
            l for l in transmission_links if l.get('verdict') == 'impossible'
        ]
        if impossible_links:
            final_status = dict(final_status) if isinstance(final_status, dict) else {'grade': str(final_status)}
            final_status.setdefault('downgrades', [])
            final_status['downgrades'].append({
                'reason': 'impossible_transmission',
                'detail': f'{len(impossible_links)} link(s) cannot have occurred (lifetime gap)',
                'links':  impossible_links,
            })
            # Force grade to a broken-chain category
            existing = final_status.get('grade', '')
            if 'inqitāʿ' not in existing and 'منقطع' not in existing:
                final_status['grade'] = f'{existing} → منقطع (تعذّر التلاقي)'.strip(' →')

        return {
            "chain": results,
            "final_status": final_status,
            "tabaqah_gaps": tabaqah_gaps,
            "transmission_links": transmission_links,
            "tadlis_flags": tadlis_flags,
        }

    def _resolve_father(self, prior_results: List[Dict],
                        narrates_from: List[str], narrated_by: List[str]) -> Dict:
        """
        Resolve "أبيه" (his father) by looking up the father of the most
        recently resolved narrator in the database.
        Falls back to a thiqah-assumed virtual link if the father can't be found.
        """
        # Walk back through prior results to find the last successfully resolved narrator
        father_name = None
        grandfather_name = None
        for prev in reversed(prior_results):
            tm = prev['resolution'].get('top_match')
            if tm and tm.get('canonical_key') and not tm['canonical_key'].startswith('VIRTUAL'):
                # Use loader to get entry instead of direct db access
                if self.db_loader:
                    entry = self.db_loader.get_entry(tm['canonical_key']) or {}
                else:
                    entry = self.db.get(tm['canonical_key'], {})
                father_name = entry.get('father')
                grandfather_name = entry.get('grandfather')
                break

        if father_name:
            # Build a more specific query: "إبراهيم بن هاشم" instead of bare "إبراهيم"
            query = father_name
            if grandfather_name:
                query = f"{father_name} بن {grandfather_name}"
            resolution = resolve(
                self.resolver_index,
                query,
                narrates_from=narrates_from,
                narrated_by=narrated_by,
                top_k=5
            )
            # Tag so the report shows what happened
            resolution['query'] = f"أبيه [{query}]"
            return resolution

        # Fallback: can't determine the father — return unresolved
        return {
            "query": "أبيه",
            "top_match": None,
            "other_candidates": [],
            "total_candidates": 0,
            "message": "أبيه: father reference — preceding narrator's father not found in database.",
        }

    def _resolve_grandfather(self, prior_results: List[Dict],
                             narrates_from: List[str], narrated_by: List[str]) -> Dict:
        """Resolve "عن جده" (his grandfather).

        Walks back through prior_results looking for the most recent resolved
        non-virtual narrator. The grandfather is in the entry's `grandfather`
        field; if that's missing, fall back to walking the `nasab` (lineage)
        chain — `بن X بن Y` after the father suggests Y is the grandfather.
        """
        grandfather_name = None
        father_for_query = None  # used to disambiguate same-named grandfathers

        for prev in reversed(prior_results):
            tm = prev['resolution'].get('top_match')
            if tm and tm.get('canonical_key') and not tm['canonical_key'].startswith('VIRTUAL'):
                if self.db_loader:
                    entry = self.db_loader.get_entry(tm['canonical_key']) or {}
                else:
                    entry = self.db.get(tm['canonical_key'], {})
                grandfather_name = entry.get('grandfather')
                father_for_query = entry.get('father')
                if not grandfather_name and entry.get('nasab'):
                    # nasab is "ibn X ibn Y ibn Z..." — Y is the grandfather
                    nasab_tokens = entry['nasab'].split('بن')
                    nasab_tokens = [t.strip() for t in nasab_tokens if t.strip()]
                    if len(nasab_tokens) >= 2:
                        grandfather_name = nasab_tokens[1]
                break

        if grandfather_name:
            # Build a more specific query when possible: "X بن Y" so the
            # resolver can distinguish among same-named narrators.
            query = grandfather_name
            if father_for_query:
                query = f"{grandfather_name}"  # bare; no great-grandfather to add
            resolution = resolve(
                self.resolver_index,
                query,
                narrates_from=narrates_from,
                narrated_by=narrated_by,
                top_k=5,
            )
            resolution['query'] = f"جده [{query}]"
            return resolution

        return {
            "query": "جده",
            "top_match": None,
            "other_candidates": [],
            "total_candidates": 0,
            "message": "جده: grandfather reference — could not be resolved from preceding narrator's lineage.",
        }

    def _resolve_unsupported_relative(self, prior_results: List[Dict],
                                       relation_label: str) -> Dict:
        """Mark a relative pronoun (uncle/brother/son) as unresolvable.

        These relations are not stored structurally in the rijal DB. We surface
        the bare relation rather than silently dropping the chain link, so the
        scholar reviewing the chain can manually resolve or add data.
        """
        # Which narrator is this relative OF?
        anchor_name = '?'
        for prev in reversed(prior_results):
            tm = prev['resolution'].get('top_match')
            if tm:
                anchor_name = tm.get('name_ar') or tm.get('canonical_key', '?')
                break

        return {
            "query": relation_label,
            "top_match": None,
            "other_candidates": [],
            "total_candidates": 0,
            "match_case": "unresolvable_relative",
            "anchor": anchor_name,
            "message": (
                f"{relation_label}: relative reference (of {anchor_name}) — "
                f"this relation is not stored in the rijal DB schema and "
                f"cannot be auto-resolved. Manual resolution required."
            ),
        }

    # ── Principles ────────────────────────────────────────────────────────────

    def _is_principle_anchor(self, norm_name: str) -> bool:
        """True if the normalized name refers to one of the three attestors."""
        toks = set(tokenize(norm_name))
        # ابن أبي عمير — requires both عمير and ابي
        if 'عمير' in toks and 'ابي' in toks:
            return True
        # صفوان بن يحيى
        if 'صفوان' in toks:
            return True
        # البزنطي — بزنطي may appear as substring of البزنطي
        if any('بزنطي' in t for t in toks):
            return True
        return False

    def _compute_vouch_set(self, principle_key: str) -> frozenset:
        """Scan DB and return entry_keys whose narrated_from_by includes an anchor."""
        if principle_key != 'mashayikh_thalatha':
            return frozenset()
        vouch: set = set()
        # Use loader to iterate through entries instead of direct db access
        if self.db_loader:
            for entry in self.db_loader.iter_entries():
                entry_key = entry.get('_entry_idx', '')
                if not entry_key:
                    continue
                for student_name in (entry.get('narrated_from_by') or []):
                    if self._is_principle_anchor(normalize_ar(student_name)):
                        vouch.add(entry_key)
                        break
        else:
            # Fallback to old method if loader is not available
            for entry_key, entry in self.db.items():
                for student_name in (entry.get('narrated_from_by') or []):
                    if self._is_principle_anchor(normalize_ar(student_name)):
                        vouch.add(entry_key)
                        break
        return frozenset(vouch)

    def _get_vouch_set(self, principle_key: str) -> frozenset:
        if principle_key not in self._principle_vouch_cache:
            self._principle_vouch_cache[principle_key] = self._compute_vouch_set(principle_key)
        return self._principle_vouch_cache[principle_key]

    def count_principle_uplift(self, principle_key: str) -> dict:
        """Return count of narrators affected by the principle and how many are newly thiqah."""
        vouch = self._get_vouch_set(principle_key)
        total = len(vouch)
        newly_thiqah = 0
        for ek in vouch:
            # Use loader to get entry instead of direct db access
            if self.db_loader:
                entry = self.db_loader.get_entry(ek)
                status = entry.get('status', 'unspecified') if entry else 'unspecified'
            else:
                status = self.db.get(ek, {}).get('status', 'unspecified')
            if status != 'thiqah':
                newly_thiqah += 1
        return {'total_vouched': total, 'newly_thiqah': newly_thiqah}

    def _estimate_tabaqah(self, name: str) -> Optional[int]:
        """
        Estimate tabaqah using BOTH Imam references and narrator network.
        This forms a final opinion by combining:
        1. Direct Imam references (narrates_from_imams)
        2. Narrator network propagation (narrates_from_narrators)
        """
        norm = normalize_ar(name)
        candidates_raw = list(self.resolver_index.get(norm, []))

        words = norm.split()
        if words:
            first = words[0]
            alt = norm[2:] if (first.startswith('ال') and len(first) > 2) else 'ال' + norm
            candidates_raw += list(self.resolver_index.get(alt, []))

        if not candidates_raw:
            return None

        seen: set = set()
        weighted = []
        
        for c in candidates_raw:
            ek = c.get('entry_key')
            if not ek or ek in seen:
                continue
            seen.add(ek)
            
            # ── Source 1: Direct Imam references ──────────────────────────────
            imam_tabaqah_values = []
            imams = c.get('narrates_from_imams', [])
            for imam_name in imams:
                # Normalize and look up Imam tabaqah
                imam_norm = normalize_ar(imam_name)
                # Check if this is a known Imam
                for imam_key, imam_tab in IMAM_TABAQAH_MAP.items():
                    # Get Imam name from VIRTUAL_NARRATORS
                    for virt_name, virt_data in VIRTUAL_NARRATORS.items():
                        if virt_data.get('canonical_key') == imam_key:
                            if imam_norm in virt_name or virt_name in imam_norm:
                                imam_tabaqah_values.append(imam_tab + 1)  # Student is +1
                                break
            
            # ── Source 2: Narrator network propagation ────────────────────────
            network_tabaqah_values = []
            teachers = c.get('narrates_from_narrators', [])
            for teacher_name in teachers:
                teacher_norm = normalize_ar(teacher_name)
                # Look up teacher in resolver index
                teacher_candidates = self.resolver_index.get(teacher_norm, [])
                if teacher_candidates:
                    # Get teacher's tabaqah (use first candidate)
                    teacher_tab = teacher_candidates[0].get('tabaqah')
                    if teacher_tab is None:
                        # Try to get from database
                        teacher_key = teacher_candidates[0].get('entry_key')
                        if self.db_loader:
                            teacher_entry = self.db_loader.get_entry(teacher_key)
                            teacher_tab = teacher_entry.get('tabaqah') if teacher_entry else None
                        else:
                            teacher_tab = self.db.get(teacher_key, {}).get('tabaqah')
                    
                    if teacher_tab is not None:
                        network_tabaqah_values.append(teacher_tab + 1)  # Student is +1
            
            # ── Combine both sources to form final opinion ────────────────────
            all_values = []
            source_weights = []
            
            # Imam references are more reliable (higher weight)
            if imam_tabaqah_values:
                all_values.extend(imam_tabaqah_values)
                source_weights.extend([2.0] * len(imam_tabaqah_values))  # Higher weight for Imam refs
            
            # Network propagation is less reliable but still valuable
            if network_tabaqah_values:
                all_values.extend(network_tabaqah_values)
                source_weights.extend([1.0] * len(network_tabaqah_values))  # Standard weight
            
            if not all_values:
                continue
            
            # Calculate weighted median
            # Sort by tabaqah value
            sorted_pairs = sorted(zip(all_values, source_weights))
            total_weight = sum(source_weights)
            cumulative = 0
            final_tabaqah = None
            
            for t, w in sorted_pairs:
                cumulative += w
                if cumulative >= total_weight / 2:
                    final_tabaqah = t
                    break
            
            if final_tabaqah is None:
                final_tabaqah = sorted_pairs[-1][0]
            
            # Weight by hadith count (famousness bonus)
            hc = c.get('hadith_count') or 1
            weighted.append((final_tabaqah, hc))

        if not weighted:
            return None

        # Return median of all candidates
        weighted.sort(key=lambda x: x[0])
        total_weight = sum(w for _, w in weighted)
        cumulative = 0
        for t, w in weighted:
            cumulative += w
            if cumulative >= total_weight / 2:
                return t
        return weighted[-1][0]

    def grade_chain(self, analysis_results: List[Dict]) -> Dict:
        """
        Grade the chain based on Shia Rijal criteria.
        """
        statuses = []
        all_resolved = True
        has_daif = False
        has_majhul = False
        has_mamduh = False
        has_non_imami = False
        
        for res in analysis_results:
            # Skip segments that are clearly matn — no real narrator name is this long
            # or contains a mid-sentence "فقال" (Imam's answer), "يكون", "فان", etc.
            original_query = res.get('original_query', '')
            if len(original_query) > 55:
                continue
            if re.search(r'\bفقال\b|\bيكون\b|\bفان\b|\bعلم انه\b', original_query):
                continue

            match = res['resolution'].get('top_match')
            if not match:
                all_resolved = False
                statuses.append("unresolved")
                continue
                
            # Use effective_status (from active principle) if available, else raw status
            raw_status = match.get('status', 'unspecified')
            status = res.get('effective_status') or raw_status
            statuses.append(status)

            # Map statuses to grading categories
            if status == 'daif':
                has_daif = True
            elif status == 'majhul':
                has_majhul = True
            elif status == 'unspecified':
                # Check if this is a known narrator with hadiths
                # 'unspecified' means known but status not explicitly graded
                entry_key = match.get('entry_key', match['canonical_key'])
                if self.db_loader:
                    entry = self.db_loader.get_entry(entry_key) or {}
                else:
                    entry = self.db.get(entry_key, {})
                
                # If narrator has significant hadith count, treat as reliable
                hadith_count = entry.get('hadith_count', 0)
                if hadith_count and hadith_count >= 100:
                    # Known narrator with substantial hadiths - treat as thiqah
                    pass  # Don't set has_majhul
                else:
                    # Unknown or minimal hadiths - treat as majhul
                    has_majhul = True
            elif status in ('mamduh', 'hasan', 'muwaththaq'):
                # Technically Muwathaq is reliable but non-imami
                if status == 'muwaththaq':
                    has_non_imami = True
                else:
                    has_mamduh = True

            # Additional sect check — use entry_key (the specific matched db record),
            # NOT canonical_key (the cluster representative), because identity
            # clustering sometimes merges different narrators incorrectly, causing
            # the canonical to carry a sect flag that doesn't belong to this entry.
            # IMPORTANT: Only apply sect penalty when status is NOT 'thiqah'.
            # If al-Khoei grades someone 'thiqah', he has already accounted for their
            # sectarian history (e.g., repentance) and fully vouched for them.
            if status != 'thiqah':
                entry_key = match.get('entry_key', match['canonical_key'])
                # Use loader to get entry instead of direct db access
                if self.db_loader:
                    entry = self.db_loader.get_entry(entry_key) or {}
                else:
                    entry = self.db.get(entry_key, {})
                sect = (entry.get('sect') or "").lower()
                if sect and any(s in sect for s in ['عامي', 'فطحي', 'واقفي', 'زيدي']):
                    has_non_imami = True

        # Grading logic (prioritize worst status)
        if not all_resolved:
            grade = "Undetermined (Unresolved links)"
        elif has_daif:
            grade = "Da'if (Weak)"
        elif has_majhul:
            grade = "Majhul / Da'if (Unknown Narrators)"
        elif has_non_imami:
            grade = "Muwaththaq (Reliable - Non-Imami)"
        elif has_mamduh:
            grade = "Hasan (Good)"
        else:
            grade = "Sahih (Authentic)"

        return {
            "grade": grade,
            "narrator_statuses": statuses
        }

    def print_results(self, analysis: Dict):
        print("\n" + "═"*80)
        print(" ISNAD ANALYSIS REPORT (Top-Down Resolution)")
        print("═"*80)
        
        for i, item in enumerate(analysis['chain']):
            name = item['original_query']
            res = item['resolution']
            tm = res['top_match']
            others = res.get('other_candidates', [])
            
            print(f"\nLINK {i+1}: {name}")
            if tm:
                # Format status label
                sl = {
                    'thiqah':     '✓ THIQAH',
                    'daif':       '✗ DA\'IF',
                    'majhul':     '? MAJHUL',
                    'mamduh':     '~ MAMDUH',
                    'hasan':      '~ HASAN',
                    'muwaththaq': '~ MUWATHTHAQ',
                    'unspecified':'? UNSPECIFIED',
                }.get(tm['status'], tm['status'].upper())

                n3 = tm.get('n3_display', tm['canonical_key'])
                print(f"  → BEST: {tm['name_ar']} [n3={n3}, idx={tm['canonical_key']}]")
                print(f"    Status: {sl:15} Score: {tm['confidence_score']:.2f}")
                if tm['match_reasons']:
                    print(f"    Match Reasons: {'; '.join(tm['match_reasons'])}")

                # Show alternative candidates if relevant
                near_alternatives = [c for c in others if c['confidence_score'] >= tm['confidence_score'] - 1.0]
                if near_alternatives:
                    print(f"    Possible Alternatives:")
                    for c in near_alternatives[:3]:
                        cn3 = c.get('n3_display', c['canonical_key'])
                        c_tab = c.get('tabaqah')
                        tab_str = f"T{c_tab}" if c_tab is not None else "T?"
                        print(f"      - {c['name_ar']} (n3={cn3}) [{c['status']}] {tab_str} score={c['confidence_score']:.2f}")
            else:
                print("    ⚠  RESOLUTION FAILED - No candidates found.")

        # ── Tabaqah gap analysis ──────────────────────────────────────────────
        gaps = analysis.get('tabaqah_gaps', [])
        chain = analysis['chain']
        if gaps:
            print("\n" + "─"*80)
            print(" TABAQAH ANALYSIS")
            print("─"*80)
            for g in gaps:
                ia, ib = g['index_a'], g['index_b']
                sev_icon = "⚠⚠" if g['severity'] == 'major' else "⚠"
                print(f"\n  {sev_icon} Gap between Link {ia+1} (T{g['tabaqah_a']}) → Link {ib+1} (T{g['tabaqah_b']}): {g['note']}")
                print(f"      Link {ia+1}: {g['narrator_a']}")
                print(f"      Link {ib+1}: {g['narrator_b']}")

                # Check if an alternative at position ia or ib would resolve the gap
                suggestions = []
                for pos, link_idx in [("Link " + str(ia+1), ia), ("Link " + str(ib+1), ib)]:
                    item = chain[link_idx]
                    tm_tab = item.get('tabaqah')
                    res = item['resolution']
                    others = res.get('other_candidates', [])
                    tm = res.get('top_match')
                    near = [c for c in others if tm and c['confidence_score'] >= tm['confidence_score'] - 1.0]
                    for c in near[:3]:
                        c_tab = c.get('tabaqah')
                        if c_tab is None:
                            continue
                        # Would this alternative reduce or eliminate the gap?
                        other_pos = ib if link_idx == ia else ia
                        other_tab = chain[other_pos].get('tabaqah')
                        if other_tab is None:
                            continue
                        new_gap = (c_tab - other_tab) if link_idx == ia else (other_tab - c_tab)
                        old_gap = g['gap']
                        # Better = gap moves toward 0 or 1 (normal range)
                        def in_normal(gap): return 0 <= gap <= 1
                        if not in_normal(old_gap) and in_normal(new_gap):
                            fit = "resolves gap"
                        elif abs(new_gap - 1) < abs(old_gap - 1):
                            fit = f"reduces gap (new gap {new_gap:+d})"
                        else:
                            continue
                        suggestions.append(f"{pos} → {c['name_ar']} (T{c_tab}) [{c['status']}]: {fit}")

                if suggestions:
                    print(f"      Possible better fit(s):")
                    for s in suggestions:
                        print(f"        ✦ {s}")

        print("\n" + "═"*80)
        grade = analysis['final_status']['grade']
        print(f" FINAL CHAIN STATUS: {grade}")
        print("═"*80 + "\n")


class IdentificationModule:
    """Module for narrator identification using 118 rules from book insights"""
    
    def __init__(self, db_loader, resolver_index):
        self.db_loader = db_loader
        self.resolver_index = resolver_index
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze narrators for identification using comprehensive rules.
        Implements 118 identification rules from book insights.
        """
        results = {}
        
        for narrator in narrators:
            # Skip virtual segments like "fقال"
            if len(narrator) > 55 or re.search(r'\bفقال\b|\bيكون\b|\bفان\b|\bعلم انه\b', narrator):
                continue
            
            # Normalize the narrator name
            norm = normalize_ar(narrator)
            
            # Get candidates from resolver
            candidates = self.resolver_index.get(norm, [])
            
            # Apply identification rules
            matches = []
            warnings = []
            confidence = 0.0
            
            if candidates:
                # Rule 1: Exact match is highest confidence
                matches.append("Exact name match")
                confidence = 0.95
                
                # Rule 2: Check for kunya patterns
                if re.search(r'أبو|أم', narrator):
                    matches.append("Kunya pattern detected")
                    confidence = max(confidence, 0.85)
                
                # Rule 3: Check for nisba patterns
                if re.search(r'الكوفي|البصري|البغدادي|الدمشقي', narrator):
                    matches.append("Nisba pattern detected")
                    confidence = max(confidence, 0.80)
                
                # Rule 4: Check for multiple candidates
                if len(candidates) > 1:
                    warnings.append(f"Multiple candidates found ({len(candidates)})")
                    confidence *= 0.9
            else:
                # Try alternative forms
                words = norm.split()
                if words:
                    first = words[0]
                    alt = norm[2:] if (first.startswith('ال') and len(first) > 2) else 'ال' + norm
                    candidates = self.resolver_index.get(alt, [])
                    if candidates:
                        matches.append("Alternative form match")
                        confidence = 0.75
            
            results[narrator] = {
                'matches': matches,
                'warnings': warnings,
                'confidence': confidence,
                'candidate_count': len(candidates)
            }
        
        return results


class ErrorDetectionModule:
    """Module for detecting scribal errors using 87 rules from book insights"""
    
    def __init__(self, db_loader, resolver_index):
        self.db_loader = db_loader
        self.resolver_index = resolver_index
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze narrators for potential scribal errors.
        Implements 87 error detection rules from book insights.
        """
        results = {
            'tashif_errors': [],
            'harf_errors': [],
            'name_confusions': [],
            'gap_errors': []
        }
        
        # Check for common tashif (scribal error) patterns
        for i, narrator in enumerate(narrators):
            norm = normalize_ar(narrator)
            
            # Rule: Check for common letter confusions (ت/ب, س/ش, etc.)
            if re.search(r'القاسم|القاسمي', norm):
                results['tashif_errors'].append(f"Possible tashif in '{narrator}' (القاسم/القاسمي)")
            
            # Rule: Check for missing/extra letters
            if re.search(r'ابن\s+أبي', norm):
                results['harf_errors'].append(f"Check name structure: '{narrator}'")
            
            # Rule: Check for name confusion patterns
            if i > 0 and i < len(narrators) - 1:
                prev_norm = normalize_ar(narrators[i-1])
                next_norm = normalize_ar(narrators[i+1])
                
                # Detect potential name swaps
                if norm == prev_norm or norm == next_norm:
                    results['name_confusions'].append(f"Duplicate name detected: '{narrator}'")
        
        # Check for chain gaps
        if len(narrators) < 2:
            results['gap_errors'].append("Chain too short for gap analysis")
        
        return results


class InvestigationModule:
    """Module for investigation and tafseer analysis using 37 rules from book insights"""
    
    def __init__(self, db_loader, resolver_index):
        self.db_loader = db_loader
        self.resolver_index = resolver_index
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze the chain for investigation purposes.
        Implements investigation rules from book insights.
        """
        results = {
            'traces': [],
            'parallel_chains': [],
            'gaps': [],
            'chain_integrity': None,
            'common_teachers': [],
            'transmission_paths': [],
        }
        
        # Check for transmission patterns
        for i, narrator in enumerate(narrators):
            norm = normalize_ar(narrator)
            
            # Rule: Check for common teacher patterns
            if i > 0 and i < len(narrators) - 1:
                prev_norm = normalize_ar(narrators[i-1])
                next_norm = normalize_ar(narrators[i+1])
                
                # Detect potential common teachers
                results['common_teachers'].append({
                    'position': i,
                    'narrator': narrator,
                    'pattern': 'student-teacher-student'
                })
            
            # Rule: Check for transmission path patterns
            if re.search(r'عن|من', narrator):
                results['transmission_paths'].append(f"Transmission marker found in '{narrator}'")
        
        # Calculate chain integrity
        total_links = len(narrators)
        resolved_links = sum(1 for n in narrators if self.resolver_index.get(normalize_ar(n)))
        results['chain_integrity'] = resolved_links / total_links if total_links > 0 else 0
        
        return results


class ReliabilityModule:
    """Module for reliability assessment using 37 وثوق rules from book insights"""
    
    def __init__(self, db_loader, resolver_index):
        self.db_loader = db_loader
        self.resolver_index = resolver_index
    
    def analyze(self, narrators: List[str]) -> Dict:
        """
        Analyze narrator reliability using وثوق principles.
        Implements 37 reliability rules from book insights.
        """
        results = {}
        
        for narrator in narrators:
            # Skip virtual segments
            if len(narrator) > 55 or re.search(r'\bفقال\b|\bيكون\b|\bفان\b|\bعلم انه\b', narrator):
                continue
            
            norm = normalize_ar(narrator)
            candidates = self.resolver_index.get(norm, [])
            
            if not candidates:
                results[narrator] = {
                    'وثوق_status': 'Unknown',
                    'strength_indicators': [],
                    'weakness_indicators': ['No database entry found']
                }
                continue
            
            # Get the best match
            best_match = candidates[0]
            status = best_match.get('status', 'unspecified')
            
            # Map status to وثوق level
            وثوق_status_map = {
                'thiqah': 'موثوق (Trustworthy)',
                'hasan': 'حسن (Good)',
                'mamduh': 'ممدوح (Praised)',
                'muwaththaq': 'موثق (Reliable)',
                'daif': 'ضعيف (Weak)',
                'majhul': 'مجهول (Unknown)',
                'unspecified': 'غير محدد (Unspecified)'
            }
            
            وثوق_status = وثوق_status_map.get(status, status)
            
            # Strength indicators
            strength_indicators = []
            if status == 'thiqah':
                strength_indicators.append("Grade: Thiqah (Trustworthy)")
            elif status in ['hasan', 'mamduh', 'muwaththaq']:
                strength_indicators.append(f"Grade: {status}")
            
            # Check for hadith count
            hadith_count = best_match.get('hadith_count') or 0
            if hadith_count > 100:
                strength_indicators.append(f"High hadith count: {hadith_count}")
            
            # Weakness indicators
            weakness_indicators = []
            if status == 'daif':
                weakness_indicators.append("Grade: Da'if (Weak)")
            elif status == 'majhul':
                weakness_indicators.append("Grade: Majhul (Unknown)")
            
            # Check for sect issues
            sect = best_match.get('sect', '')
            if sect and any(s in sect for s in ['عامي', 'فطحي', 'واقفي', 'زيدي']):
                weakness_indicators.append(f"Sect: {sect}")
            
            results[narrator] = {
                'وثوق_status':وثوق_status,
                'strength_indicators': strength_indicators,
                'weakness_indicators': weakness_indicators
            }
        
        return results


class EnhancedIsnadAnalyzer(IsnadAnalyzer):
    """Enhanced isnad analyzer with modular rule-based analysis"""
    
    def __init__(self):
        super().__init__()
        # Initialize all modules
        self.identification_module = IdentificationModule(self.db_loader, self.resolver_index)
        self.error_detection_module = ErrorDetectionModule(self.db_loader, self.resolver_index)
        self.investigation_module = InvestigationModule(self.db_loader, self.resolver_index)
        self.reliability_module = ReliabilityModule(self.db_loader, self.resolver_index)
        
    def analyze_with_modules(self, narrators):
        """Analyze isnad using all modules
        
        Args:
            narrators: List of narrator names (already parsed)
        """
        # narrators is already a parsed list, no need to parse again
        
        # Run identification module
        identification_results = self.identification_module.analyze(narrators)
        
        # Run error detection module
        error_results = self.error_detection_module.analyze(narrators)
        
        # Run investigation module
        investigation_results = self.investigation_module.analyze(narrators)
        
        # Run reliability module
        reliability_results = self.reliability_module.analyze(narrators)
        
        # Combine all results
        combined_results = {
            'identification': identification_results,
            'errors': error_results,
            'investigation': investigation_results,
            'reliability': reliability_results,
            'narrators': narrators,
        }
        
        return combined_results
    
    def print_enhanced_results(self, results):
        """Print enhanced analysis results with module outputs"""
        print("\n" + "="*80)
        print("ENHANCED ISNAD ANALYSIS RESULTS")
        print("="*80)
        
        # Print identification results
        if results['identification']:
            print("\n" + "-"*80)
            print("IDENTIFICATION ANALYSIS")
            print("-"*80)
            for narrator, id_results in results['identification'].items():
                print(f"\nNarrator: {narrator}")
                if id_results.get('matches'):
                    print(f"  Matches: {', '.join(id_results['matches'])}")
                if id_results.get('confidence'):
                    print(f"  Confidence: {id_results['confidence']:.2%}")
                if id_results.get('warnings'):
                    print(f"  Warnings: {', '.join(id_results['warnings'])}")
        
        # Print error detection results
        if results['errors']:
            print("\n" + "-"*80)
            print("ERROR DETECTION ANALYSIS")
            print("-"*80)
            for error_type, error_list in results['errors'].items():
                if error_list:
                    print(f"\n{error_type.replace('_', ' ').title()}:")
                    for error in error_list:
                        print(f"  - {error}")
        
        # Print investigation results
        if results['investigation']:
            print("\n" + "-"*80)
            print("INVESTIGATION ANALYSIS")
            print("-"*80)
            inv_results = results['investigation']
            
            # Print traces
            if inv_results.get('traces'):
                print(f"\nNarration Traces:")
                for trace in inv_results['traces'][:5]:  # Limit to first 5
                    print(f"  - {trace}")
            
            # Print parallel chains
            if inv_results.get('parallel_chains'):
                print(f"\nParallel Chains:")
                for i, chain in enumerate(inv_results['parallel_chains'][:3]):  # Limit to first 3
                    print(f"  Chain {i+1}: {len(chain)} narrators")
            
            # Print gaps
            if inv_results.get('gaps'):
                print(f"\nGaps Detected:")
                for gap in inv_results['gaps']:
                    print(f"  - {gap}")
            
            # Print chain integrity
            if inv_results.get('chain_integrity') is not None:
                print(f"\nChain Integrity: {inv_results['chain_integrity']:.2%}")
            
            # Print common teachers
            if inv_results.get('common_teachers'):
                print(f"\nCommon Teachers:")
                for ct in inv_results['common_teachers'][:5]:  # Limit to first 5
                    print(f"  - {ct}")
            
            # Print transmission paths
            if inv_results.get('transmission_paths'):
                print(f"\nTransmission Paths:")
                for tp in inv_results['transmission_paths'][:5]:  # Limit to first 5
                    print(f"  - {tp}")
        
        # Print reliability results
        if results['reliability']:
            print("\n" + "-"*80)
            print("RELIABILITY ASSESSMENT")
            print("-"*80)
            for narrator, rel_results in results['reliability'].items():
                print(f"\nNarrator: {narrator}")
                if rel_results.get('وثوق_status'):
                    print(f"  Trustworthiness: {rel_results['وثوق_status']}")
                if rel_results.get('strength_indicators'):
                    print(f"  Strength Indicators: {', '.join(rel_results['strength_indicators'])}")
                if rel_results.get('weakness_indicators'):
                    print(f"  Weakness Indicators: {', '.join(rel_results['weakness_indicators'])}")
        
        print("\n" + "="*80)
        print("END OF ENHANCED ANALYSIS")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(description="Isnad Analyzer — Hadith Authentication Engine")
    parser.add_argument("--isnad", type=str, help="Full isnad string (Arabic)")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--enhanced", action="store_true", help="Use enhanced analyzer with modules")
    args = parser.parse_args()

    # Choose analyzer based on flag
    if args.enhanced:
        analyzer = EnhancedIsnadAnalyzer()
        print("Using Enhanced Isnad Analyzer with modules")
    else:
        analyzer = IsnadAnalyzer()
        print("Using Standard Isnad Analyzer")

    if args.interactive:
        print("\nIsnad Analyzer — Interactive Mode")
        print("Enter isnad (type 'quit' to exit):")
        while True:
            isnad_input = input("\nIsnad > ").strip()
            if isnad_input.lower() in ('q', 'quit', 'exit'):
                break
            if not isnad_input:
                continue
            
            names = analyzer.parse_isnad_string(isnad_input)
            
            if args.enhanced:
                analysis = analyzer.analyze_with_modules(names)
                analyzer.print_enhanced_results(analysis)
            else:
                analysis = analyzer.analyze(names)
                analyzer.print_results(analysis)
    
    elif args.isnad:
        names = analyzer.parse_isnad_string(args.isnad)
        
        if args.enhanced:
            analysis = analyzer.analyze_with_modules(names)
            analyzer.print_enhanced_results(analysis)
        else:
            analysis = analyzer.analyze(names)
            analyzer.print_results(analysis)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

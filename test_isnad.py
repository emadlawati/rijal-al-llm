#!/usr/bin/env python3
"""
Regression Test Suite for Isnad Analyzer
========================================

Tests the critical components:
1. Arabic text normalization
2. Isnad boundary extraction
3. Chain parsing
4. Narrator resolution
5. Full grading pipeline
6. Tabaqah inference

Run with: python test_isnad.py -v
"""

import sys
import io
import unittest
import json
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

from rijal_resolver import normalize_ar, tokenize
from isnad_extractor import extract_isnad
from isnad_parser import IsnadTokenizer, IsnadParser, parse_isnad_new


def n(s):
    """Shortcut for normalize_ar in tests."""
    return normalize_ar(s)


# ── Test Arabic Normalization ───────────────────────────────────────────────

class TestNormalization(unittest.TestCase):
    """Tests for rijal_resolver.normalize_ar()."""

    def test_alef_variants(self):
        """All alef variants should collapse to ا."""
        variants = ['أحمد', 'إحمد', 'آحمد', 'ٱحمد']
        for v in variants:
            self.assertEqual(normalize_ar(v), 'احمد')

    def test_tashkeel_removal(self):
        """Diacritics should be stripped."""
        self.assertEqual(normalize_ar('مُحَمَّدٌ'), 'محمد')

    def test_tatweel_removal(self):
        """Tatweel (kashida) should be removed."""
        self.assertEqual(normalize_ar('مـحـمـد'), 'محمد')

    def test_split_allah_repair(self):
        """Quranic split-الله should be repaired."""
        self.assertEqual(normalize_ar('الل ه'), 'الله')

    def test_nisabur_normalization(self):
        """نيسابور → نيشابور."""
        self.assertEqual(normalize_ar('من نيسابور'), 'من نيشابور')

    def test_rahman_normalization(self):
        """رحمان → رحمن."""
        self.assertEqual(normalize_ar('عبد الرحمان'), 'عبد الرحمن')

    def test_whitespace_collapse(self):
        """Multiple spaces should collapse to one."""
        self.assertEqual(normalize_ar('أحمد   بن   محمد'), 'احمد بن محمد')

    def test_abn_preservation(self):
        """The [ا-ي] regex bug: don't break ابن into ا بن."""
        result = normalize_ar('ابن محمد')
        self.assertIn('ابن', result)
        self.assertNotIn('ا بن', result)

    def test_complex_name(self):
        """Complex real-world name."""
        name = 'أَحْمَدُ بْنُ مُحَمَّدِ بْنِ عِيسَى الأَشْعَرِيُّ'
        result = normalize_ar(name)
        self.assertEqual(result, 'احمد بن محمد بن عيسى الاشعري')


# ── Test Isnad Extraction ───────────────────────────────────────────────────

class TestIsnadExtraction(unittest.TestCase):
    """Tests for isnad_extractor.extract_isnad()."""

    def test_basic_chain(self):
        """Simple chain ending with Imam Sadiq."""
        text = (
            "أحمد بن محمد، عن الحسين بن سعيد، عن ابن أبي عمير، "
            "عن عمر بن اذينة، عن أبي عبد الله (ع) قال: الحلال بين"
        )
        result = extract_isnad(text)
        self.assertIn(n('أبي عبد الله'), result)
        self.assertNotIn(n('الحلال'), result)

    def test_early_qal(self):
        """Chain where قال appears early but isn't the matn boundary."""
        text = (
            "1 - محمد بن يحيى، عن أحمد بن محمد، عن الوشاء، "
            "عن أبان بن عثمان، عن زرارة، عن أبي جعفر (ع) قال"
        )
        result = extract_isnad(text)
        self.assertIn(n('زرارة'), result)
        self.assertIn(n('أبي جعفر'), result)

    def test_group_narrator(self):
        """Chain starting with group narrator."""
        text = (
            "عدة من أصحابنا، عن أحمد بن محمد، عن ابن أبي عمير، "
            "عن أبي عبد الله (ع) قال"
        )
        result = extract_isnad(text)
        self.assertIn(n('عدة من أصحابنا'), result)

    def test_kulayni_header(self):
        """Strip al-Kulayni book header."""
        text = (
            "أخبرنا أبو جعفر محمد بن يعقوب الكليني قال حدثني "
            "أحمد بن إدريس، عن محمد بن عبد الجبار، عن صفوان، "
            "عن أبي الحسن الرضا (ع) قال"
        )
        result = extract_isnad(text)
        self.assertNotIn(n('الكليني'), result)
        self.assertIn(n('أبي الحسن الرضا'), result)

    def test_no_imam_fallback(self):
        """Text without clear Imam — should return best candidate."""
        text = "أحمد بن محمد، عن الحسين بن سعيد قال شيء ما"
        result = extract_isnad(text)
        self.assertGreater(len(result), 20)


# ── Test Recursive Parser ───────────────────────────────────────────────────

class TestIsnadParser(unittest.TestCase):
    """Tests for isnad_parser recursive descent parser."""

    def test_tokenize_basic(self):
        """Basic tokenization."""
        text = "أحمد بن محمد، عن الحسين بن سعيد"
        tokens = list(IsnadTokenizer(text))
        types = [t.type.name for t in tokens]
        self.assertIn('CONNECTOR', types)
        self.assertIn('NAME', types)

    def test_parse_basic_chain(self):
        """Parse a simple chain."""
        text = "أحمد بن محمد، عن الحسين بن سعيد، عن ابن أبي عمير"
        segments = parse_isnad_new(text)
        self.assertGreaterEqual(len(segments), 3)
        for seg in segments:
            self.assertIsInstance(seg, str)
            self.assertGreater(len(seg), 0)

    def test_parse_group_narrator(self):
        """Parse chain with group narrator."""
        text = "عدة من أصحابنا، عن أحمد بن محمد"
        segments = parse_isnad_new(text)
        # Parser normalizes text
        self.assertEqual(segments[0], n('عدة من أصحابنا'))

    def test_parse_father_reference(self):
        """Parse chain with 'ابيه' (his father)."""
        text = "أحمد بن محمد، عن أبيه، عن أبي عبد الله"
        segments = parse_isnad_new(text)
        self.assertIn('ابيه', segments)

    def test_parse_complex_connectors(self):
        """Parse chain with complex connectors like 'بإسناده عن'."""
        text = "بإسناده عن أحمد بن محمد، عن أبي عبد الله"
        segments = parse_isnad_new(text)
        self.assertGreaterEqual(len(segments), 2)
        # Parser normalizes text
        self.assertIn(n('أحمد بن محمد'), segments)

    def test_parse_honorifics(self):
        """Parse chain with Imam honorifics."""
        text = "عن أبي عبد الله (ع) قال"
        segments = parse_isnad_new(text)
        self.assertTrue(any('عبد الله' in s for s in segments))


# ── Test Full Pipeline ──────────────────────────────────────────────────────

class TestFullPipeline(unittest.TestCase):
    """Integration tests for the full analyzer pipeline."""

    @classmethod
    def setUpClass(cls):
        from isnad_analyzer import IsnadAnalyzer
        cls.analyzer = IsnadAnalyzer()

    def test_sahih_chain(self):
        """A known Sahih chain should grade as Sahih."""
        chain = [
            "أحمد بن محمد",
            "الحسين بن سعيد",
            "ابن أبي عمير",
            "عمر بن اذينة",
            "أبي عبد الله"
        ]
        analysis = self.analyzer.analyze(chain)
        self.assertEqual(analysis['final_status']['grade'], 'Sahih (Authentic)')

    def test_iddah_group(self):
        """Chain starting with 'عدة من أصحابنا' should be handled."""
        chain = ["عدة من أصحابنا", "أحمد بن محمد", "أبي عبد الله"]
        analysis = self.analyzer.analyze(chain)
        self.assertGreater(len(analysis['chain']), 0)

    def test_father_resolution(self):
        """Chain with 'ابيه' should try to resolve father."""
        chain = ["أحمد بن محمد", "ابيه", "أبي عبد الله"]
        analysis = self.analyzer.analyze(chain)
        second = analysis['chain'][1]
        self.assertIn('resolution', second)

    def test_imam_termination(self):
        """Chain should be truncated at first Imam."""
        text = "أحمد بن محمد، عن أبي عبد الله، عن شيء آخر"
        parsed = self.analyzer.parse_isnad_string(text)
        self.assertIn('عبد الله', parsed[-1])

    def test_tabaqah_gaps(self):
        """Chain with tabaqah info should produce gap analysis."""
        chain = ["أحمد بن محمد", "أبي عبد الله"]
        analysis = self.analyzer.analyze(chain)
        self.assertIn('tabaqah_gaps', analysis)


# ── Test Tabaqah Inference ──────────────────────────────────────────────────

class TestTabaqahInference(unittest.TestCase):
    """Tests for tabaqah_inference engine."""

    def test_imam_anchor(self):
        """Imam names should map to correct tabaqah."""
        from tabaqah_inference import IMAM_TABAQAH
        self.assertEqual(IMAM_TABAQAH['IMAM_BAQIR'], 4)
        self.assertEqual(IMAM_TABAQAH['IMAM_SADIQ'], 5)
        self.assertEqual(IMAM_TABAQAH['IMAM_KADHIM'], 6)

    def test_propagation_logic(self):
        """A narrator who narrates FROM Imam X is in Imam X's tabaqah.

        Per al-Burujirdi's system (and the AH ranges in alf_rajul_extractor.py),
        T5 == "Companions of Imam al-Sadiq" — companions and the Imam share
        the same tabaqah. So a narrator from al-Sadiq (T5) is himself T5.
        """
        db = {
            'test1': {
                'name_ar': 'أحمد بن محمد',
                'narrates_from_imams': ['أبي عبد الله (ع)'],
                'narrates_from_narrators': [],
                'tabaqah': None,
            }
        }
        from tabaqah_inference import TabaqahInferenceEngine
        engine = TabaqahInferenceEngine(db)
        result = engine.infer_all()
        self.assertIn('test1', result)
        self.assertEqual(result['test1']['tabaqah'], 5)


# ── Known Bug Regression Tests ─────────────────────────────────────────────

class TestKnownBugs(unittest.TestCase):
    """Regression tests for previously-fixed bugs."""

    def test_abn_regex_bug(self):
        """The [ا-ي] regex range bug."""
        text = 'ابن محبوب'
        from isnad_analyzer import IsnadAnalyzer
        analyzer = IsnadAnalyzer()
        names = analyzer.parse_isnad_string(text)
        for name in names:
            self.assertNotEqual(name.strip(), 'ا')

    def test_abn_normalization_order(self):
        """The ابن→بن normalization must happen AFTER compound-split."""
        text = 'ابن محبوب عن أحمد'
        from isnad_analyzer import IsnadAnalyzer
        analyzer = IsnadAnalyzer()
        names = analyzer.parse_isnad_string(text)
        self.assertTrue(
            any('محبوب' in n for n in names),
            f"Expected 'محبوب' in names, got {names}"
        )

    def test_faqal_matn_leak(self):
        """Segments containing فقال must be skipped, not stripped."""
        from isnad_analyzer import IsnadAnalyzer
        analyzer = IsnadAnalyzer()
        statuses = analyzer.grade_chain([
            {'original_query': 'أحمد بن محمد', 'resolution': {
                'top_match': {'status': 'thiqah', 'canonical_key': '123', 'entry_key': '123'}
            }},
            {'original_query': 'فقال الرجل', 'resolution': {
                'top_match': None
            }},
        ])
        self.assertIn('Sahih', statuses['grade'])


# ── Smoke Test: Run on Real Data ────────────────────────────────────────────

class TestRealDataSmoke(unittest.TestCase):
    """Quick smoke tests against real allBooks.json data."""

    def test_load_allbooks(self):
        """allBooks.json should load."""
        path = Path('allBooks.json')
        if not path.exists():
            self.skipTest('allBooks.json not found')
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.assertGreater(len(data), 1000)

    def test_load_rijal_db(self):
        """rijal_database_merged.json should load."""
        path = Path('rijal_database_merged.json')
        if not path.exists():
            self.skipTest('rijal_database_merged.json not found')
        with open(path, 'r', encoding='utf-8') as f:
            db = json.load(f)
        self.assertGreater(len(db), 1000)

    def test_extract_and_parse(self):
        """Extract isnad from a real hadith and parse it."""
        path = Path('allBooks.json')
        if not path.exists():
            self.skipTest('allBooks.json not found')
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        kafi = [h for h in data if 'kafi' in h.get('bookId', '').lower()]
        if not kafi:
            self.skipTest('No Kafi hadiths found')
        hadith = kafi[0]
        arabic = hadith.get('arabicText', '')
        if not arabic:
            self.skipTest('No Arabic text')
        isnad = extract_isnad(arabic)
        self.assertGreater(len(isnad), 10)
        names = parse_isnad_new(isnad)
        self.assertGreaterEqual(len(names), 1)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

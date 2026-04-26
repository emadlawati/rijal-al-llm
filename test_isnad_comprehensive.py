#!/usr/bin/env python3
"""
Comprehensive Test Suite for Isnad Analyzer (Phase 2)
======================================================

Tests the enhanced tabaqah inference and irsal detection features.
"""

import json
import sys
from pathlib import Path
from isnad_analyzer import IsnadAnalyzer

def test_case(name, isnad, expected_grade, expected_irsal=None, expected_gap=None):
    """Run a single test case and report results."""
    print(f"\n{'='*80}")
    print(f"TEST: {name}")
    print(f"ISNAD: {isnad}")
    print(f"{'='*80}")
    
    analyzer = IsnadAnalyzer()
    names = analyzer.parse_isnad_string(isnad)
    analysis = analyzer.analyze(names)
    analyzer.print_results(analysis)
    
    grade = analysis['final_status']['grade']
    gaps = analysis.get('tabaqah_gaps', [])
    
    # Check grade
    grade_match = expected_grade in grade
    print(f"\n✓ Grade Check: Expected '{expected_grade}' → Got '{grade}' → {'PASS' if grade_match else 'FAIL'}")
    
    # Check irsal detection
    if expected_irsal:
        has_irsal = any('إرسال' in str(g.get('note', '')) for g in gaps)
        irsal_match = has_irsal == expected_irsal
        print(f"✓ Irsal Detection: Expected {expected_irsal} → Got {has_irsal} → {'PASS' if irsal_match else 'FAIL'}")
    else:
        irsal_match = True
    
    # Check gap detection
    if expected_gap:
        has_gap = len(gaps) > 0
        gap_match = has_gap == expected_gap
        print(f"✓ Gap Detection: Expected {expected_gap} → Got {has_gap} → {'PASS' if gap_match else 'FAIL'}")
    else:
        gap_match = True
    
    return grade_match and irsal_match and gap_match


def main():
    print("="*80)
    print("COMPREHENSIVE TEST SUITE FOR ISNAD ANALYZER (PHASE 2)")
    print("="*80)
    
    tests = [
        {
            "name": "Test 1: Standard Chain (Sahih)",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة",
            "expected_grade": "Sahih",
            "expected_irsal": False,
            "expected_gap": True,  # Has tabaqah gap but still sahih
        },
        {
            "name": "Test 2: Mursal Chain (Anonymous Narrator)",
            "isnad": "عمن رواه، عن زرارة، عن أبي جعفر",
            "expected_grade": "Majhul",
            "expected_irsal": True,
            "expected_gap": False,
        },
        {
            "name": "Test 3: Chain with Imam",
            "isnad": "أحمد بن محمد، عن علي بن الحسين، عن زرارة",
            "expected_grade": "Sahih",
            "expected_irsal": False,
            "expected_gap": False,
        },
        {
            "name": "Test 4: Short Chain",
            "isnad": "زرارة، عن أبي جعفر",
            "expected_grade": "Sahih",
            "expected_irsal": False,
            "expected_gap": False,
        },
        {
            "name": "Test 5: Multiple Narrators",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد، عن محمد بن سنان، عن زرارة",
            "expected_grade": "Sahih",
            "expected_irsal": False,
            "expected_gap": True,
        },
    ]
    
    results = []
    for test in tests:
        result = test_case(
            test["name"],
            test["isnad"],
            test["expected_grade"],
            test.get("expected_irsal"),
            test.get("expected_gap")
        )
        results.append((test["name"], result))
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"\nPassed: {passed}/{total}")
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {name}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n⚠ {total - passed} TEST(S) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())

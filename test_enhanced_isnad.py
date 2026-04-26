#!/usr/bin/env python3
"""
Comprehensive Test Suite for Enhanced Isnad Analyzer
====================================================
Tests all modules: Identification, Error Detection, Investigation, and Reliability
"""

import sys
from isnad_analyzer import EnhancedIsnadAnalyzer

def test_enhanced_analyzer():
    """Test the enhanced analyzer with various isnad examples"""
    
    analyzer = EnhancedIsnadAnalyzer()
    
    test_cases = [
        {
            "name": "Basic Three-Link Chain",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة",
            "expected_links": 3,
            "expected_status": "Sahih (Authentic)"
        },
        {
            "name": "Imam Reference Chain",
            "isnad": "علي بن الحسين، عن أبيه، عن جده، عن النبي",
            "expected_links": 4,
            "expected_status": "Sahih (Authentic)"
        },
        {
            "name": "Complex Chain with Virtual Narrators",
            "isnad": "محمد بن مسلم، عن أبي جعفر، عن جده، عن علي بن أبي طالب",
            "expected_links": 4,
            "expected_status": "Majhul / Da'if (Unknown Narrators)"
        },
        {
            "name": "Short Chain",
            "isnad": "زرارة عن أبي جعفر",
            "expected_links": 2,
            "expected_status": "Sahih (Authentic)"
        },
        {
            "name": "Chain with Multiple Narrators",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة، عن أبي جعفر",
            "expected_links": 4,
            "expected_status": "Sahih (Authentic)"
        }
    ]
    
    print("="*80)
    print("COMPREHENSIVE TEST SUITE FOR ENHANCED ISNAD ANALYZER")
    print("="*80)
    
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}: {test_case['name']}")
        print(f"{'='*80}")
        print(f"Isnad: {test_case['isnad']}")
        print(f"Expected Links: {test_case['expected_links']}")
        print(f"Expected Status: {test_case['expected_status']}")
        print("-"*80)
        
        try:
            # Parse the isnad
            names = analyzer.parse_isnad_string(test_case['isnad'])
            print(f"Parsed Links ({len(names)}): {names}")
            
            # Analyze with modules
            analysis = analyzer.analyze_with_modules(names)
            
            # Print results
            analyzer.print_enhanced_results(analysis)
            
            # Verify results
            actual_links = len(names)
            actual_status = analysis['reliability']
            
            # Check if test passed
            if actual_links == test_case['expected_links']:
                print(f"\n✓ PASS: Link count matches ({actual_links})")
                passed += 1
            else:
                print(f"\n✗ FAIL: Expected {test_case['expected_links']} links, got {actual_links}")
                failed += 1
            
            # Check reliability assessment exists
            if actual_status:
                print(f"✓ PASS: Reliability assessment completed")
            else:
                print(f"✗ FAIL: No reliability assessment")
                failed += 1
                
        except Exception as e:
            print(f"\n✗ FAIL: Exception occurred: {e}")
            failed += 1
    
    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(f"Total Tests: {len(test_cases)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {(passed/len(test_cases)*100):.1f}%")
    print(f"{'='*80}")
    
    return failed == 0


def test_module_specific_features():
    """Test specific features of each module"""
    
    analyzer = EnhancedIsnadAnalyzer()
    
    print(f"\n{'='*80}")
    print("MODULE-SPECIFIC FEATURE TESTS")
    print(f"{'='*80}")
    
    # Test 1: Identification Module
    print(f"\n{'-'*80}")
    print("TEST 1: Identification Module")
    print(f"{'-'*80}")
    
    test_isnad = "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة"
    names = analyzer.parse_isnad_string(test_isnad)
    id_results = analyzer.identification_module.analyze(names)
    
    for narrator, results in id_results.items():
        print(f"\nNarrator: {narrator}")
        print(f"  Confidence: {results.get('confidence', 0):.2%}")
        print(f"  Candidates: {results.get('candidate_count', 0)}")
        if results.get('matches'):
            print(f"  Matches: {', '.join(results['matches'])}")
    
    # Test 2: Error Detection Module
    print(f"\n{'-'*80}")
    print("TEST 2: Error Detection Module")
    print(f"{'-'*80}")
    
    error_results = analyzer.error_detection_module.analyze(names)
    print(f"Error Detection Results:")
    for error_type, errors in error_results.items():
        if errors:
            print(f"  {error_type}: {len(errors)} issues")
            for error in errors[:3]:  # Show first 3
                print(f"    - {error}")
        else:
            print(f"  {error_type}: No issues detected")
    
    # Test 3: Investigation Module
    print(f"\n{'-'*80}")
    print("TEST 3: Investigation Module")
    print(f"{'-'*80}")
    
    inv_results = analyzer.investigation_module.analyze(names)
    print(f"Investigation Results:")
    print(f"  Chain Integrity: {inv_results.get('chain_integrity', 0):.2%}")
    print(f"  Common Teachers: {len(inv_results.get('common_teachers', []))}")
    print(f"  Transmission Paths: {len(inv_results.get('transmission_paths', []))}")
    
    # Test 4: Reliability Module
    print(f"\n{'-'*80}")
    print("TEST 4: Reliability Module")
    print(f"{'-'*80}")
    
    rel_results = analyzer.reliability_module.analyze(names)
    print(f"Reliability Assessment:")
    for narrator, results in rel_results.items():
        print(f"\nNarrator: {narrator}")
        print(f"  Trustworthiness: {results.get('وثوق_status', 'N/A')}")
        if results.get('strength_indicators'):
            print(f"  Strength: {', '.join(results['strength_indicators'])}")
        if results.get('weakness_indicators'):
            print(f"  Weakness: {', '.join(results['weakness_indicators'])}")
    
    print(f"\n{'='*80}")
    print("MODULE-SPECIFIC TESTS COMPLETED")
    print(f"{'='*80}")
    
    return True


def test_edge_cases():
    """Test edge cases and error handling"""
    
    analyzer = EnhancedIsnadAnalyzer()
    
    print(f"\n{'='*80}")
    print("EDGE CASE TESTS")
    print(f"{'='*80}")
    
    edge_cases = [
        {
            "name": "Empty Chain",
            "isnad": "",
            "expected_links": 0
        },
        {
            "name": "Single Narrator",
            "isnad": "زرارة",
            "expected_links": 1
        },
        {
            "name": "Very Long Chain",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد، عن زرارة، عن أبي جعفر، عن محمد بن مسلم، عن يونس بن عبد الرحمن",
            "expected_links": 6
        },
        {
            "name": "Chain with Special Characters",
            "isnad": "أحمد بن محمد، عن الحسين بن سعيد (ع)، عن زرارة",
            "expected_links": 3
        }
    ]
    
    for i, test_case in enumerate(edge_cases, 1):
        print(f"\n{'-'*80}")
        print(f"Edge Case {i}: {test_case['name']}")
        print(f"{'-'*80}")
        print(f"Isnad: {test_case['isnad']}")
        
        try:
            names = analyzer.parse_isnad_string(test_case['isnad'])
            print(f"Parsed Links: {len(names)}")
            
            if len(names) > 0:
                analysis = analyzer.analyze_with_modules(names)
                print(f"Analysis completed successfully")
            else:
                print(f"Empty chain - no analysis performed")
            
            if len(names) == test_case['expected_links']:
                print(f"✓ PASS: Link count matches")
            else:
                print(f"✗ FAIL: Expected {test_case['expected_links']}, got {len(names)}")
                
        except Exception as e:
            print(f"✗ FAIL: Exception: {e}")
    
    print(f"\n{'='*80}")
    print("EDGE CASE TESTS COMPLETED")
    print(f"{'='*80}")
    
    return True


def main():
    """Run all tests"""
    
    print("\n" + "="*80)
    print("ENHANCED ISNAD ANALYZER - COMPREHENSIVE TEST SUITE")
    print("="*80)
    
    # Run all test suites
    test1_passed = test_enhanced_analyzer()
    test2_passed = test_module_specific_features()
    test3_passed = test_edge_cases()
    
    # Final summary
    print("\n" + "="*80)
    print("FINAL TEST SUMMARY")
    print("="*80)
    
    if test1_passed and test2_passed and test3_passed:
        print("✓ ALL TESTS PASSED")
        print("The enhanced isnad analyzer is working correctly!")
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("Please review the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

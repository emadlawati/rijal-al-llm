#!/usr/bin/env python3
"""
Practical Rules for Isnad Analysis
===================================

Extracted from "المهام الخمس" book insights
Provides identification and scribal error detection rules
"""

import re
from typing import Dict, List, Optional, Tuple, Any


class IdentificationRules:
    """
    Identification rules (118 rules) for narrator identification
    """
    
    @staticmethod
    def apply_layer_clue(narrator: str, chain: List[str], index: int) -> Optional[Dict]:
        """
        قرينة الطبقة: Use generation/clan layer to identify narrator
        
        Rule 19-20: Layer is most important clue for identification
        Rule 23: Use layer to distinguish between shared names
        """
        # Get expected layer range from neighbors
        if index > 0:
            prev_narrator = chain[index - 1]
            # Estimate previous narrator's layer
            prev_layer = IdentificationRules.estimate_layer(prev_narrator)
            if prev_layer:
                # Narrator should be 1-2 layers after previous
                expected_min = prev_layer + 1
                expected_max = prev_layer + 3
                return {
                    "clue": "layer",
                    "expected_range": (expected_min, expected_max),
                    "source": "neighbor_layer"
                }
        return None
    
    @staticmethod
    def apply_student_teacher_clue(narrator: str, teacher: str) -> Optional[Dict]:
        """
        قرينة التلمذة: Identify narrator by who they studied under
        
        Rule 28: If student narrates from shared teacher, frequent narration reveals teacher
        """
        # Check if narrator is known student of teacher
        # This would require database lookup
        return {
            "clue": "student_teacher",
            "teacher": teacher,
            "strength": "medium"
        }
    
    @staticmethod
    def apply_teacher_student_clue(narrator: str, student: str) -> Optional[Dict]:
        """
        قرينة المشيخة: Identify narrator by who studied under them
        
        Rule 29: Teacher's name reveals student's identity
        """
        return {
            "clue": "teacher_student",
            "student": student,
            "strength": "medium"
        }
    
    @staticmethod
    def apply_intermediary_clue(prev_narrator: str, next_narrator: str) -> Optional[Dict]:
        """
        قرينة التوسّط: Identify middle narrator in chain of three
        
        Rule 30: Most common and reliable identification method
        Rule 39: Standard intermediary pattern
        """
        return {
            "clue": "intermediary",
            "prev": prev_narrator,
            "next": next_narrator,
            "strength": "high"
        }
    
    @staticmethod
    def apply_chain_clue(chain: List[str]) -> Optional[Dict]:
        """
        قرينة السلسلة: Repeated chain identifies all members
        
        Rule 41: Strongest identification clue
        Rule 59: Chain pattern reveals narrator identity
        """
        if len(chain) >= 3:
            return {
                "clue": "chain",
                "length": len(chain),
                "strength": "very_high"
            }
        return None
    
    @staticmethod
    def apply_rarity_clue(narrator: str) -> Optional[Dict]:
        """
        قرينة الندرة: Rare names are easily identified
        
        Rule 42: Rare/unusual names need no other clues
        Rule 50: Examples: مشمعل، معتّب، منخل، أرطأة، صندل، منبّه
        """
        rare_names = [
            "مشمعل", "معتب", "منخل", "أرطأة", "صندل", "منبه",
            "ارطأة", "معتب", "منخل", "صندل"
        ]
        
        # Check if name contains rare patterns
        is_rare = any(rare in narrator for rare in rare_names)
        
        return {
            "clue": "rarity",
            "is_rare": is_rare,
            "strength": "very_high" if is_rare else "low"
        }
    
    @staticmethod
    def apply_context_clue(narrator: str, chain_context: Dict) -> Optional[Dict]:
        """
        قرينة السياق: Use consistent patterns in narrations
        
        Rule 55: Same teacher, question style, etc.
        """
        return {
            "clue": "context",
            "patterns": chain_context.get("patterns", []),
            "strength": "medium"
        }
    
    @staticmethod
    def apply_topic_clue(narrator: str, topic: str) -> Optional[Dict]:
        """
        قرينة موضوع الرواية: Match narration topic to known books
        
        Rule 51: Divorce topics match Muhammad bin Abi Umair's book
        """
        # Topic-to-narrator mapping
        topic_map = {
            "divorce": "محمد بن أبي عمير",
            "marriage": "محمد بن أبي عمير",
            "inheritance": "Various narrators",
            "prayer": "Various narrators"
        }
        
        return {
            "clue": "topic",
            "topic": topic,
            "expected_narrator": topic_map.get(topic),
            "strength": "medium"
        }
    
    @staticmethod
    def apply_fame_clue(narrator: str, layer: int) -> Optional[Dict]:
        """
        قرينة الشهرة: Famous narrator in layer is default choice
        
        Rule 57: If one narrator is very famous, name defaults to them
        """
        return {
            "clue": "fame",
            "layer": layer,
            "strength": "medium"
        }
    
    @staticmethod
    def apply_place_clue(narrator: str, place: str) -> Optional[Dict]:
        """
        قرينة المكان: Use narrator's location to identify
        
        Rule 26: Location can help distinguish narrators
        """
        return {
            "clue": "place",
            "place": place,
            "strength": "low"
        }
    
    @staticmethod
    def apply_family_clue(narrator: str, relative: str) -> Optional[Dict]:
        """
        قرينة القرابة: Family relationships help identification
        
        Rule 61: e.g., Ali bin Hassan is nephew of Abdul Rahman
        """
        return {
            "clue": "family",
            "relative": relative,
            "relationship": "nephew",
            "strength": "medium"
        }
    
    @staticmethod
    def apply_madhab_clue(narrator: str, madhab: str) -> Optional[Dict]:
        """
        قرينة المذهب: Use sectarian affiliation
        
        Rule 62: Waqifi narrators vs others
        """
        return {
            "clue": "madhab",
            "madhab": madhab,
            "strength": "medium"
        }
    
    @staticmethod
    def estimate_layer(narrator: str) -> Optional[int]:
        """
        Estimate narrator's generation layer
        """
        # Simple estimation based on name patterns
        # In production, this would use database lookup
        if "ابن" in narrator or "بن" in narrator:
            # Likely later generation
            return 6  # Example
        return None


class ScribalErrorRules:
    """
    Scribal error detection rules (87 rules)
    """
    
    @staticmethod
    def detect_similar_writing_error(name1: str, name2: str) -> Optional[Dict]:
        """
        Rule 17: Detect errors due to similar writing in old script
        
        Examples:
        - خلف/خالد - similar in old script
        - هارون/مروان - similar writing
        """
        # Similar writing pairs in old Arabic script
        similar_pairs = [
            ("خلف", "خالد"),
            ("خلف", "خلد"),
            ("هارون", "مروان"),
            ("محمد", "أحمد"),
            ("عمر", "عمرو"),
            ("سالم", "سلم"),
            ("هلال", "مليك"),
        ]
        
        for pair in similar_pairs:
            if (name1.endswith(pair[0]) and name2.endswith(pair[1])) or \
               (name1.endswith(pair[1]) and name2.endswith(pair[0])):
                return {
                    "error_type": "similar_writing",
                    "pair": pair,
                    "severity": "medium"
                }
        return None
    
    @staticmethod
    def detect_dot_error(name: str) -> Optional[Dict]:
        """
        Rule 49: Detect dot placement errors
        
        Example: إسماعيل بن يسار → إسماعيل بن بشار
        """
        # Common dot errors
        dot_errors = [
            ("يسار", "بشار"),
            ("يسار", "بشار"),
        ]
        
        for wrong, correct in dot_errors:
            if wrong in name:
                return {
                    "error_type": "dot_error",
                    "wrong": wrong,
                    "correct": correct,
                    "severity": "high"
                }
        return None
    
    @staticmethod
    def detect_eye_skip(chain: List[str]) -> Optional[Dict]:
        """
        Rule 47: Detect eye skip due to repeated phrases
        
        Example: "صفوان بن يحيى الأزرق" - eye jumps between repeated "يحيى"
        """
        # Check for repeated words in consecutive narrators
        for i in range(len(chain) - 1):
            words1 = chain[i].split()
            words2 = chain[i + 1].split()
            
            # Check for repeated words
            for w1 in words1:
                if w1 in words2 and len(w1) > 2:  # Ignore short words
                    return {
                        "error_type": "eye_skip",
                        "repeated_word": w1,
                        "position": i,
                        "severity": "medium"
                    }
        return None
    
    @staticmethod
    def detect_reversal_error(name: str) -> Optional[Dict]:
        """
        Rule 16: Detect name part reversal
        
        Example: إبراهيم بن نعيم → نعيم بن إبراهيم
        """
        # Check if name parts are reversed
        if " بن " in name:
            parts = name.split(" بن ")
            if len(parts) == 2:
                # Check if reversed version exists in database
                reversed_name = f"{parts[1]} بن {parts[0]}"
                return {
                    "error_type": "reversal",
                    "original": name,
                    "reversed": reversed_name,
                    "severity": "high"
                }
        return None
    
    @staticmethod
    def detect_addition_error(name: str) -> Optional[Dict]:
        """
        Rule 15: Detect extra words added to name
        
        Example: غياث بن إبراهيم ← غياث بن كلوب (with added "بن إبراهيم")
        """
        # Check for suspicious additions
        suspicious_patterns = [
            r"بن\s+\S+\s+بن\s+",  # Double "بن"
            r"ابي\s+\S+\s+بن\s+",  # "ابي X بن Y"
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, name):
                return {
                    "error_type": "addition",
                    "name": name,
                    "severity": "medium"
                }
        return None
    
    @staticmethod
    def detect_omission_error(chain: List[str]) -> Optional[Dict]:
        """
        Rule 51: Detect omitted words
        
        Example: "عبد الله بن محمد بن خلف" ← "عبد الله بن محمد بن خالد"
        """
        # Check for missing intermediaries
        for i in range(len(chain) - 2):
            # If gap between layers is too large, may be omission
            # This would require layer estimation
            pass
        return None
    
    @staticmethod
    def detect_confusion_between(chain: List[str]) -> Optional[Dict]:
        """
        Rule 44-46: Detect confusion between similar terms
        
        Examples:
        - "عن" vs "بن"
        - "محمد" vs "أحمد"
        - "عمر" vs "عمرو"
        """
        confusion_pairs = [
            ("عن", "بن"),
            ("محمد", "أحمد"),
            ("عمر", "عمرو"),
        ]
        
        for i, narrator in enumerate(chain):
            for wrong, correct in confusion_pairs:
                if wrong in narrator and correct not in narrator:
                    # Check if correct version makes more sense
                    return {
                        "error_type": "confusion",
                        "wrong_term": wrong,
                        "correct_term": correct,
                        "position": i,
                        "severity": "medium"
                    }
        return None
    
    @staticmethod
    def detect_rare_name_error(name: str) -> Optional[Dict]:
        """
        Rule 39: Rare name not found in references indicates error
        
        Example: "أبان بن عيسى بن عبد الله القمي" - not in indexes
        """
        # Check if name is unusually rare
        rare_indicators = [
            "غريب", "نادر", "شاذ", "غير معروف"
        ]
        
        # In production, would check against database
        # For now, flag names with unusual patterns
        if len(name.split()) > 4:  # Very long name
            return {
                "error_type": "rare_name",
                "name": name,
                "severity": "low"
            }
        return None
    
    @staticmethod
    def detect_version_difference(chain1: List[str], chain2: List[str]) -> Optional[Dict]:
        """
        Rule 40: Detect differences between manuscript versions
        
        Example: Different versions of same chain
        """
        if chain1 != chain2:
            # Find differences
            differences = []
            for i, (n1, n2) in enumerate(zip(chain1, chain2)):
                if n1 != n2:
                    differences.append({
                        "position": i,
                        "version1": n1,
                        "version2": n2
                    })
            
            return {
                "error_type": "version_difference",
                "differences": differences,
                "severity": "medium"
            }
        return None


class IsnadPracticalRules:
    """
    Main class combining all practical rules for isnad analysis
    """
    
    def __init__(self):
        self.identification = IdentificationRules()
        self.scribal_errors = ScribalErrorRules()
    
    def analyze_chain_with_rules(self, chain: List[str]) -> Dict:
        """
        Analyze chain using all practical rules
        """
        results = {
            "identification_clues": [],
            "scribal_errors": [],
            "recommendations": []
        }
        
        # Apply identification rules
        for i, narrator in enumerate(chain):
            # Layer clue
            layer_info = self.identification.apply_layer_clue(narrator, chain, i)
            if layer_info:
                results["identification_clues"].append(layer_info)
            
            # Rarity clue
            rarity_info = self.identification.apply_rarity_clue(narrator)
            if rarity_info["is_rare"]:
                results["identification_clues"].append(rarity_info)
            
            # Intermediary clue (for middle narrators)
            if i > 0 and i < len(chain) - 1:
                intermediary_info = self.identification.apply_intermediary_clue(
                    chain[i-1], chain[i+1]
                )
                results["identification_clues"].append(intermediary_info)
        
        # Apply scribal error detection
        # Similar writing errors
        for i in range(len(chain) - 1):
            error = self.scribal_errors.detect_similar_writing_error(
                chain[i], chain[i+1]
            )
            if error:
                results["scribal_errors"].append(error)
        
        # Eye skip detection
        eye_skip = self.scribal_errors.detect_eye_skip(chain)
        if eye_skip:
            results["scribal_errors"].append(eye_skip)
        
        # Confusion detection
        confusion = self.scribal_errors.detect_confusion_between(chain)
        if confusion:
            results["scribal_errors"].append(confusion)
        
        # Generate recommendations
        if results["scribal_errors"]:
            results["recommendations"].append(
                "Check chain for scribal errors - differences detected between narrators"
            )
        
        if len(results["identification_clues"]) > 0:
            results["recommendations"].append(
                f"Applied {len(results['identification_clues'])} identification clues"
            )
        
        return results


# Utility functions
def normalize_ar(text: str) -> str:
    """Normalize Arabic text for comparison"""
    import re
    text = re.sub(r'[أإآٱ]', 'ا', text)
    text = re.sub(r'ـ', '', text)
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def estimate_layer(narrator: str) -> Optional[int]:
    """Estimate narrator's generation layer"""
    # Simple heuristic based on name patterns
    narrator_norm = normalize_ar(narrator)
    
    # Check for Imam references
    imam_patterns = [
        "ابي جعفر", "ابي عبد الله", "الباقر", "الصادق",
        "الكاظم", "الرضا", "الجواد", "الهادي", "العسکري"
    ]
    
    for pattern in imam_patterns:
        if pattern in narrator_norm:
            return 4  # Companion generation
    
    # Check for common narrator patterns
    if "ابن" in narrator_norm or "بن" in narrator_norm:
        return 6  # Later generation
    
    return None


if __name__ == "__main__":
    # Test the rules
    rules = IsnadPracticalRules()
    
    test_chain = [
        "أحمد بن محمد",
        "عن الحسين بن سعيد",
        "عن محمد بن سنان",
        "عن زرارة"
    ]
    
    results = rules.analyze_chain_with_rules(test_chain)
    print("Analysis Results:")
    print(f"Identification Clues: {len(results['identification_clues'])}")
    print(f"Scribal Errors: {len(results['scribal_errors'])}")
    print(f"Recommendations: {results['recommendations']}")

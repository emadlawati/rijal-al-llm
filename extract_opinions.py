import re
import json
import traceback

def extract():
    try:
        file_path = 'asanid kafi/ترتيب أسانيد الكافي ج١ - مصحح.md'
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        opinions = []
        current_page = 'Unknown'
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Check for page markers <!-- Page: X -->
            page_match = re.search(r'<!-- Page: (\d+) -->', line)
            if page_match:
                current_page = page_match.group(1)
                continue
            
            if line.startswith('*'):
                j = i
                opinion_text = []
                is_burujurdi = False
                while j < len(lines):
                    l = lines[j].strip()
                    opinion_text.append(l)
                    # Burujurdi opinions ends with ح ط.
                    if 'ح ط' in l:
                        is_burujurdi = True
                        break
                    
                    if not l and len(opinion_text) > 1: # empty line means end of paragraph maybe
                        break
                        
                    j += 1
                
                # Check the combined text just to be sure
                full_opinion = ' '.join(opinion_text)
                if 'ح ط' in full_opinion:
                    is_burujurdi = True
                    
                if is_burujurdi:
                    preceding_lines = []
                    k = i - 1
                    # Go up to find the chain
                    while k >= 0 and len(preceding_lines) < 3:
                        prev_line = lines[k].strip()
                        if prev_line and not prev_line.startswith('<!--') and not prev_line.startswith('*'):
                            preceding_lines.append(prev_line)
                        k -= 1
                    preceding_lines.reverse()
                    
                    opinions.append({
                        'page': current_page,
                        'preceding_context': preceding_lines,
                        'opinion': full_opinion
                    })

        with open('opinions_extracted.json', 'w', encoding='utf-8') as f:
            json.dump(opinions, f, ensure_ascii=False, indent=2)

        print(f'Extracted {len(opinions)} opinions to opinions_extracted.json')
    except Exception as e:
        traceback.print_exc()

if __name__ == '__main__':
    extract()

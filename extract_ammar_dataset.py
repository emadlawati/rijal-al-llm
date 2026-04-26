#!/usr/bin/env python3
"""
Extract Ammaar Muslim hadith dataset from allBooks.json
"""
import json
import os
from pathlib import Path

print('Starting Phase 1: Extracting Ammaar Muslim dataset...')

# Load the allBooks.json data
with open('allBooks.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f'Total hadiths in database: {len(data):,}')

# Filter for Ammaar Muslim translations
ammar_hadiths = []
for item in data:
    translator = item.get('translator', '')
    if 'ammaar' in translator.lower() and 'muslim' in translator.lower():
        ammar_hadiths.append(item)

print(f'Found {len(ammar_hadiths)} Ammaar Muslim hadiths')

# Create dataset directory
dataset_dir = Path('ammar_muslim_dataset')
dataset_dir.mkdir(exist_ok=True)

# Prepare training data in instruction format
training_data = []
for i, h in enumerate(ammar_hadiths):
    arabic = h.get('arabicText', '').strip()
    english = h.get('englishText', '').strip()
    
    if len(arabic) < 20 or len(english) < 20:
        continue
        
    training_example = {
        'id': i,
        'instruction': 'Translate this Arabic hadith to English in the style of Ammaar Muslim.',
        'input': arabic,
        'output': english,
        'metadata': {
            'book': h.get('book', ''),
            'translator': h.get('translator', ''),
            'chapter': h.get('chapter', ''),
            'category': h.get('category', ''),
            'hadith_id': h.get('id', '')
        }
    }
    training_data.append(training_example)

print(f'Prepared {len(training_data)} training examples')

# Split into train/validation/test (500/30/25)
train_data = training_data[:500]
val_data = training_data[500:530]
test_data = training_data[530:555]

# Save datasets
with open(dataset_dir / 'train.json', 'w', encoding='utf-8') as f:
    json.dump(train_data, f, ensure_ascii=False, indent=2)
    
with open(dataset_dir / 'validation.json', 'w', encoding='utf-8') as f:
    json.dump(val_data, f, ensure_ascii=False, indent=2)
    
with open(dataset_dir / 'test.json', 'w', encoding='utf-8') as f:
    json.dump(test_data, f, ensure_ascii=False, indent=2)

# Also save in plain text format for easy viewing
with open(dataset_dir / 'dataset_info.txt', 'w', encoding='utf-8') as f:
    f.write('Ammaar Muslim Hadith Translation Dataset\n')
    f.write('========================================\n\n')
    f.write(f'Total examples: {len(training_data)}\n')
    f.write(f'Training: {len(train_data)} examples\n')
    f.write(f'Validation: {len(val_data)} examples\n')
    f.write(f'Test: {len(test_data)} examples\n\n')
    
    f.write('Sample training example:\n')
    f.write(f'Instruction: {train_data[0]["instruction"]}\n')
    f.write(f'Input (Arabic): {train_data[0]["input"][:100]}...\n')
    f.write(f'Output (English): {train_data[0]["output"][:100]}...\n')

print(f'Dataset saved to {dataset_dir}/')
print(f'  - train.json ({len(train_data)} examples)')
print(f'  - validation.json ({len(val_data)} examples)')
print(f'  - test.json ({len(test_data)} examples)')
print(f'  - dataset_info.txt')

# Also create a CSV version for easy viewing
import csv
with open(dataset_dir / 'dataset.csv', 'w', encoding='utf-8', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['ID', 'Arabic', 'English', 'Book', 'Translator'])
    for example in training_data:
        writer.writerow([
            example['id'],
            example['input'][:200],  # First 200 chars
            example['output'][:200],  # First 200 chars
            example['metadata']['book'],
            example['metadata']['translator']
        ])
print(f'  - dataset.csv (for easy viewing)')
import json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
with open('mufid_statuses.json','r',encoding='utf-8') as f:
    data = json.load(f)

from collections import Counter
sc = Counter(e['status'] for e in data.values())
print('Final distribution from clean al-Mufid text:')
for s in ['thiqah','majhul','unspecified','daif','mamduh','hasan','muwaththaq']:
    c = sc.get(s,0)
    pct = 100*c/len(data)
    print(f'  {s:<15} {c:>6,} ({pct:>5.1f}%)')
print(f'  {"TOTAL":<15} {len(data):>6,}')
print()

# Check unspecified breakdown
unsp = [k for k in data if data[k]['status'] == 'unspecified']
has_xref = 0
has_source = 0
for k in unsp:
    e = data[k]
    src = e.get('status_source') or ''
    if src:
        has_source += 1

print(f'Unspecified with a source: {has_source}/{len(unsp)}')
print()

# Compare with old rijal_database.json
with open('rijal_database.json','r',encoding='utf-8') as f:
    old_db = json.load(f)

# Match by najaf number (the key in old_db is index 0-based?)
# Check what keys look like
old_keys = list(old_db.keys())[:5]
print(f'Old DB sample keys: {old_keys}')

# Let's check the match rate
matched = 0
agree = 0
disagree = 0
disagree_samples = []
for k, e in data.items():
    if k in old_db:
        matched += 1
        old_status = old_db[k].get('status', 'unspecified')
        new_status = e['status']
        if old_status == new_status:
            agree += 1
        else:
            disagree += 1
            if len(disagree_samples) < 20:
                old_name = old_db[k].get('name_ar','?')
                disagree_samples.append((k, old_name, old_status, new_status))

print(f'\nMatched entries: {matched:,}')
print(f'  Agree on status: {agree:,} ({100*agree/max(matched,1):.1f}%)')
print(f'  Disagree: {disagree:,} ({100*disagree/max(matched,1):.1f}%)')
print(f'\nDisagreement samples:')
for k, name, old_s, new_s in disagree_samples:
    mark = ''
    if old_s == 'majhul' and new_s == 'thiqah':
        mark = ' ** UPGRADE'
    elif old_s == 'thiqah' and new_s == 'majhul':
        mark = ' ** DOWNGRADE'
    elif old_s == 'majhul' and new_s == 'unspecified':
        mark = ' (was inferred majhul, now genuinely unspecified)'
    print(f'  [{k:>5}] old={old_s:<14} new={new_s:<14}{mark}')

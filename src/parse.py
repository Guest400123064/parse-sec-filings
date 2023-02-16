#!/usr/bin/env python3
# %%
import os
import glob

import json

import tqdm
import logging

import re
import bs4
import trafilatura


logging.basicConfig(filename='log/extract-item1a-full.log', 
                    filemode='w',
                    level=logging.WARNING)


def extract_item1a(f: str) -> str:

    # Find the html section (throw away metadata)
    pattern_html = re.compile(r'<html.*?>(.*?)</html>', flags=(re.DOTALL | re.IGNORECASE))
    with open(f, 'r', encoding='utf8') as fin:
        matched_html = pattern_html.search(fin.read())

        # Use bs4 to parse (fix) html
        if matched_html is None:
            logging.error(f'Corrupted format: Empty content between <html>(.*)</html>, < {f} >')
            raise Exception('Corrupted format: Empty content between <html>(.*)</html>')
        soup = bs4.BeautifulSoup(matched_html.group(1), features='html5lib')
    
    # Find starting tag and ending tag for section Item 1A
    patterns_start = [
        r"^item 1a ?risk factors$",

        # NEM edge cases
        r"^item 1a ?risk factors \(.*?\)$"
    ]
    patterns_end = [
        r"^item 1b ?unresolved staff comments$", 
        r"^item 2 ?properties$",
        
        # NEM edge cases
        r"^item 2 ?properties \(.*?\)$",

        # LODE edge cases
        r"^item 2 description of properties$"
    ]

    patterns_start = re.compile(r'|'.join(patterns_start), flags=(re.IGNORECASE | re.DOTALL))
    patterns_end = re.compile(r'|'.join(patterns_end), flags=(re.IGNORECASE | re.DOTALL))

    index_start = index_end = -1
    tag_start = tag_end = None

    # Iterate through all fonts BACKWARD (to skip table of contents)
    for i, tag in enumerate((list(soup.find_all('font'))
                                + list(soup.find_all('b'))
                                + list(soup.find_all('p'))
                                + list(soup.find_all('div'))
                                + list(soup.find_all('table')))[::-1]):
        # Clean text
        s = re.sub(r'[.:,;]', '', ''.join(tag.strings))
        s = re.sub(r'\s+', ' ', s).strip()

        # Start tag
        if patterns_start.match(s) is not None:
            index_start = i
            tag_start = tag

        # End tag
        if patterns_end.match(s) is not None:
            index_end = i
            tag_end = tag
        
        if tag_start and tag_end:
            break

    if index_start <= index_end:
        logging.warning(f'Potential un-matched start-end pair: <{f}>')
    assert tag_start is not None
    assert tag_end is not None

    # Extract the Item 1A section html and extract texts
    item1a_pattern = f'{re.escape(str(tag_start))}(.*?){re.escape(str(tag_end))}'  # EXACT MATCHING, NO SPECIAL CHAR
    item1a_html = re.compile(item1a_pattern, flags=re.DOTALL).search(str(soup)).group(1)
    item1a_text = trafilatura.extract(item1a_html, favor_precision=True, include_tables=False)

    # Post processing
    #   - Remove page numbers
    #   - Merge new pseudo-paragraphs
    def _post_process(s: str) -> str:

        # Page numbers are of <\n[0-9]{1}{2}\n> pattern
        s = re.sub(r'\n[0-9]{1,2}\n', r' ', s)

        # Merge paragraphs with lower-case starting to
        #   the proceeding paragraph
        r = ['']
        for p in s.split('\n'):
            if ((p[0].isalpha() and p[0].islower()) 
                    or p[0].isnumeric()
                    or not p[0].isalnum()):  
                r[-1] = f'{r[-1]} {p}'.strip()
            else:
                r.append(p)
        return '\n'.join(r).strip()

    ret = _post_process(item1a_text)
    if len(ret) < 32:
        logging.warning(f'Section too short: <{f}> --> <{ret}>')
    return ret


# %%
if __name__ == '__main__':

    # Use annual 10-K forms after 2005 since Item 1A is not 
    #   required before then.
    fs = sorted([f for f in glob.glob(r'data/filings/*/10-K/*/__RAW__.htm') 
                    if re.search(r'/20(0[6-9]|[1-2]\d)\d+/', f)])
    
    # Extract file date (yyyy-mm) and company names (symbols) from paths
    ts = [f'{s.group(1)}-{s.group(2)}' for s in [re.search(r'/(20\d\d)(\d\d)\d+/', f) for f in fs]]
    cs = [re.findall(r'filings/(.*?)/10-K', f)[0] for f in fs]

    # Try extract Item 1A
    status = []
    item1a = []

    print('[ info ] :: start extraction...')
    for f in tqdm.tqdm(fs):
        try:
            item1a.append(extract_item1a(f))
            status.append(0)
        except:
            logging.error(f'Unknown error: < {f} >')
            item1a.append(f'<FAILED>{f}</FAILED>')
            status.append(1)
    print('[ info ] :: DONE!')

    # Convert to DataFrame and write to CSV
    outputs = []
    for c, t, i, s in zip(cs, ts, item1a, status):
        outputs.append({'symbol': c, 
                        'filing_time': t, 
                        'item1a': i, 
                        'status': s})
    
    print('[ info ] :: dump to json file... ')
    with open('data/extracts/item1a-full.json', 'w') as fout:
        json.dump(outputs, fout, indent=True)
    print('[ info ] :: DONE!')
    print('[ info ] :: please find logging from <extract-item1a-full.log> for failed cases')

"""
Download SEC FOIA Investment Adviser data and extract only the columns
needed by the Streamlit app. Saves a slim CSV (~3-5MB) to data/sec_advisers.csv.

Designed to run in GitHub Actions (where SEC doesn't block requests).
"""

import requests
import zipfile
import io
import sys
from datetime import date
from pathlib import Path

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

SEC_BASE_URL = 'https://www.sec.gov/files/investment/data/information-about-registered-investment-advisers-exempt-reporting-advisers/'

# Only the columns the app actually uses (13 out of 448)
KEEP_COLUMNS = [
    'Primary Business Name',
    'Organization CRD#',
    'SEC Status Effective Date',
    'Latest ADV Filing Date',
    'SEC Current Status',
    'Main Office City',
    'Main Office State',
    'Main Office Telephone Number',
    'Website Address',
    'Legal Name',
    '5A',       # Employees
    '5C(1)',    # Clients
    '5F(2)(c)', # AUM
]

OUTPUT_PATH = Path(__file__).resolve().parent.parent / 'data' / 'sec_advisers.csv'


def build_candidate_urls():
    today = date.today()
    candidates = []
    for months_back in range(0, 4):
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        for day in [1, 2]:
            d = date(year, month, day)
            stamp = d.strftime('%m%d%y')
            candidates.append((f'{SEC_BASE_URL}ia{stamp}.zip', d.strftime('%Y-%m-%d')))
    return candidates


def main():
    import pandas as pd

    candidates = build_candidate_urls()

    print(f'Trying {len(candidates)} candidate URLs...')
    resp = None
    used_label = None

    for url, label in candidates:
        print(f'  Trying {label}... ', end='')
        try:
            r = requests.get(url, headers=HEADERS, timeout=300, allow_redirects=True)
            if r.status_code == 200:
                print(f'OK ({len(r.content) / 1024 / 1024:.1f} MB)')
                resp = r
                used_label = label
                break
            else:
                print(f'HTTP {r.status_code}')
        except requests.RequestException as e:
            print(f'Error: {e}')

    if resp is None:
        print('ERROR: All candidate URLs failed.')
        sys.exit(1)

    # Extract CSV from ZIP
    print('Extracting CSV from ZIP...')
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, encoding='latin-1', low_memory=False)

    print(f'Full dataset: {len(df)} rows, {len(df.columns)} columns')

    # Keep only needed columns
    available = [c for c in KEEP_COLUMNS if c in df.columns]
    slim = df[available].copy()

    # Save slim CSV
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    slim.to_csv(OUTPUT_PATH, index=False)
    size_mb = OUTPUT_PATH.stat().st_size / 1024 / 1024
    print(f'Saved {len(slim)} rows, {len(available)} columns to {OUTPUT_PATH} ({size_mb:.1f} MB)')
    print(f'Data snapshot: {used_label}')


if __name__ == '__main__':
    main()

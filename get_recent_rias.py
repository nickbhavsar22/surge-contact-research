import requests
import zipfile
import io
import logging
import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

SEC_BASE_URL = 'https://www.sec.gov/files/investment/data/information-about-registered-investment-advisers-exempt-reporting-advisers/'

# Pre-downloaded CSV (committed by GitHub Actions nightly)
_PRELOADED_CSV = Path(__file__).resolve().parent / 'data' / 'sec_advisers.csv'


def _build_candidate_urls():
    """Build candidate SEC FOIA ZIP URLs for recent months."""
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


def _download_sec_zip(candidates, headers, timeout=300, log=None):
    """Try each candidate URL with a full download. Return (response, url, label) or None."""
    for url, label in candidates:
        try:
            if log:
                log(f'Trying SEC database: {label}...')
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp, url, label
        except requests.RequestException:
            continue
    return None


def _load_predownloaded_csv(log=None):
    """Load pre-downloaded SEC data CSV (committed by GitHub Actions).

    Returns DataFrame or None if the file doesn't exist.
    """
    if not _PRELOADED_CSV.exists():
        return None
    if log:
        log('Loading pre-downloaded SEC database...')
    df = pd.read_csv(_PRELOADED_CSV, low_memory=False)
    if log:
        log(f'Loaded {len(df):,} records from pre-downloaded data')
    return df


def get_recent_rias(days_back=30, start_date=None, end_date=None, export_csv=False, progress_callback=None):
    """
    Fetch recently registered Investment Advisers from SEC FOIA database.

    Args:
        days_back: Number of days to look back (used when start_date/end_date not provided)
        start_date: Filter start date (datetime.date). Overrides days_back.
        end_date: Filter end date (datetime.date). Overrides days_back.
        export_csv: If True, exports results to CSV file
        progress_callback: Optional callable(message) for UI status updates

    Returns:
        dict with keys:
            'df': DataFrame with filtered RIA registrations
            'total_records': int, total records in SEC database
            'snapshot_date': str, date of the SEC data snapshot
            'zip_url': str, the SEC ZIP URL used
            'error': str or None
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)

    # Try pre-downloaded CSV first (committed by GitHub Actions nightly).
    # Falls back to live SEC download if the file doesn't exist (e.g. local dev).
    zip_url = None
    df = _load_predownloaded_csv(log=log)

    if df is None:
        # Fallback: download directly from SEC (works locally, blocked on some cloud hosts)
        log('No pre-downloaded data found. Downloading from SEC...')
        candidates = _build_candidate_urls()
        download = _download_sec_zip(candidates, HEADERS, timeout=300, log=log)

        if download is None:
            error_msg = 'Could not load SEC data. Pre-downloaded file missing and live download failed.'
            log(f'ERROR: {error_msg}')
            return {'df': pd.DataFrame(), 'total_records': 0, 'snapshot_date': None, 'zip_url': None, 'error': error_msg}

        resp, zip_url, url_label = download
        log(f'Downloaded SEC database: {url_label}')

        log('Extracting and parsing data...')
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, encoding='latin-1', low_memory=False)

    # Convert dates
    df['Status_Date'] = pd.to_datetime(df['SEC Status Effective Date'], errors='coerce')
    df['Filing_Date'] = pd.to_datetime(df['Latest ADV Filing Date'], errors='coerce')

    data_date = df['Status_Date'].max()
    total_records = len(df)

    log(f'Database contains {total_records:,} registered investment advisers')
    log(f'Data snapshot date: {data_date.strftime("%Y-%m-%d")}')

    # Determine date range for filtering
    if start_date and end_date:
        filter_start = pd.Timestamp(start_date)
        filter_end = pd.Timestamp(end_date)
        log(f'Filtering: {start_date} to {end_date}')
    else:
        filter_end = data_date
        filter_start = data_date - timedelta(days=days_back)
        log(f'Filtering: last {days_back} days (since {filter_start.strftime("%Y-%m-%d")})')

    # Filter for registrations in the date range
    new_rias = df[(df['Status_Date'] >= filter_start) & (df['Status_Date'] <= filter_end)].copy()
    new_rias = new_rias.sort_values('Status_Date', ascending=False)

    log(f'Found {len(new_rias)} new RIA registrations')

    if new_rias.empty:
        return {
            'df': pd.DataFrame(),
            'total_records': total_records,
            'snapshot_date': data_date.strftime('%Y-%m-%d'),
            'zip_url': zip_url,
            'error': None
        }

    # Select key columns for output
    output_cols = {
        'Primary Business Name': 'Company',
        'Organization CRD#': 'CRD',
        'Status_Date': 'Registered',
        'SEC Current Status': 'Status',
        'Main Office City': 'City',
        'Main Office State': 'State',
        'Main Office Telephone Number': 'Phone',
        'Website Address': 'Website',
        'Legal Name': 'Legal_Name',
        '2A(1)': 'SEC_Registered',
        '2A(2)': 'ERA',
        '5A': 'Employees',
        '5C(1)': 'Clients',
        '5F(2)(a)': 'AUM_Discretionary',
        '5F(2)(b)': 'AUM_NonDiscretionary',
        '5F(2)(c)': 'AUM',
    }

    available_cols = [col for col in output_cols.keys() if col in new_rias.columns]
    result = new_rias[available_cols].copy()
    result.columns = [output_cols[col] for col in available_cols]

    # Clean AUM columns: strip whitespace, convert to numeric
    for aum_col in ['AUM', 'AUM_Discretionary', 'AUM_NonDiscretionary']:
        if aum_col in result.columns:
            result[aum_col] = (
                result[aum_col]
                .astype(str)
                .str.strip()
                .str.replace(',', '', regex=False)
                .str.replace('.00', '', regex=False)
            )
            result[aum_col] = pd.to_numeric(result[aum_col], errors='coerce').fillna(0).astype(int)

    # Add AUM bracket column for threshold intelligence
    if 'AUM' in result.columns:
        result['AUM_Bracket'] = pd.cut(
            result['AUM'],
            bins=[-1, 0, 100_000_000, 110_000_000, 150_000_000, float('inf')],
            labels=['No AUM', '< $100M', '$100M–$110M', '$110M–$150M', '$150M+'],
        )

    # Derive registration type with sub-threshold flag
    if 'SEC_Registered' in result.columns and 'ERA' in result.columns:
        def _reg_type(r):
            if str(r.get('SEC_Registered', '')).strip().upper() == 'Y':
                aum = r.get('AUM', 0) if pd.notna(r.get('AUM', 0)) else 0
                return 'SEC (Sub-threshold)' if aum < 100_000_000 else 'SEC-Registered'
            if str(r.get('ERA', '')).strip().upper() == 'Y':
                return 'ERA'
            return 'State-Registered'
        result['Registration_Type'] = result.apply(_reg_type, axis=1)

    # Clean Employees and Clients columns
    for col in ['Employees', 'Clients']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0).astype(int)

    # Export to CSV if requested
    if export_csv:
        filename = f'new_rias_{datetime.now().strftime("%Y%m%d")}.csv'
        result.to_csv(filename, index=False)
        log(f'Exported to: {filename}')

    return {
        'df': result,
        'total_records': total_records,
        'snapshot_date': data_date.strftime('%Y-%m-%d'),
        'zip_url': zip_url,
        'error': None
    }


def get_era_pipeline(aum_min=50_000_000, aum_max=150_000_000, progress_callback=None):
    """
    Fetch Exempt Reporting Advisers approaching the SEC registration threshold.

    ERAs with AUM between aum_min and aum_max are potential SEC registrants.
    The $110M threshold triggers mandatory SEC registration.

    Returns:
        dict with keys:
            'df': DataFrame with ERA pipeline prospects
            'total_eras': int, total ERAs in SEC database
            'total_records': int, total records in SEC database
            'snapshot_date': str, date of the SEC data snapshot
            'error': str or None
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)

    # Load data (same source as get_recent_rias)
    zip_url = None
    df = _load_predownloaded_csv(log=log)

    if df is None:
        log('No pre-downloaded data found. Downloading from SEC...')
        candidates = _build_candidate_urls()
        download = _download_sec_zip(candidates, HEADERS, timeout=300, log=log)

        if download is None:
            error_msg = 'Could not load SEC data.'
            log(f'ERROR: {error_msg}')
            return {'df': pd.DataFrame(), 'total_eras': 0, 'total_records': 0,
                    'snapshot_date': None, 'error': error_msg}

        resp, zip_url, url_label = download
        log(f'Downloaded SEC database: {url_label}')

        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, encoding='latin-1', low_memory=False)

    df['Status_Date'] = pd.to_datetime(df['SEC Status Effective Date'], errors='coerce')
    data_date = df['Status_Date'].max()
    total_records = len(df)

    # Filter for ERAs: 2A(2) = Y
    if '2A(2)' not in df.columns:
        return {'df': pd.DataFrame(), 'total_eras': 0, 'total_records': total_records,
                'snapshot_date': data_date.strftime('%Y-%m-%d'),
                'error': 'ERA column (2A(2)) not found in data. Re-run data pipeline.'}

    eras = df[df['2A(2)'] == 'Y'].copy()
    total_eras = len(eras)
    log(f'Found {total_eras} Exempt Reporting Advisers in database')

    # Map columns
    output_cols = {
        'Primary Business Name': 'Company',
        'Organization CRD#': 'CRD',
        'Status_Date': 'Registered',
        'SEC Current Status': 'Status',
        'Main Office City': 'City',
        'Main Office State': 'State',
        'Main Office Telephone Number': 'Phone',
        'Website Address': 'Website',
        'Legal Name': 'Legal_Name',
        '2A(1)': 'SEC_Registered',
        '2A(2)': 'ERA',
        '5A': 'Employees',
        '5C(1)': 'Clients',
        '5F(2)(a)': 'AUM_Discretionary',
        '5F(2)(b)': 'AUM_NonDiscretionary',
        '5F(2)(c)': 'AUM',
    }

    available_cols = [col for col in output_cols.keys() if col in eras.columns]
    result = eras[available_cols].copy()
    result.columns = [output_cols[col] for col in available_cols]

    # Clean AUM columns
    for aum_col in ['AUM', 'AUM_Discretionary', 'AUM_NonDiscretionary']:
        if aum_col in result.columns:
            result[aum_col] = (
                result[aum_col]
                .astype(str)
                .str.strip()
                .str.replace(',', '', regex=False)
                .str.replace('.00', '', regex=False)
            )
            result[aum_col] = pd.to_numeric(result[aum_col], errors='coerce').fillna(0).astype(int)

    # Add AUM bracket
    if 'AUM' in result.columns:
        result['AUM_Bracket'] = pd.cut(
            result['AUM'],
            bins=[-1, 0, 100_000_000, 110_000_000, 150_000_000, float('inf')],
            labels=['No AUM', '< $100M', '$100M–$110M', '$110M–$150M', '$150M+'],
        )

    # Derive registration type with sub-threshold flag
    if 'SEC_Registered' in result.columns and 'ERA' in result.columns:
        def _reg_type_era(r):
            if str(r.get('SEC_Registered', '')).strip().upper() == 'Y':
                aum = r.get('AUM', 0) if pd.notna(r.get('AUM', 0)) else 0
                return 'SEC (Sub-threshold)' if aum < 100_000_000 else 'SEC-Registered'
            if str(r.get('ERA', '')).strip().upper() == 'Y':
                return 'ERA'
            return 'State-Registered'
        result['Registration_Type'] = result.apply(_reg_type_era, axis=1)

    # Clean Employees and Clients columns
    for col in ['Employees', 'Clients']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0).astype(int)

    # Filter to AUM range
    pipeline = result[(result['AUM'] >= aum_min) & (result['AUM'] <= aum_max)].copy()
    pipeline = pipeline.sort_values('AUM', ascending=False)

    log(f'Pipeline: {len(pipeline)} ERAs with AUM ${aum_min/1e6:.0f}M–${aum_max/1e6:.0f}M')

    return {
        'df': pipeline,
        'total_eras': total_eras,
        'total_records': total_records,
        'snapshot_date': data_date.strftime('%Y-%m-%d'),
        'error': None,
    }


if __name__ == '__main__':
    print('=' * 90)
    print('SEC REGISTERED INVESTMENT ADVISERS - NEW REGISTRATIONS')
    print('Data Source: SEC FOIA Investment Adviser Database')
    print('=' * 90)
    print()

    result = get_recent_rias(days_back=30, export_csv=True)

    if result['error']:
        print(f"\nError: {result['error']}")
    elif not result['df'].empty:
        df = result['df']
        print()
        for _, row in df.iterrows():
            print(f"Company:    {row['Company']}")
            print(f"CRD#:       {row['CRD']}")
            print(f"Registered: {row['Registered'].strftime('%Y-%m-%d') if pd.notna(row['Registered']) else 'N/A'}")
            print(f"Status:     {row['Status']}")
            print(f"Location:   {row['City']}, {row['State']}")
            print(f"Phone:      {row['Phone']}")
            print(f"Website:    {row['Website'] if pd.notna(row['Website']) else 'N/A'}")
            print('-' * 90)
        print(f'\nTotal new RIAs: {len(df)}')
    else:
        print('No new registrations found in the specified timeframe.')

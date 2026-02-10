import requests
import zipfile
import io
import logging
import pandas as pd
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

SEC_BASE_URL = 'https://www.sec.gov/files/investment/data/information-about-registered-investment-advisers-exempt-reporting-advisers/'


def _build_candidate_urls():
    """Build candidate SEC FOIA ZIP URLs for recent months.

    The SEC publishes monthly snapshots with filenames like ia010226.zip (MMDDYY).
    Returns candidate (url, date_label) tuples for the 1st and 2nd of each month
    going back 4 months.
    """
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
    """Try each candidate URL with a full download. Return (response, url, label) or None.

    Skips the separate URL discovery step because SEC blocks lightweight
    probe requests (HEAD, Range, even streaming GET) from cloud server IPs.
    Wrong URLs return 404 quickly; the correct one downloads the full ZIP.
    """
    for url, label in candidates:
        try:
            if log:
                log(f'Trying SEC database: {label}...')
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp, url, label
            # 404 = wrong date, try next; 403 = blocked, try next
        except requests.RequestException:
            continue
    return None


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

    # Download SEC database — tries candidate URLs directly (no separate discovery
    # step, because SEC blocks probe requests from cloud IPs).
    log('Downloading SEC Investment Adviser database...')
    candidates = _build_candidate_urls()
    download = _download_sec_zip(candidates, HEADERS, timeout=300, log=log)

    if download is None:
        error_msg = 'Could not download SEC database. All candidate URLs failed (SEC may be blocking this server).'
        log(f'ERROR: {error_msg}')
        return {'df': pd.DataFrame(), 'total_records': 0, 'snapshot_date': None, 'zip_url': None, 'error': error_msg}

    resp, zip_url, url_label = download
    log(f'Downloaded SEC database: {url_label}')

    # Extract CSV from ZIP
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
        '5A': 'Employees',
        '5C(1)': 'Clients',
        '5F(2)(c)': 'AUM',
    }

    available_cols = [col for col in output_cols.keys() if col in new_rias.columns]
    result = new_rias[available_cols].copy()
    result.columns = [output_cols[col] for col in available_cols]

    # Clean AUM column: strip whitespace, convert to numeric
    if 'AUM' in result.columns:
        result['AUM'] = (
            result['AUM']
            .astype(str)
            .str.strip()
            .str.replace(',', '', regex=False)
            .str.replace('.00', '', regex=False)
        )
        result['AUM'] = pd.to_numeric(result['AUM'], errors='coerce').fillna(0).astype(int)

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

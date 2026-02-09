"""
SurgeONE.ai Fit Scoring Engine
Scores newly registered RIAs on how well they match SurgeONE's ICP.

SurgeONE.ai is an AI-powered compliance, cybersecurity, and data platform
for SEC/FINRA/state-regulated financial firms. Their ICP:
  - SEC-registered investment advisers and broker-dealers
  - Firms needing compliance infrastructure (newly registered = high urgency)
  - Firms with teams (not just solo practitioners)
  - Tech-forward firms managing client assets
  - Firms actively concerned with compliance, cybersecurity, data management
"""

import requests
import re
import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# ---------------------------------------------------------------------------
# ICP keyword sets (lowercase for matching)
# ---------------------------------------------------------------------------

# Company name keywords suggesting wealth/advisory focus
NAME_ADVISORY_KEYWORDS = [
    'wealth', 'advisory', 'advisors', 'financial planning', 'capital',
    'investment', 'asset management', 'portfolio', 'retirement',
    'fiduciary', 'private client', 'family office',
]

# Company name keywords suggesting a team (not solo)
NAME_TEAM_KEYWORDS = [
    'group', 'partners', 'associates', '& co', 'team', 'consulting',
    'services', 'solutions', 'global', 'strategic',
]

# Top financial market states (higher density of RIAs, larger firms)
TOP_FINANCIAL_STATES = {
    'NY', 'CA', 'TX', 'FL', 'CT', 'MA', 'IL', 'NJ', 'PA', 'CO',
}

# Website content keywords and their point values
WEBSITE_SIGNALS = {
    # Compliance/regulatory awareness (strong SurgeONE fit)
    'compliance': [
        'compliance', 'regulatory', 'fiduciary', 'sec registered',
        'form adv', 'disclosure', 'audit', 'examination',
    ],
    # Core advisory services
    'advisory_services': [
        'wealth management', 'financial planning', 'investment advisory',
        'portfolio management', 'asset management', 'retirement planning',
        'estate planning', 'tax planning', 'financial advisor',
    ],
    # Team indicators (implies scale)
    'team': [
        'our team', 'meet the team', 'our advisors', 'our professionals',
        'leadership', 'managing director', 'vice president', 'partner',
        'staff', 'employees',
    ],
    # Client/AUM indicators (implies established business)
    'clients': [
        'assets under management', 'aum', 'clients', 'high net worth',
        'institutional', 'individuals', 'families', 'client service',
    ],
    # Technology/digital indicators (implies tech-forward)
    'technology': [
        'technology', 'digital', 'platform', 'portal', 'fintech',
        'innovation', 'automated', 'online', 'app',
    ],
    # Cybersecurity/data awareness (direct SurgeONE alignment)
    'cybersecurity': [
        'cybersecurity', 'data protection', 'privacy', 'information security',
        'data management', 'secure', 'encryption',
    ],
}

# Points per signal category
WEBSITE_SIGNAL_POINTS = {
    'compliance': 14,
    'advisory_services': 12,
    'team': 10,
    'clients': 10,
    'technology': 8,
    'cybersecurity': 11,
}


def _safe_int(val, default=0):
    """Safely convert a value to int."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _score_from_data(row):
    """Score a prospect using SEC data fields. Returns (score, max_possible, reasons)."""
    score = 0
    reasons = []

    company = str(row.get('Company', '')).lower()
    state = str(row.get('State', '')).strip().upper()
    website = str(row.get('Website', ''))
    phone = str(row.get('Phone', ''))
    employees = _safe_int(row.get('Employees', 0))
    clients = _safe_int(row.get('Clients', 0))
    aum = _safe_int(row.get('AUM', 0))

    # Has website (8 pts)
    if website and website.lower() not in ('nan', '', 'none'):
        score += 8
        reasons.append('has_website')

    # Has phone (3 pts)
    if phone and phone.lower() not in ('nan', '', 'none'):
        score += 3
        reasons.append('has_phone')

    # Company name suggests advisory/wealth focus (6 pts)
    if any(kw in company for kw in NAME_ADVISORY_KEYWORDS):
        score += 6
        reasons.append('name_advisory')

    # Company name suggests team/scale (4 pts)
    if any(kw in company for kw in NAME_TEAM_KEYWORDS):
        score += 4
        reasons.append('name_team')

    # Top financial state (4 pts)
    if state in TOP_FINANCIAL_STATES:
        score += 4
        reasons.append('top_state')

    # Employee count signals (10 pts max)
    if employees >= 10:
        score += 10
        reasons.append('team_10+')
    elif employees >= 3:
        score += 6
        reasons.append('team_3+')
    elif employees >= 1:
        score += 2
        reasons.append('has_employees')

    # AUM signals (10 pts max) — higher AUM = more infrastructure needs
    if aum >= 1_000_000_000:  # $1B+
        score += 10
        reasons.append('aum_1B+')
    elif aum >= 100_000_000:  # $100M+
        score += 8
        reasons.append('aum_100M+')
    elif aum >= 10_000_000:  # $10M+
        score += 5
        reasons.append('aum_10M+')
    elif aum > 0:
        score += 2
        reasons.append('has_aum')

    # Client count signals (5 pts max) — more clients = more compliance needs
    if clients >= 100:
        score += 5
        reasons.append('clients_100+')
    elif clients >= 10:
        score += 3
        reasons.append('clients_10+')
    elif clients > 0:
        score += 1
        reasons.append('has_clients')

    return score, 50, reasons


def _fetch_website_text(url, timeout=15):
    """Fetch homepage text content. Returns lowercase text or None."""
    if not url or url.lower() in ('nan', '', 'none'):
        return None

    # Normalize URL
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code != 200:
            return None

        # Strip HTML tags, keep text
        text = re.sub(r'<script[^>]*>[\s\S]*?</script>', ' ', resp.text, flags=re.IGNORECASE)
        text = re.sub(r'<style[^>]*>[\s\S]*?</style>', ' ', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).lower()
        return text

    except Exception as e:
        logger.debug('Could not fetch %s: %s', url, e)
        return None


def _score_from_website(website_text):
    """Score a prospect based on website content. Returns (score, max_possible, reasons)."""
    if not website_text:
        return 0, 0, []

    score = 0
    max_possible = 5  # base points for reachable site
    score += 5  # site is reachable
    reasons = ['site_reachable']

    for category, keywords in WEBSITE_SIGNALS.items():
        max_possible += WEBSITE_SIGNAL_POINTS[category]
        if any(kw in website_text for kw in keywords):
            score += WEBSITE_SIGNAL_POINTS[category]
            reasons.append(category)

    return score, max_possible, reasons


def calculate_fit_score(row):
    """Calculate fit score for a single RIA prospect.

    Returns:
        dict with 'Fit_Score' (int 0-100 or 'N/A') and 'Fit_Reasons' (str)
    """
    data_score, data_max, data_reasons = _score_from_data(row)

    website = str(row.get('Website', ''))
    has_website = website and website.lower() not in ('nan', '', 'none')

    if not has_website and data_score <= 3:
        # No website and barely any data — can't assess
        return {'Fit_Score': 'N/A', 'Fit_Reasons': 'Insufficient data'}

    if has_website:
        website_text = _fetch_website_text(website)
        web_score, web_max, web_reasons = _score_from_website(website_text)
    else:
        web_score, web_max, web_reasons = 0, 0, []

    total_score = data_score + web_score
    total_max = data_max + web_max

    if total_max == 0:
        return {'Fit_Score': 'N/A', 'Fit_Reasons': 'Insufficient data'}

    # Normalize to 0-100
    normalized = round((total_score / total_max) * 100)
    normalized = min(normalized, 100)

    all_reasons = data_reasons + web_reasons
    return {'Fit_Score': normalized, 'Fit_Reasons': ', '.join(all_reasons)}


def score_dataframe(df, progress_callback=None):
    """Score all RIAs in a DataFrame for SurgeONE.ai fit.

    Args:
        df: DataFrame with RIA data (Company, Website, State, etc.)
        progress_callback: Optional callable(current, total, message)

    Returns:
        dict with:
            'df': DataFrame with Fit_Score and Fit_Reasons columns, sorted by score desc
            'scored': int count of scored prospects
            'na_count': int count of N/A prospects
    """
    df = df.copy()
    total = len(df)
    scored_count = 0
    na_count = 0

    fit_scores = []
    fit_reasons = []

    for i, (idx, row) in enumerate(df.iterrows()):
        position = i + 1
        company = str(row.get('Company', 'Unknown'))[:40]

        if progress_callback:
            progress_callback(position, total, f'[{position}/{total}] Scoring: {company}')

        result = calculate_fit_score(row)
        fit_scores.append(result['Fit_Score'])
        fit_reasons.append(result['Fit_Reasons'])

        if result['Fit_Score'] == 'N/A':
            na_count += 1
        else:
            scored_count += 1

        # Rate limit website fetches (only if prospect has a website)
        website = str(row.get('Website', ''))
        if website and website.lower() not in ('nan', '', 'none'):
            time.sleep(0.5)

    df['Fit_Score'] = fit_scores
    df['Fit_Reasons'] = fit_reasons

    # Sort: numeric scores descending first, then N/A at the bottom
    numeric_mask = df['Fit_Score'] != 'N/A'
    df_scored = df[numeric_mask].copy()
    df_na = df[~numeric_mask].copy()

    df_scored['_sort_score'] = pd.to_numeric(df_scored['Fit_Score'])
    df_scored = df_scored.sort_values('_sort_score', ascending=False).drop(columns=['_sort_score'])

    df_sorted = pd.concat([df_scored, df_na], ignore_index=True)

    return {
        'df': df_sorted,
        'scored': scored_count,
        'na_count': na_count,
    }

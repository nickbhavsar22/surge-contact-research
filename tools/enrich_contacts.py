"""
Contact enrichment tool for RIA firms.

Given a website URL, discovers contact name, email, and title using:
1. Hunter.io Domain Search API (if API key configured)
2. Website scraping (/contact, /about, /team pages)
"""

import requests
import re
import logging
import time
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

REQUEST_TIMEOUT = 15

# Title priority for selecting the best contact (lower index = higher priority)
TITLE_PRIORITY = [
    'chief compliance officer', 'cco',
    'principal', 'managing member', 'managing partner',
    'founder', 'co-founder',
    'chief executive officer', 'ceo',
    'president',
    'owner',
    'chief operating officer', 'coo',
    'chief financial officer', 'cfo',
    'director', 'managing director',
    'partner',
    'advisor', 'adviser',
]

# Subpages to crawl for contact info
CONTACT_SUBPAGES = [
    '/contact', '/contact-us', '/about', '/about-us',
    '/team', '/our-team', '/people', '/leadership',
    '/staff', '/our-firm', '/bio', '/advisors',
]

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)

PHONE_PATTERN = re.compile(
    r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
)

EXCLUDED_EMAIL_DOMAINS = {
    'sec.gov', 'finra.org', 'example.com', 'sampleemail.com',
    'email.com', 'domain.com', 'yourcompany.com', 'company.com',
}

EXCLUDED_EMAIL_PREFIXES = {
    'info@', 'support@', 'admin@', 'webmaster@', 'noreply@',
    'no-reply@', 'sales@', 'marketing@', 'help@', 'contact@',
    'hello@', 'office@', 'mail@', 'general@',
}

TITLE_SEARCH_PATTERN = re.compile(
    r'(?:chief compliance officer|cco|principal|managing member|'
    r'managing partner|founder|co-founder|ceo|president|owner|'
    r'chief executive|chief operating|partner|director|advisor|adviser)',
    re.IGNORECASE
)

NAME_PATTERN = re.compile(
    r'\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b'
)

CORP_WORDS = {'LLC', 'INC', 'LTD', 'CORP', 'LP', 'LLP', 'THE', 'AND', 'GROUP'}

# Words that look like names (capitalized) but are financial/product terms.
# If ANY word in a candidate name matches, it is rejected.
FALSE_NAME_WORDS = {
    'CASH', 'ACCOUNT', 'ACCOUNTS', 'RESERVE', 'FUND', 'FUNDS', 'TRUST',
    'CAPITAL', 'INVESTMENT', 'INVESTMENTS', 'ADVISORY', 'ADVISORS',
    'ADVISERS', 'WEALTH', 'MANAGEMENT', 'FINANCIAL', 'SECURITIES',
    'SERVICES', 'PORTFOLIO', 'ASSET', 'ASSETS', 'EQUITY', 'BOND', 'BONDS',
    'MARKET', 'MARKETS', 'TRADING', 'RETIREMENT', 'PLANNING', 'BROKERAGE',
    'BANKING', 'INSURANCE', 'COMPLIANCE', 'REGISTERED', 'PROGRAM', 'BANKS',
    'BEST', 'HIGH', 'LOW', 'NET', 'WORTH', 'RATE', 'RATES', 'YIELD',
    'PERFORMANCE', 'REPORT', 'RETURNS', 'INCOME', 'INTEREST', 'DEPOSIT',
    'SAVINGS', 'CHECKING', 'CREDIT', 'DEBIT', 'LOAN', 'MORTGAGE',
    'PREMIER', 'PREMIUM', 'BASIC', 'STANDARD', 'ADVANCED', 'SELECT',
    'ABOUT', 'MORE', 'LEARN', 'VIEW', 'DETAILS', 'TERMS', 'PRIVACY',
    'POLICY', 'CONTACT', 'HELP', 'SUPPORT', 'HOME', 'BACK', 'NEXT',
}


def extract_domain(url):
    """Extract bare domain from a website URL.

    Returns domain string (e.g., 'acmwealth.com') or None if invalid.
    """
    if not url or str(url).lower().strip() in ('nan', '', 'none'):
        return None
    url = str(url).strip()
    if not url.startswith('http'):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        domain = domain.lower().removeprefix('www.')
        return domain if '.' in domain else None
    except Exception:
        return None


def _hunter_domain_search(domain, api_key):
    """Query Hunter.io Domain Search API for emails at a domain.

    Free tier: 50 searches/month, up to 10 results per call.
    Returns list of dicts with keys: name, email, title, source.
    """
    if not domain or not api_key:
        return []

    url = 'https://api.hunter.io/v2/domain-search'
    params = {
        'domain': domain,
        'api_key': api_key,
        'limit': 10,
    }

    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 429:
            logger.warning('Hunter.io rate limit reached')
            return []
        if resp.status_code == 401:
            logger.warning('Hunter.io API key invalid')
            return []
        if resp.status_code != 200:
            logger.debug('Hunter.io returned HTTP %d for %s', resp.status_code, domain)
            return []

        data = resp.json().get('data', {})
        emails = data.get('emails', [])

        results = []
        for entry in emails:
            first = (entry.get('first_name') or '').strip()
            last = (entry.get('last_name') or '').strip()
            name = f'{first} {last}'.strip() if first or last else ''
            results.append({
                'name': name,
                'email': entry.get('value', ''),
                'title': entry.get('position') or '',
                'source': 'hunter.io',
            })
        return results

    except requests.RequestException as e:
        logger.debug('Hunter.io request failed for %s: %s', domain, e)
        return []


def _fetch_page_soup(url):
    """Fetch a URL and return a BeautifulSoup object, or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True)
        if resp.status_code != 200:
            return None
        return BeautifulSoup(resp.text, 'html.parser')
    except requests.RequestException:
        return None


def _extract_emails_from_soup(soup, domain):
    """Extract email addresses from parsed HTML.

    Checks mailto links and regex over visible text.
    Filters generic/government emails. Sorts domain-matching emails first.
    """
    found = set()

    # mailto links
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.lower().startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if EMAIL_PATTERN.fullmatch(email):
                found.add(email)

    # Regex over visible text
    text = soup.get_text(separator=' ')
    for match in EMAIL_PATTERN.finditer(text):
        found.add(match.group().lower())

    # Filter exclusions
    filtered = []
    for email in found:
        email_domain = email.split('@')[1] if '@' in email else ''
        if email_domain in EXCLUDED_EMAIL_DOMAINS:
            continue
        if any(email.startswith(prefix) for prefix in EXCLUDED_EMAIL_PREFIXES):
            continue
        if email_domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
            continue
        filtered.append(email)

    if domain:
        filtered.sort(key=lambda e: (0 if domain in e else 1, e))

    return filtered


def _extract_contacts_from_soup(soup, domain):
    """Extract structured contacts (name + title + email) from a page.

    Uses two strategies:
    A) Find title keywords in text, look for person names on adjacent lines
    B) Find HTML elements with team/staff/bio CSS classes
    Then assigns emails to named contacts by matching name parts.
    """
    contacts = []
    emails = _extract_emails_from_soup(soup, domain)
    text_content = soup.get_text(separator='\n')

    # Strategy A: Look for "Name, Title" or "Name - Title" patterns on same line
    # This is conservative to avoid false positives from marketing text
    name_title_patterns = [
        # "John Smith, Chief Compliance Officer" or "John Smith | CEO"
        re.compile(
            r'\b([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)\s*[,|–—\-]\s*'
            r'((?:Chief\s+)?(?:Compliance\s+Officer|CCO|Principal|Managing\s+(?:Member|Partner|Director)'
            r'|Founder|Co-Founder|CEO|President|Owner|COO|CFO|Partner|Director'
            r'|Chief\s+Executive|Chief\s+Operating|Chief\s+Financial|Advisor|Adviser))',
            re.IGNORECASE
        ),
        # "Chief Compliance Officer: John Smith" or "CCO - John Smith"
        re.compile(
            r'(?:(?:Chief\s+)?(?:Compliance\s+Officer|CCO|Principal|Managing\s+(?:Member|Partner|Director)'
            r'|Founder|Co-Founder|CEO|President|Owner|COO|CFO|Partner|Director'
            r'|Chief\s+Executive|Chief\s+Operating|Chief\s+Financial|Advisor|Adviser))'
            r'\s*[:\-–—|]\s*([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)',
            re.IGNORECASE
        ),
    ]

    lines = text_content.split('\n')
    for line in lines:
        for pat_idx, pattern in enumerate(name_title_patterns):
            match = pattern.search(line)
            if not match:
                continue
            if pat_idx == 0:
                name, title_found = match.group(1).strip(), match.group(2).strip()
            else:
                name, title_found = match.group(1).strip(), ''
                # Extract title from the match for pattern 2
                title_m = TITLE_SEARCH_PATTERN.search(line)
                title_found = title_m.group().strip() if title_m else ''

            name_words = name.upper().split()
            if any(w in name_words for w in CORP_WORDS):
                continue
            if any(w in FALSE_NAME_WORDS for w in name_words):
                continue
            if len(name) < 4 or len(name) > 40:
                continue

            contacts.append({
                'name': name,
                'email': '',
                'title': title_found.title() if title_found else '',
                'source': 'website',
            })
            break

    # Strategy B: Structured team cards via CSS classes
    for selector in ['[class*="team"]', '[class*="staff"]', '[class*="bio"]',
                     '[class*="member"]', '[class*="advisor"]']:
        cards = soup.select(selector)
        for card in cards[:10]:
            card_text = card.get_text(separator='\n')
            names_in_card = NAME_PATTERN.findall(card_text)
            titles_in_card = TITLE_SEARCH_PATTERN.findall(card_text)
            if names_in_card and titles_in_card:
                name = names_in_card[0].strip()
                if len(name) < 4 or len(name) > 40:
                    continue
                name_words = name.upper().split()
                if any(w in name_words for w in CORP_WORDS):
                    continue
                if any(w in FALSE_NAME_WORDS for w in name_words):
                    continue
                contacts.append({
                    'name': name,
                    'email': '',
                    'title': titles_in_card[0].strip().title(),
                    'source': 'website',
                })

    # Assign emails to contacts by matching name parts to email local part
    for contact in contacts:
        if contact['email']:
            continue
        parts = contact['name'].lower().split()
        if not parts:
            continue
        for email in emails:
            local = email.split('@')[0].lower()
            if any(part in local for part in parts if len(part) > 2):
                contact['email'] = email
                break

    # If we found emails but no named contacts, create entries from emails
    if emails and not contacts:
        for email in emails[:3]:
            contacts.append({
                'name': '',
                'email': email,
                'title': '',
                'source': 'website',
            })

    return contacts


def _scrape_website_contacts(website_url):
    """Scrape a firm's website for contact information.

    Crawls homepage plus common subpages. Rate-limits with 0.3s delay.
    Returns list of contact dicts from all pages combined.
    """
    if not website_url or str(website_url).lower().strip() in ('nan', '', 'none'):
        return []

    url = str(website_url).strip()
    if not url.startswith('http'):
        url = 'https://' + url

    domain = extract_domain(website_url)
    all_contacts = []
    fetched_urls = set()

    # Fetch homepage
    soup = _fetch_page_soup(url)
    if soup is None:
        if url.startswith('https://'):
            url = 'http://' + url[8:]
            soup = _fetch_page_soup(url)
        if soup is None:
            return []

    fetched_urls.add(url.rstrip('/'))
    all_contacts.extend(_extract_contacts_from_soup(soup, domain))

    # Discover subpage links from homepage navigation
    discovered_subpages = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower().strip()
        for subpage in CONTACT_SUBPAGES:
            if subpage.lstrip('/') in href:
                full_url = urljoin(url, a_tag['href'])
                discovered_subpages.add(full_url.rstrip('/'))
                break

    # Also try standard subpage paths directly
    base = url.rstrip('/')
    for subpage in CONTACT_SUBPAGES:
        discovered_subpages.add(f'{base}{subpage}')

    # Fetch subpages (limit to 6)
    subpage_count = 0
    for sub_url in discovered_subpages:
        if sub_url in fetched_urls:
            continue
        if subpage_count >= 6:
            break

        time.sleep(0.3)
        sub_soup = _fetch_page_soup(sub_url)
        if sub_soup:
            fetched_urls.add(sub_url)
            all_contacts.extend(_extract_contacts_from_soup(sub_soup, domain))
            subpage_count += 1

    return all_contacts


def _select_best_contact(candidates):
    """Select the single best contact from all discovered candidates.

    Priority: (1) has both name+email, (2) higher title rank, (3) Hunter.io source.
    Returns dict with keys: name, email, title (any may be empty string).
    """
    empty = {'name': '', 'email': '', 'title': ''}
    if not candidates:
        return empty

    def _title_rank(title):
        t = title.lower().strip()
        for i, priority_title in enumerate(TITLE_PRIORITY):
            if priority_title in t:
                return i
        return 999

    def _sort_key(contact):
        has_both = bool(contact.get('name')) and bool(contact.get('email'))
        has_email = bool(contact.get('email'))
        title_rank = _title_rank(contact.get('title', ''))
        source_rank = 0 if contact.get('source') == 'hunter.io' else 1
        return (not has_both, not has_email, title_rank, source_rank)

    candidates_sorted = sorted(candidates, key=_sort_key)
    best = candidates_sorted[0]

    return {
        'name': best.get('name', '').strip(),
        'email': best.get('email', '').strip(),
        'title': best.get('title', '').strip(),
    }


def enrich_contact(website_url, hunter_api_key=None):
    """Discover the best contact for an RIA firm.

    Combines Hunter.io (if API key provided) and website scraping.

    Args:
        website_url: The firm's website URL from SEC data
        hunter_api_key: Optional Hunter.io API key (skip if None/empty)

    Returns:
        dict with keys: contact_name, contact_email, contact_title
    """
    all_candidates = []

    domain = extract_domain(website_url)

    # Method 1: Hunter.io (if configured)
    if hunter_api_key and domain:
        hunter_results = _hunter_domain_search(domain, hunter_api_key)
        all_candidates.extend(hunter_results)

    # Method 2: Website scraping
    scraped = _scrape_website_contacts(website_url)
    all_candidates.extend(scraped)

    best = _select_best_contact(all_candidates)

    return {
        'contact_name': best['name'],
        'contact_email': best['email'],
        'contact_title': best['title'],
    }

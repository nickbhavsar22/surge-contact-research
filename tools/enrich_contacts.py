"""
Contact enrichment tool for RIA firms.

Given a website URL, discovers contact name, email, title, phone, and
LinkedIn using:
1. Hunter.io Domain Search API (if API key configured)
2. Hunter.io Email Finder API (targeted fallback when name found but no email)
3. Website scraping (/contact, /about, /team pages)
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
    r'chief executive|chief operating|chief financial|partner|'
    r'director|managing director|advisor|adviser)',
    re.IGNORECASE
)

# Pattern to match a standalone title line (line is mostly just a title)
STANDALONE_TITLE_PATTERN = re.compile(
    r'^\s*(?:(?:chief\s+)?(?:compliance\s+officer|executive\s+officer|'
    r'operating\s+officer|financial\s+officer)|'
    r'cco|ceo|coo|cfo|principal|managing\s+(?:member|partner|director)|'
    r'founder(?:\s*[&+]\s*(?:managing\s+partner|ceo|president))?|'
    r'co-founder|president|owner|partner|director|'
    r'(?:senior\s+)?(?:advisor|adviser)|'
    r'(?:head\s+of\s+\w+(?:\s+\w+)?)|'
    r'(?:portfolio\s+manager))'
    r'(?:\s*[&+,/]\s*(?:(?:chief\s+)?(?:compliance\s+officer|executive\s+officer|'
    r'operating\s+officer|financial\s+officer)|'
    r'cco|ceo|coo|cfo|principal|managing\s+(?:member|partner|director)|'
    r'founder|president|owner|partner|director|'
    r'head\s+of\s+\w+(?:\s+\w+)?|portfolio\s+manager))*'
    r'\s*$',
    re.IGNORECASE
)

# Pattern to extract a person name from the start of a bio paragraph
# Matches: "Sam Caspersen has...", "Voltaire is the...", "Todd C. Schneider is..."
BIO_NAME_PATTERN = re.compile(
    r'^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    r'(?:\s+is\b|\s+has\b|\s+joined\b|\s+serves?\b|\s+leads?\b'
    r'|\s+brings?\b|\s+founded\b|\s+manages?\b|\s+oversees?\b'
    r'|,\s|\s+\(|\s+–\s|\s+—\s)'
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
    'WHO', 'WE', 'ARE', 'OUR', 'YOUR', 'MEET', 'WORK', 'WITH',
    'HOW', 'WHY', 'WHAT', 'GET', 'STARTED', 'READY', 'FIRM',
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
    Returns list of dicts with keys: name, email, title, source,
    confidence, seniority, department, phone, linkedin, verified.
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
            verification = entry.get('verification') or {}
            results.append({
                'name': name,
                'email': entry.get('value', ''),
                'title': entry.get('position') or '',
                'source': 'hunter.io',
                'confidence': entry.get('confidence') or 0,
                'seniority': entry.get('seniority') or '',
                'department': entry.get('department') or '',
                'phone': entry.get('phone_number') or '',
                'linkedin': entry.get('linkedin') or '',
                'verified': verification.get('status') or '',
            })
        return results

    except requests.RequestException as e:
        logger.debug('Hunter.io request failed for %s: %s', domain, e)
        return []


def _hunter_email_finder(domain, first_name, last_name, api_key):
    """Find the most likely email for a person at a domain.

    Uses 1 credit per successful lookup. Call only when we have a name
    from scraping but no email.
    Returns dict with keys: email, confidence, phone, linkedin, verified
    or empty dict on failure.
    """
    if not domain or not api_key or not last_name:
        return {}

    url = 'https://api.hunter.io/v2/email-finder'
    params = {
        'domain': domain,
        'first_name': first_name,
        'last_name': last_name,
        'api_key': api_key,
    }

    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (429, 401):
            return {}
        if resp.status_code != 200:
            logger.debug('Hunter.io Email Finder returned HTTP %d', resp.status_code)
            return {}

        data = resp.json().get('data', {})
        email = data.get('email') or ''
        if not email:
            return {}

        verification = data.get('verification') or {}
        return {
            'email': email,
            'confidence': data.get('score') or 0,
            'phone': data.get('phone_number') or '',
            'linkedin': data.get('linkedin') or '',
            'verified': verification.get('status') or '',
        }

    except requests.RequestException as e:
        logger.debug('Hunter.io Email Finder failed: %s', e)
        return {}


def get_hunter_account_info(api_key):
    """Check Hunter.io account status (remaining credits).

    Free endpoint — does not consume credits.
    Returns dict with keys: used, limit, remaining (or empty on failure).
    """
    if not api_key:
        return {}

    try:
        resp = requests.get(
            'https://api.hunter.io/v2/account',
            params={'api_key': api_key},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return {}

        data = resp.json().get('data', {})
        requests_info = data.get('requests') or {}
        searches = requests_info.get('searches') or {}
        return {
            'used': searches.get('used') or 0,
            'limit': searches.get('available') or 0,
        }
    except requests.RequestException:
        return {}


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
            raw = href[7:].split('?')[0].strip()
            # Strip URL encoding artifacts (%20, +, etc.)
            raw = raw.replace('%20', '').replace('+', '').strip()
            email = raw.lower()
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


def _is_valid_person_name(name):
    """Check if a string looks like a real person name (not a product/heading)."""
    if not name or len(name) < 4 or len(name) > 50:
        return False
    name_words = name.upper().split()
    if len(name_words) < 2:
        return False
    if any(w in name_words for w in CORP_WORDS):
        return False
    if any(w in FALSE_NAME_WORDS for w in name_words):
        return False
    return True


def _extract_contacts_from_soup(soup, domain):
    """Extract structured contacts (name + title + email) from a page.

    Three strategies for how RIA websites typically display team info:
    A) Title on standalone line, name starts the next paragraph
       (most common pattern on small RIA sites)
    B) "Name, Title" or "Name - Title" on the same line
    C) Structured HTML team cards via CSS classes
    Then assigns emails to named contacts by matching name parts.
    """
    contacts = []
    emails = _extract_emails_from_soup(soup, domain)
    text_content = soup.get_text(separator='\n')

    lines = text_content.split('\n')
    # Clean up: strip whitespace from all lines for consistent matching
    stripped = [line.strip() for line in lines]

    # Strategy A: Title on standalone line → name starts next non-empty line
    # Pattern seen on real RIA sites:
    #   "Founder & Managing Partner"    ← standalone title line
    #   ""                              ← blank
    #   "Sam Caspersen has fifteen..."  ← name starts the bio
    for i, line in enumerate(stripped):
        if not line:
            continue
        # Check if this line is a standalone title (short line, mostly title text)
        if len(line) > 80:
            continue
        if not STANDALONE_TITLE_PATTERN.match(line):
            continue

        title_found = line.strip()

        # Look forward for name in the next non-empty line(s)
        # Use a wide window (10 lines) because extracted text often has many blanks
        for j in range(i + 1, min(i + 10, len(stripped))):
            next_line = stripped[j]
            if not next_line:
                continue
            # Try to extract a person name starting the bio paragraph
            bio_match = BIO_NAME_PATTERN.match(next_line)
            if bio_match:
                name = bio_match.group(1).strip()
                if _is_valid_person_name(name):
                    contacts.append({
                        'name': name,
                        'email': '',
                        'title': title_found.title(),
                        'source': 'website',
                    })
            break  # Only check the first non-empty line after title

    # Strategy B: "Name, Title" or "Name - Title" on the same line
    same_line_pattern = re.compile(
        r'\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)\s*[,|–—\-]\s*'
        r'((?:Chief\s+)?(?:Compliance\s+Officer|CCO|Principal|Managing\s+(?:Member|Partner|Director)'
        r'|Founder|Co-Founder|CEO|President|Owner|COO|CFO|Partner|Director'
        r'|Chief\s+Executive|Chief\s+Operating|Chief\s+Financial|Advisor|Adviser))',
        re.IGNORECASE
    )
    for line in stripped:
        match = same_line_pattern.search(line)
        if match:
            name = match.group(1).strip()
            title_found = match.group(2).strip()
            if _is_valid_person_name(name):
                contacts.append({
                    'name': name,
                    'email': '',
                    'title': title_found.title(),
                    'source': 'website',
                })

    # Strategy C: Structured team cards via CSS classes
    for selector in ['[class*="team"]', '[class*="staff"]', '[class*="bio"]',
                     '[class*="member"]', '[class*="advisor"]',
                     '[class*="person"]', '[class*="profile"]']:
        cards = soup.select(selector)
        for card in cards[:10]:
            card_text = card.get_text(separator='\n')
            card_lines = [cl.strip() for cl in card_text.split('\n') if cl.strip()]
            # Look for title line + name in card
            for ci, cline in enumerate(card_lines):
                if len(cline) > 80:
                    continue
                if STANDALONE_TITLE_PATTERN.match(cline):
                    # Check surrounding lines for a name
                    for offset in [-1, 1, -2, 2]:
                        ni = ci + offset
                        if 0 <= ni < len(card_lines):
                            name_match = NAME_PATTERN.match(card_lines[ni])
                            if name_match and _is_valid_person_name(name_match.group(1)):
                                contacts.append({
                                    'name': name_match.group(1).strip(),
                                    'email': '',
                                    'title': cline.title(),
                                    'source': 'website',
                                })
                                break
                    break
            # Also try: NAME_PATTERN + TITLE in card without standalone title line
            names_in_card = NAME_PATTERN.findall(card_text)
            titles_in_card = TITLE_SEARCH_PATTERN.findall(card_text)
            if names_in_card and titles_in_card:
                name = names_in_card[0].strip()
                if _is_valid_person_name(name):
                    # Avoid duplicating contacts already found
                    if not any(c['name'] == name for c in contacts):
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


def _seniority_rank(seniority):
    """Rank Hunter.io seniority levels (lower = more senior)."""
    ranks = {'executive': 0, 'senior': 1, 'management': 2}
    return ranks.get((seniority or '').lower(), 5)


def _select_best_contact(candidates):
    """Select the best contact by merging the best name+title with the best email.

    On small RIA sites, the founder's name/title and a contact email often appear
    as separate entries (e.g., "Sam Caspersen, CEO" on the about page and
    "marc@firm.com" in the footer). This function merges them:
    1. Find the best *named* contact (by title priority, seniority, confidence)
    2. Find the best *email* (prefer verified, domain-matching, non-generic)
    3. If the named contact has no email, attach the best available email

    Returns dict with keys: name, email, title, phone, linkedin
    (any may be empty string).
    """
    empty = {'name': '', 'email': '', 'title': '', 'phone': '', 'linkedin': ''}
    if not candidates:
        return empty

    def _title_rank(title):
        t = title.lower().strip()
        for i, priority_title in enumerate(TITLE_PRIORITY):
            if priority_title in t:
                return i
        return 999

    # Find the best named contact (must have a name)
    named = [c for c in candidates if c.get('name')]
    best_named = None
    if named:
        named.sort(key=lambda c: (
            _title_rank(c.get('title', '')),
            _seniority_rank(c.get('seniority', '')),
            0 if c.get('source') == 'hunter.io' else 1,
            -(c.get('confidence') or 0),
        ))
        best_named = named[0]

    # Find the best email across all candidates
    # Prefer verified > unverified, and emails matched to the best named contact
    best_email = ''
    best_email_candidate = None
    for c in candidates:
        email = c.get('email', '').strip()
        if email:
            if best_named and c.get('name') == best_named.get('name') and email:
                best_email = email
                best_email_candidate = c
                break
            if not best_email:
                best_email = email
                best_email_candidate = c
            elif c.get('verified') == 'valid' and (best_email_candidate or {}).get('verified') != 'valid':
                best_email = email
                best_email_candidate = c

    # Collect phone and linkedin from the best sources available
    phone = ''
    linkedin = ''
    for c in ([best_named, best_email_candidate] + candidates):
        if c is None:
            continue
        if not phone and c.get('phone'):
            phone = c['phone']
        if not linkedin and c.get('linkedin'):
            linkedin = c['linkedin']
        if phone and linkedin:
            break

    if best_named:
        return {
            'name': best_named.get('name', '').strip(),
            'email': best_named.get('email', '').strip() or best_email,
            'title': best_named.get('title', '').strip(),
            'phone': phone,
            'linkedin': linkedin,
        }
    elif best_email:
        return {'name': '', 'email': best_email, 'title': '', 'phone': phone, 'linkedin': linkedin}
    else:
        return empty


def enrich_contact(website_url, hunter_api_key=None):
    """Discover the best contact for an RIA firm.

    Combines Hunter.io (if API key provided) and website scraping.
    Uses Email Finder as a targeted fallback when scraping finds a name
    but no email.

    Args:
        website_url: The firm's website URL from SEC data
        hunter_api_key: Optional Hunter.io API key (skip if None/empty)

    Returns:
        dict with keys: contact_name, contact_email, contact_title,
        contact_phone, contact_linkedin
    """
    all_candidates = []

    domain = extract_domain(website_url)

    # Method 1: Hunter.io Domain Search (if configured)
    if hunter_api_key and domain:
        hunter_results = _hunter_domain_search(domain, hunter_api_key)
        all_candidates.extend(hunter_results)

    # Method 2: Website scraping
    scraped = _scrape_website_contacts(website_url)
    all_candidates.extend(scraped)

    best = _select_best_contact(all_candidates)

    # Method 3: Email Finder fallback — if we have a name but no email,
    # try a targeted lookup (1 credit). Only fires when needed.
    if hunter_api_key and domain and best['name'] and not best['email']:
        parts = best['name'].split()
        if len(parts) >= 2:
            found = _hunter_email_finder(domain, parts[0], parts[-1], hunter_api_key)
            if found.get('email'):
                best['email'] = found['email']
                if not best['phone'] and found.get('phone'):
                    best['phone'] = found['phone']
                if not best['linkedin'] and found.get('linkedin'):
                    best['linkedin'] = found['linkedin']

    return {
        'contact_name': best['name'],
        'contact_email': best['email'],
        'contact_title': best['title'],
        'contact_phone': best.get('phone', ''),
        'contact_linkedin': best.get('linkedin', ''),
    }

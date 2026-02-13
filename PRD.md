# PRD: Surge Contact Research — SEC RIA Lead Discovery Pipeline

## Overview

Surge Contact Research is a three-stage data pipeline that identifies newly registered Investment Advisers (RIAs) from the SEC's public FOIA database, scores them against SurgeONE.ai's ideal customer profile, and enriches records with contact information via website scraping and Hunter.io. The output is a filterable Streamlit dashboard with CSV export of sales-ready leads.

## Business Objective

Generate high-quality leads for financial services outreach by targeting firms in their earliest registration window — when they are most likely to need services (compliance, technology, marketing, operations). Newly registered RIAs represent a "surge" buying signal: they have budget, urgency, and unmet vendor needs.

## Data Sources

| Source | URL Pattern | Format | Update Frequency |
|--------|------------|--------|-----------------|
| SEC FOIA Investment Adviser Database | `sec.gov/files/investment/data/.../ia*.zip` | ZIP → CSV | Monthly |
| RIA Firm Websites | Scraped from Website Address in SEC data | HTML | Live |
| Hunter.io Domain Search API | `api.hunter.io/v2/domain-search` | JSON | Live (50/month free tier) |

## Pipeline Stages

### Stage 1: Discovery (`get_recent_rias.py`)

**Purpose:** Identify newly registered RIAs within a configurable lookback window.

**Process:**
1. Download the SEC FOIA Investment Adviser ZIP archive
2. Extract and parse the CSV (latin-1 encoded)
3. Convert `SEC Status Effective Date` and `Latest ADV Filing Date` to datetime
4. Filter for registrations within the last N days (default: 30) relative to the most recent data point
5. Select and rename key columns for clean output
6. Export to timestamped CSV

**Input:** SEC FOIA ZIP file (remote)
**Output:** `new_rias_YYYYMMDD.csv`

### Stage 2: Fit Scoring (`score_fit.py`)

**Purpose:** Score each RIA against SurgeONE.ai's ideal customer profile using data signals and website content analysis.

**Scoring Rubric (normalized to 0-100):**
- **Data score (max 50 pts):** website presence (8), phone (3), name keywords indicating advisory focus (6+4), top financial state (4), employees (up to 10), AUM (up to 10), clients (up to 5)
- **Website score (max 70 pts):** site reachable (5), compliance keywords (14), advisory services (12), cybersecurity (11), team section (10), client/AUM info (10), technology (8)

Firms with insufficient data (data score <= 3 and no website) are marked "N/A".

### Stage 3: Contact Enrichment (`tools/enrich_contacts.py`)

**Purpose:** Enrich each RIA record with contact name, email, and title by scraping their website and querying Hunter.io.

**Process:**
1. Query Hunter.io Domain Search API for employee emails (if API key configured)
2. Scrape firm homepage + common subpages (/contact, /about, /team, /leadership, /advisors, /bio)
3. Extract contacts using three strategies:
   - **Strategy A:** Standalone title line followed by name in bio paragraph
   - **Strategy B:** Name and title on same line (e.g., "Sam Caspersen, CEO")
   - **Strategy C:** Structured HTML team cards (CSS class matching)
4. Select best contact by title priority (CCO > Principal > Managing Member > VP > etc.)
5. Match contact names to extracted emails by comparing name parts to email local part
6. Filter out corporate entities, generic emails (info@, support@), and government domains

**Title Priority:** Chief Compliance Officer, CCO, Principal, Managing Member, Managing Director, Managing Partner, CEO, President, Founder, Owner, Partner, Director, VP

## Data Schema

### Stage 1 Output Fields

| Field | Source Column | Description |
|-------|--------------|-------------|
| `Company` | Primary Business Name | DBA name of the firm |
| `CRD` | Organization CRD# | Central Registration Depository number (unique identifier) |
| `Registered` | SEC Status Effective Date | Date the firm's SEC registration became effective |
| `Status` | SEC Current Status | Current registration status |
| `City` | Main Office City | Headquarters city |
| `State` | Main Office State | Headquarters state |
| `Phone` | Main Office Telephone Number | Primary phone number |
| `Website` | Website Address | Firm website |
| `Legal_Name` | Legal Name | Legal entity name |

### Fit Scoring Fields

| Field | Source | Description |
|-------|--------|-------------|
| `Fit_Score` | `score_fit.py` | 0-100 normalized score (or "N/A" for insufficient data) |
| `Fit_Reasons` | `score_fit.py` | Semicolon-delimited breakdown of scoring factors |

### Contact Enrichment Fields

| Field | Extraction Method | Description |
|-------|------------------|-------------|
| `Contact_Name` | Website scraping + Hunter.io | Best-matched person name by title priority |
| `Contact_Email` | Hunter.io API + email regex | Business email matched to contact name |
| `Contact_Title` | Title pattern matching | Role title (e.g., "Chief Compliance Officer", "Principal") |

## Technical Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `streamlit` | 1.54.0 | Web UI framework |
| `requests` | 2.32.5 | HTTP client for SEC downloads and website scraping |
| `pandas` | 2.3.3 | Data manipulation and CSV I/O |
| `beautifulsoup4` | 4.13.4 | HTML parsing for contact extraction |
| `zipfile` | stdlib | Extract SEC FOIA ZIP archives |

## Rate Limiting & Compliance

- **SEC download:** 300s timeout, candidate URL fallback (tries multiple monthly snapshots)
- **Website scraping:** 0.3s delay between subpage fetches, 15s timeout per request
- **Contact enrichment:** 0.5s delay between firms during batch enrichment
- **Hunter.io:** Free tier limited to 50 domain searches/month; 429 errors caught and logged
- **User-Agent:** Standard browser UA string to comply with website access policies
- **Data source:** All data is from publicly available SEC FOIA datasets and public RIA firm websites

## Known Limitations

1. **Website-dependent enrichment:** Contact extraction relies on scraping firm websites. Sites with non-standard layouts, JavaScript-rendered content, or no team/contact pages may not yield results.
2. **Monthly data lag:** The SEC FOIA database is updated monthly, so the date filter is relative to the snapshot date, not real-time.
3. **Hunter.io quota:** Free tier allows 50 domain searches/month. Beyond that, enrichment falls back to website scraping only.
4. **Single contact per firm:** Only the highest-priority contact is returned per firm; additional contacts are discarded.
5. **Ephemeral cache:** SQLite cache on Streamlit Cloud resets on reboot/redeploy. Scores and enrichments are recomputed as needed (fast enough for the dataset size).
6. **SEC CCO data unavailable:** Item 1.J (Chief Compliance Officer) is deliberately excluded from the public FOIA CSV, and Form ADV PDFs render as flat images without extractable text for filled-in values.

## Future Enhancements (Suggested)

- Add LinkedIn profile matching for enriched contacts
- Persistent database backend for deduplication and historical tracking
- Add configurable output formats (JSON, Excel)
- Integrate with CRM via API for direct lead import
- Async website fetching for faster batch enrichment

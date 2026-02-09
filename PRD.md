# PRD: Surge Contact Research — SEC RIA Lead Discovery Pipeline

## Overview

Surge Contact Research is a two-stage data pipeline that identifies newly registered Investment Advisers (RIAs) from the SEC's public FOIA database and enriches those records with contact information by parsing Form ADV PDF filings. The output is a sales-ready CSV of recently registered financial advisory firms with key contact details.

## Business Objective

Generate high-quality leads for financial services outreach by targeting firms in their earliest registration window — when they are most likely to need services (compliance, technology, marketing, operations). Newly registered RIAs represent a "surge" buying signal: they have budget, urgency, and unmet vendor needs.

## Data Sources

| Source | URL Pattern | Format | Update Frequency |
|--------|------------|--------|-----------------|
| SEC FOIA Investment Adviser Database | `sec.gov/files/investment/data/.../ia*.zip` | ZIP → CSV | Monthly |
| Form ADV PDF Filings | `reports.adviserinfo.sec.gov/reports/ADV/{CRD}/PDF/{CRD}.pdf` | PDF | Per-filing |

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

### Stage 2: Enrichment (`scrape_ria_contacts.py`)

**Purpose:** Enrich each RIA record with contact name, email, and title by parsing their Form ADV PDF.

**Process:**
1. Load the Stage 1 CSV output
2. For each firm (by CRD number), download the Form ADV PDF from SEC
3. Extract text from the first 15 pages using `pdfplumber`
4. Apply regex patterns to extract:
   - **Principal/Owner name** — from the "your last, first, and middle names" section
   - **Chief Compliance Officer** — from section J of the filing (takes priority over principal)
   - **Email addresses** — first valid non-SEC/FINRA email found
5. Filter out corporate entities (LLC, INC, LTD, etc.) from name fields
6. Save enriched CSV with `Contact_Name`, `Contact_Email`, `Contact_Title` columns

**Input:** `new_rias_contacts_30days.csv`
**Output:** `new_rias_contacts_with_names.csv`

### Stage 3 (Prototype): PDF Extraction Testing (`test_pdf_extract.py`)

**Purpose:** Standalone test script used during development to validate regex extraction logic against known CRD numbers.

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

### Stage 2 Enrichment Fields

| Field | Extraction Method | Description |
|-------|------------------|-------------|
| `Contact_Name` | Regex on Form ADV text | Principal owner or CCO name |
| `Contact_Email` | Regex email pattern | First valid business email found |
| `Contact_Title` | Derived from extraction source | "Principal/Owner" or "Chief Compliance Officer" |

## Technical Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | * | HTTP client for SEC downloads |
| `pdfplumber` | * | PDF text extraction from Form ADV filings |
| `pandas` | * | Data manipulation and CSV I/O |
| `zipfile` | stdlib | Extract SEC FOIA ZIP archives |

## Rate Limiting & Compliance

- **Rate limit:** 1-second delay between PDF requests (`time.sleep(1)`)
- **User-Agent:** Standard browser UA string to comply with SEC access policies
- **Timeout:** 45s per PDF request, 120s for ZIP download
- **Page limit:** Only first 15 pages of each PDF are processed (performance optimization)
- **Data source:** All data is from publicly available SEC FOIA datasets and public Form ADV filings

## Known Limitations

1. **Regex-based extraction:** Contact name and title extraction relies on regex patterns matching specific Form ADV formatting. PDFs with non-standard layouts may not yield results.
2. **Monthly data lag:** The SEC FOIA database is updated monthly, so the "last 30 days" filter is relative to the snapshot date, not real-time.
3. **Hardcoded ZIP URL:** The SEC FOIA ZIP URL in `get_recent_rias.py` includes a date-stamped filename (`ia010226.zip`) that must be updated manually when new snapshots are released.
4. **Single email capture:** Only the first valid non-government email is captured per firm; additional contacts are discarded.
5. **No deduplication:** Running the pipeline multiple times may produce overlapping records if the SEC data overlaps between snapshots.
6. **No persistent storage:** Results are CSV-only with no database backend for historical tracking.

## Future Enhancements (Suggested)

- Automate ZIP URL discovery by scraping the SEC FOIA index page
- Add LinkedIn profile matching for enriched contacts
- Store results in a database for deduplication and historical tracking
- Add configurable output formats (JSON, Excel)
- Integrate with CRM via API for direct lead import

# Surge Contact Research

Discover newly registered Investment Advisers (RIAs) from the SEC's public FOIA database, score them against SurgeONE.ai's ideal customer profile, and enrich records with contact information via website scraping and Hunter.io. The output is a filterable, downloadable dashboard of sales-ready leads.

## Architecture

```
SEC FOIA ZIP (monthly, ~100MB)
    |
    v
[GitHub Actions: nightly download] --> data/sec_advisers.csv (committed)
    |
    v
[app.py] --> get_recent_rias.py (filter by date range)
    |
    v
[score_fit.py] --> Data scoring (employees, AUM, clients, state, name keywords)
                   Website scoring (compliance, advisory, cybersecurity, team, tech keywords)
    |
    v
[tools/enrich_contacts.py] --> Hunter.io API + website scraping (contact name, email, title)
    |
    v
Streamlit dashboard --> filterable table + CSV export
```

## Prerequisites

- Python 3.11+
- A Streamlit Cloud account (for deployment) or local terminal (for development)

## Local Setup

```bash
# Clone the repo
git clone https://github.com/nickbhavsar22/surge-contact-research.git
cd surge-contact-research

# Install dependencies
pip install -r requirements.txt

# Configure secrets (see .env.example for reference)
# Option A: Streamlit secrets file (recommended)
mkdir -p .streamlit
cp .env.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml with your values

# Option B: Environment variables
export app_password="your_password"
export HUNTER_API_KEY="your_key"  # optional

# Run the app
streamlit run app.py
```

The app will open at `http://localhost:8501`.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `app_password` | Yes | Password to access the app (checked via SHA256 hash) |
| `hunter_api_key` | No | Hunter.io API key for email enrichment. Free tier: 50 searches/month. Without this, contact enrichment still works via website scraping only. |

## Deployment (Streamlit Community Cloud)

1. Push code to GitHub (private repo)
2. Connect repo to [Streamlit Community Cloud](https://share.streamlit.io/)
3. Configure secrets via the Streamlit Cloud dashboard (not in the repo):
   - `app_password = "your_password"`
   - `hunter_api_key = "your_key"` (optional)
4. The app deploys automatically on push to `main`

**Nightly automation:** GitHub Actions runs `tools/update_sec_data.py` daily at 6 AM UTC to refresh `data/sec_advisers.csv` with the latest SEC FOIA snapshot.

## Project Structure

```
app.py                  # Streamlit UI (main entry point)
get_recent_rias.py      # SEC database fetch and date filtering
score_fit.py            # ICP fit scoring engine (data + website signals)
cache_db.py             # SQLite cache for scores and enrichments
tools/
  enrich_contacts.py    # Website + Hunter.io contact discovery
  update_sec_data.py    # SEC ZIP download (used by GitHub Actions)
data/
  sec_advisers.csv      # Pre-downloaded SEC data (committed, auto-updated)
.github/workflows/
  update-sec-data.yml   # Nightly SEC data refresh
PRD.md                  # Product requirements document
```

## Further Reading

See [PRD.md](PRD.md) for the full product requirements, data schema, scoring rubric, and known limitations.

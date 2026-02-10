"""
Surge Contact Research — Streamlit UI
Discover newly registered RIAs, score them against SurgeONE.ai's ICP.
"""

import streamlit as st
import pandas as pd
import html
import hashlib
import logging
import base64
import time
import os
from pathlib import Path
from datetime import date, timedelta

from get_recent_rias import get_recent_rias
from score_fit import calculate_fit_score
from cache_db import lookup_scores, save_scores, lookup_enrichments, save_enrichments
from tools.enrich_contacts import enrich_contact

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Logo (base64-encoded for inline HTML)
# ---------------------------------------------------------------------------
_logo_dir = Path(__file__).parent
_logo_light_path = _logo_dir / "logo-transparent.png"
_logo_dark_path = _logo_dir / "logo.png"

_logo_light_b64 = base64.b64encode(_logo_light_path.read_bytes()).decode() if _logo_light_path.exists() else ""
_logo_dark_b64 = base64.b64encode(_logo_dark_path.read_bytes()).decode() if _logo_dark_path.exists() else ""


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_get_recent_rias(start_date, end_date):
    """Cache SEC ZIP download for 1 hour to avoid repeated 100MB+ downloads."""
    return get_recent_rias(start_date=start_date, end_date=end_date)

# ---------------------------------------------------------------------------
# Page config & branding
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title='SurgeONE.ai',
    page_icon='📡',
    layout='wide',
    initial_sidebar_state='expanded',
)

BRAND_CSS = """
<style>
    /* Header bar */
    .brand-header {
        background: linear-gradient(135deg, #070B14 0%, #0f172a 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        border: 1px solid #1e293b;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .brand-header img {
        max-height: 70px;
        width: auto;
    }

    /* Sidebar logo */
    .sidebar-logo {
        text-align: center;
        padding: 0.5rem 0 1rem 0;
    }
    .sidebar-logo img {
        max-height: 40px;
        width: auto;
        opacity: 0.9;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: #070B14;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 1rem;
    }
    div[data-testid="stMetric"] label {
        color: #8B99AD !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #0EA5E9 !important;
    }

    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: #070B14;
        border-right: 1px solid #1e293b;
    }
    section[data-testid="stSidebar"] .stMarkdown h3 {
        color: #3B82F6;
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown span,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stCaption,
    section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] label,
    section[data-testid="stSidebar"] [data-testid="stCaption"] {
        color: #cbd5e1 !important;
    }
    section[data-testid="stSidebar"] hr {
        border-color: #1e293b;
    }
    section[data-testid="stSidebar"] [data-testid="stDateInput"] label {
        color: #cbd5e1 !important;
    }

    /* Primary buttons */
    .stButton > button[kind="primary"] {
        background: #3B82F6;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
    }
    .stButton > button[kind="primary"]:hover {
        background: #2563eb;
    }

    /* Download button */
    .stDownloadButton > button {
        background: #0EA5E9;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
    }

    /* Info boxes */
    .info-box {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 8px;
        padding: 1rem;
        color: #8B99AD;
        font-size: 0.85rem;
        margin-top: 1rem;
    }

    /* Progress text */
    .progress-text {
        color: #8B99AD;
        font-family: monospace;
        font-size: 0.8rem;
    }
</style>
"""

st.markdown(BRAND_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Authentication gate
# ---------------------------------------------------------------------------

def _check_password():
    """Return True if the user has entered the correct password."""
    expected = st.secrets.get("app_password", "")
    if not expected:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.markdown(
        '<div class="brand-header">'
        f'<img src="data:image/png;base64,{_logo_light_b64}" alt="SurgeONE.ai">'
        '</div>',
        unsafe_allow_html=True,
    )

    password = st.text_input("Enter password to continue", type="password")
    if password:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        expected_hash = hashlib.sha256(expected.encode()).hexdigest()
        if password_hash == expected_hash:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

if not _check_password():
    st.stop()


def _get_hunter_api_key():
    """Get Hunter.io API key from Streamlit secrets or environment.
    Returns None if not configured (enrichment still works via scraping only).
    """
    key = st.secrets.get("hunter_api_key", "")
    if key:
        return key
    return os.environ.get("HUNTER_API_KEY", "") or None

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if 'discovered_df' not in st.session_state:
    st.session_state.discovered_df = None
if 'scored_df' not in st.session_state:
    st.session_state.scored_df = None
if 'discovery_stats' not in st.session_state:
    st.session_state.discovery_stats = None
if 'scoring_stats' not in st.session_state:
    st.session_state.scoring_stats = None

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown(
    '<div class="brand-header">'
    f'<img src="data:image/png;base64,{_logo_light_b64}" alt="SurgeONE.ai">'
    '</div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar — Controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        f'<div class="sidebar-logo">'
        f'<img src="data:image/png;base64,{_logo_light_b64}" alt="SurgeONE.ai">'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown('### Date Range')
    st.caption('Select the registration date window to search')

    col_start, col_end = st.columns(2)
    with col_start:
        start_date = st.date_input(
            'From',
            value=date.today() - timedelta(days=30),
            max_value=date.today(),
            format='MM/DD/YYYY',
        )
    with col_end:
        end_date = st.date_input(
            'To',
            value=date.today(),
            max_value=date.today(),
            format='MM/DD/YYYY',
        )

    if start_date > end_date:
        st.error('Start date must be before end date.')

    st.markdown('---')

    st.markdown('### Discover & Score')
    st.caption('Find new RIAs and auto-score their fit for SurgeONE.ai')
    discover_btn = st.button(
        'Find New RIAs',
        type='primary',
        use_container_width=True,
        disabled=(start_date > end_date),
    )

    # Database info
    if st.session_state.discovery_stats:
        stats = st.session_state.discovery_stats
        st.markdown('---')
        st.markdown('### Database Info')
        safe_date = html.escape(str(stats["snapshot_date"]))
        safe_total = html.escape(f'{stats["total_records"]:,}')
        st.markdown(
            f'<div class="info-box">'
            f'<strong>Snapshot:</strong> {safe_date}<br>'
            f'<strong>Total advisers:</strong> {safe_total}<br>'
            f'<strong>Source:</strong> SEC FOIA'
            f'</div>',
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_aum(val):
    """Format AUM integer to human-readable string (e.g., $1.2B, $500M)."""
    try:
        n = int(float(val))
    except (ValueError, TypeError):
        return ''
    if n <= 0:
        return ''
    if n >= 1_000_000_000:
        return f'${n / 1_000_000_000:.1f}B'
    if n >= 1_000_000:
        return f'${n / 1_000_000:.0f}M'
    if n >= 1_000:
        return f'${n / 1_000:.0f}K'
    return f'${n}'


def _build_col_config(df):
    """Build Streamlit column config dict for the results table."""
    config = {
        'CRD': st.column_config.NumberColumn('CRD #', format='%d'),
        'Registered': st.column_config.DateColumn('Registered', format='YYYY-MM-DD'),
        'Website': st.column_config.LinkColumn('Website'),
    }
    if 'Fit_Score' in df.columns:
        config['Fit_Score'] = st.column_config.TextColumn('Fit Score')
    if 'AUM_Display' in df.columns:
        config['AUM_Display'] = st.column_config.TextColumn('AUM')
    if 'Employees' in df.columns:
        config['Employees'] = st.column_config.NumberColumn('Employees', format='%d')
    if 'Clients' in df.columns:
        config['Clients'] = st.column_config.NumberColumn('Clients', format='%d')
    if 'Contact_Name' in df.columns:
        config['Contact_Name'] = st.column_config.TextColumn('Contact')
    if 'Contact_Email' in df.columns:
        config['Contact_Email'] = st.column_config.TextColumn('Email')
    if 'Contact_Title' in df.columns:
        config['Contact_Title'] = st.column_config.TextColumn('Title')
    return config


def _display_cols(df):
    """Return columns to show (hide internal columns from table)."""
    hide = {'Fit_Reasons', 'AUM', 'Legal_Name'}
    return [c for c in df.columns if c not in hide]


def _sort_by_fit_score(df):
    """Sort DataFrame: numeric Fit_Score descending, N/A at bottom."""
    if 'Fit_Score' not in df.columns:
        return df
    numeric_mask = df['Fit_Score'] != 'N/A'
    df_num = df[numeric_mask].copy()
    df_na = df[~numeric_mask].copy()
    df_num['_sort'] = pd.to_numeric(df_num['Fit_Score'], errors='coerce')
    df_num = df_num.sort_values('_sort', ascending=False).drop(columns=['_sort'])
    return pd.concat([df_num, df_na], ignore_index=True)


def _safe_crd(val):
    """Convert a CRD value to int, or return None if invalid."""
    try:
        crd = int(val)
        return crd if crd > 0 else None
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Discovery + live fit scoring (with cache)
# ---------------------------------------------------------------------------

if discover_btn:
    # -- Phase 1: Download & filter SEC database --
    status_container = st.empty()
    messages = []

    def discovery_progress(msg):
        messages.append(msg)
        status_container.info('\n'.join(messages))

    with st.spinner('Downloading and filtering SEC database...'):
        discovery_progress('Downloading SEC database (cached for 1 hour)...')
        result = _cached_get_recent_rias(start_date, end_date)

    status_container.empty()

    if result['error']:
        logger.error('Discovery failed: %s', result['error'])
        st.error(f"Discovery failed: {result['error']}")
    elif result['df'].empty:
        st.session_state.discovery_stats = {
            'snapshot_date': result['snapshot_date'] or 'N/A',
            'total_records': result['total_records'],
        }
        st.info("No new registrations in this date range.")
    else:
        st.session_state.discovery_stats = {
            'snapshot_date': result['snapshot_date'] or 'N/A',
            'total_records': result['total_records'],
        }
        st.session_state.discovered_df = result['df']
        st.session_state.scored_df = None
        st.session_state.scoring_stats = None

        df = result['df'].copy()
        total = len(df)

        # Add formatted AUM display column
        if 'AUM' in df.columns:
            df['AUM_Display'] = df['AUM'].apply(_format_aum)

        # -- Phase 2: Check cache for existing scores and contacts --
        crd_list = [_safe_crd(c) for c in df['CRD']]
        valid_crds = [c for c in crd_list if c is not None]
        cached_scores = lookup_scores(valid_crds)
        cached_enrichments = lookup_enrichments(valid_crds)
        hunter_key = _get_hunter_api_key()

        # Initialize columns
        df['Fit_Score'] = '...'
        df['Fit_Reasons'] = ''
        df['Contact_Name'] = ''
        df['Contact_Email'] = ''
        df['Contact_Title'] = ''

        # Populate from cache
        cached_count = 0
        new_indices = []
        for idx, row in df.iterrows():
            crd = _safe_crd(row['CRD'])
            if crd and crd in cached_scores:
                df.at[idx, 'Fit_Score'] = cached_scores[crd]['fit_score']
                df.at[idx, 'Fit_Reasons'] = cached_scores[crd]['fit_reasons']
                cached_count += 1
            else:
                new_indices.append(idx)
            # Populate cached contacts regardless of score cache status
            if crd and crd in cached_enrichments:
                df.at[idx, 'Contact_Name'] = cached_enrichments[crd].get('contact_name', '')
                df.at[idx, 'Contact_Email'] = cached_enrichments[crd].get('contact_email', '')
                df.at[idx, 'Contact_Title'] = cached_enrichments[crd].get('contact_title', '')

        new_count = len(new_indices)
        st.success(
            f"Found **{total}** RIAs — **{cached_count}** from cache, "
            f"**{new_count}** to score."
        )

        # -- Phase 3: Score only NEW rows with live table updates --
        if new_count > 0:
            progress_bar = st.progress(0)
            progress_text = st.empty()
            table_placeholder = st.empty()

            table_placeholder.dataframe(
                df[_display_cols(df)],
                use_container_width=True,
                height=500,
                column_config=_build_col_config(df),
            )

            scored_count = 0
            na_count = 0
            new_score_records = []
            new_enrich_records = []

            for i, idx in enumerate(new_indices):
                position = i + 1
                row = df.loc[idx]
                company = str(row.get('Company', 'Unknown'))[:40]
                crd = _safe_crd(row['CRD'])
                website = str(row.get('Website', ''))

                progress_bar.progress(position / new_count)
                progress_text.markdown(
                    f'<p class="progress-text">[{position}/{new_count}] Scoring: {company}</p>',
                    unsafe_allow_html=True,
                )

                score_result = calculate_fit_score(row)
                df.at[idx, 'Fit_Score'] = score_result['Fit_Score']
                df.at[idx, 'Fit_Reasons'] = score_result['Fit_Reasons']

                if score_result['Fit_Score'] == 'N/A':
                    na_count += 1
                else:
                    scored_count += 1

                # Queue score for cache save
                if crd:
                    new_score_records.append({
                        'crd': crd,
                        'company': str(row.get('Company', '')),
                        'website': website,
                        'fit_score': str(score_result['Fit_Score']),
                        'fit_reasons': score_result['Fit_Reasons'],
                    })

                # Enrich contact (skip if already cached)
                has_website = website and website.lower() not in ('nan', '', 'none')
                already_enriched = crd and crd in cached_enrichments
                if has_website and not already_enriched:
                    progress_text.markdown(
                        f'<p class="progress-text">[{position}/{new_count}] Enriching: {company}</p>',
                        unsafe_allow_html=True,
                    )
                    contact = enrich_contact(website, hunter_api_key=hunter_key)
                    df.at[idx, 'Contact_Name'] = contact['contact_name']
                    df.at[idx, 'Contact_Email'] = contact['contact_email']
                    df.at[idx, 'Contact_Title'] = contact['contact_title']

                    if crd:
                        new_enrich_records.append({
                            'crd': crd,
                            'contact_name': contact['contact_name'],
                            'contact_email': contact['contact_email'],
                            'contact_title': contact['contact_title'],
                        })
                elif not has_website:
                    time.sleep(0.5)

                # Refresh table every 5 rows or on last row
                if position % 5 == 0 or position == new_count:
                    table_placeholder.dataframe(
                        df[_display_cols(df)],
                        use_container_width=True,
                        height=500,
                        column_config=_build_col_config(df),
                    )

            progress_bar.empty()
            progress_text.empty()
            table_placeholder.empty()

            # Persist new scores and enrichments to cache
            save_scores(new_score_records)
            save_enrichments(new_enrich_records)
        else:
            scored_count = 0
            na_count = 0

        # Sort and save
        df_final = _sort_by_fit_score(df)

        st.session_state.scored_df = df_final
        st.session_state.scoring_stats = {
            'scored': scored_count + cached_count,
            'na_count': na_count,
            'from_cache': cached_count,
            'newly_scored': scored_count + na_count,
        }

        if new_count > 0:
            st.success(
                f"Complete: **{scored_count}** newly scored, "
                f"**{cached_count}** from cache, "
                f"**{na_count}** N/A."
            )
        st.rerun()

# ---------------------------------------------------------------------------
# Main area — Results display
# ---------------------------------------------------------------------------

display_df = None
if st.session_state.scored_df is not None and not st.session_state.scored_df.empty:
    display_df = st.session_state.scored_df
elif st.session_state.discovered_df is not None and not st.session_state.discovered_df.empty:
    display_df = st.session_state.discovered_df

if display_df is not None:
    # Metrics row
    is_scored = st.session_state.scored_df is not None and 'Fit_Score' in display_df.columns

    if is_scored:
        s_stats = st.session_state.scoring_stats
        numeric_scores = pd.to_numeric(display_df['Fit_Score'], errors='coerce')
        avg_score = numeric_scores.mean()
        avg_label = f'{avg_score:.0f}' if pd.notna(avg_score) else 'N/A'

        # Calculate AUM summary
        aum_col = display_df.get('AUM')
        total_aum = 0
        if aum_col is not None:
            total_aum = pd.to_numeric(aum_col, errors='coerce').sum()
        aum_label = _format_aum(total_aum) if total_aum > 0 else 'N/A'

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric('Total RIAs', len(display_df))
        m2.metric('Avg Fit Score', avg_label)
        m3.metric('Total AUM', aum_label)
        m4.metric('From Cache', s_stats.get('from_cache', 0))
        m5.metric('States', display_df['State'].nunique())
    else:
        m1, m2, m3 = st.columns(3)
        m1.metric('Total RIAs', len(display_df))
        m2.metric('States', display_df['State'].nunique())
        m3.metric('Date Range', f"{start_date.strftime('%m/%d')} — {end_date.strftime('%m/%d')}")

    st.markdown('---')

    # Filters
    filter_col1, filter_col2 = st.columns([1, 3])
    with filter_col1:
        states = sorted(display_df['State'].dropna().unique().tolist())
        selected_states = st.multiselect('Filter by State', states, default=[])

    filtered_df = display_df.copy()
    if selected_states:
        filtered_df = filtered_df[filtered_df['State'].isin(selected_states)]

    # Data table
    st.dataframe(
        filtered_df[_display_cols(filtered_df)],
        use_container_width=True,
        height=500,
        column_config=_build_col_config(filtered_df),
    )

    st.caption(f'Showing {len(filtered_df)} of {len(display_df)} records')

    # Download (includes Fit_Reasons in CSV)
    csv_data = filtered_df.to_csv(index=False).encode('utf-8-sig')
    filename = f'surge_rias_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'

    st.download_button(
        label=f'Download CSV ({len(filtered_df)} records)',
        data=csv_data,
        file_name=filename,
        mime='text/csv',
    )


else:
    # Empty state
    st.markdown('---')
    st.markdown(
        '<div style="text-align:center; padding: 4rem 2rem; color: #8B99AD;">'
        '<h3 style="color:#0b4f6c;">Ready to discover new RIAs</h3>'
        '<p>Select a date range in the sidebar and click <strong>Find New RIAs</strong> to begin.</p>'
        '<p style="font-size:0.8rem; margin-top:2rem;">'
        'Data sourced from SEC FOIA Investment Adviser database &amp; Form ADV filings.'
        '</p>'
        '</div>',
        unsafe_allow_html=True,
    )

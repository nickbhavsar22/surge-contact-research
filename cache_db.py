"""
SQLite cache for RIA fit scores and enriched contacts.

Keyed by CRD number so repeated discovery runs skip already-processed firms.
"""

import sqlite3
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path(tempfile.gettempdir()) / "surge_cache.db"


def _connect():
    """Return a connection with WAL mode for better concurrent read performance."""
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the cache table if it doesn't exist, and migrate if needed."""
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ria_cache (
                crd            INTEGER PRIMARY KEY,
                company        TEXT,
                website        TEXT,
                fit_score      TEXT,
                fit_reasons    TEXT,
                scored_at      TEXT,
                contact_name   TEXT,
                contact_email  TEXT,
                contact_title  TEXT,
                enriched_at    TEXT,
                contact_phone    TEXT,
                contact_linkedin TEXT
            )
        """)
        # Migrate existing tables missing the new columns
        existing = {row[1] for row in conn.execute("PRAGMA table_info(ria_cache)").fetchall()}
        for col in ('contact_phone', 'contact_linkedin'):
            if col not in existing:
                conn.execute(f"ALTER TABLE ria_cache ADD COLUMN {col} TEXT")
        conn.commit()


def lookup_scores(crd_list):
    """Batch lookup cached fit scores.

    Args:
        crd_list: list of CRD numbers (int)

    Returns:
        dict[int, dict] mapping CRD → {'fit_score': ..., 'fit_reasons': ...}
        Only includes CRDs that have been scored (scored_at is not NULL).
    """
    if not crd_list:
        return {}

    init_db()
    result = {}
    with _connect() as conn:
        # SQLite has a variable limit (~999), so batch in chunks
        for i in range(0, len(crd_list), 500):
            chunk = crd_list[i:i + 500]
            placeholders = ','.join('?' * len(chunk))
            rows = conn.execute(
                f"SELECT crd, fit_score, fit_reasons FROM ria_cache "
                f"WHERE crd IN ({placeholders}) AND scored_at IS NOT NULL",
                chunk,
            ).fetchall()
            for row in rows:
                result[row['crd']] = {
                    'fit_score': row['fit_score'],
                    'fit_reasons': row['fit_reasons'] or '',
                }
    return result


def save_scores(records):
    """Upsert fit score data for a batch of RIAs.

    Args:
        records: list of dicts with keys: crd, company, website, fit_score, fit_reasons
    """
    if not records:
        return

    init_db()
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO ria_cache (crd, company, website, fit_score, fit_reasons, scored_at)
            VALUES (:crd, :company, :website, :fit_score, :fit_reasons, :scored_at)
            ON CONFLICT(crd) DO UPDATE SET
                company    = excluded.company,
                website    = excluded.website,
                fit_score  = excluded.fit_score,
                fit_reasons = excluded.fit_reasons,
                scored_at  = excluded.scored_at
            """,
            [{**r, 'scored_at': now} for r in records],
        )
        conn.commit()
    logger.info("Saved %d fit scores to cache", len(records))


def lookup_enrichments(crd_list):
    """Batch lookup cached enrichment data.

    Args:
        crd_list: list of CRD numbers (int)

    Returns:
        dict[int, dict] mapping CRD → contact fields dict.
        Only includes CRDs that have been enriched (enriched_at is not NULL).
    """
    if not crd_list:
        return {}

    init_db()
    result = {}
    with _connect() as conn:
        for i in range(0, len(crd_list), 500):
            chunk = crd_list[i:i + 500]
            placeholders = ','.join('?' * len(chunk))
            rows = conn.execute(
                f"SELECT crd, contact_name, contact_email, contact_title, "
                f"contact_phone, contact_linkedin FROM ria_cache "
                f"WHERE crd IN ({placeholders}) AND enriched_at IS NOT NULL",
                chunk,
            ).fetchall()
            for row in rows:
                result[row['crd']] = {
                    'contact_name': row['contact_name'],
                    'contact_email': row['contact_email'],
                    'contact_title': row['contact_title'],
                    'contact_phone': row['contact_phone'] or '',
                    'contact_linkedin': row['contact_linkedin'] or '',
                }
    return result


def save_enrichments(records):
    """Upsert enrichment data for a batch of RIAs.

    Args:
        records: list of dicts with keys: crd, contact_name, contact_email,
                 contact_title, contact_phone, contact_linkedin
    """
    if not records:
        return

    init_db()
    now = datetime.now(timezone.utc).isoformat()
    # Ensure new fields have defaults for backwards compatibility
    enriched = []
    for r in records:
        enriched.append({
            'contact_phone': '',
            'contact_linkedin': '',
            **r,
            'enriched_at': now,
        })
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO ria_cache (crd, contact_name, contact_email, contact_title,
                                   contact_phone, contact_linkedin, enriched_at)
            VALUES (:crd, :contact_name, :contact_email, :contact_title,
                    :contact_phone, :contact_linkedin, :enriched_at)
            ON CONFLICT(crd) DO UPDATE SET
                contact_name     = excluded.contact_name,
                contact_email    = excluded.contact_email,
                contact_title    = excluded.contact_title,
                contact_phone    = excluded.contact_phone,
                contact_linkedin = excluded.contact_linkedin,
                enriched_at      = excluded.enriched_at
            """,
            enriched,
        )
        conn.commit()
    logger.info("Saved %d enrichments to cache", len(records))

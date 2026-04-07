"""
Tennis Pipeline — Ingest Rankings
Downloads atp_rankings_20s.csv from Jeff Sackmann's GitHub
and inserts rankings into tennis.rankings.

- First run : inserts all current rankings up to today
- Weekly run: inserts only new rankings since last ingested date

Usage:
    python ingest_rankings.py
"""

import os
import io
import hashlib
import logging
import sys
from datetime import datetime

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from config import DB_CONFIG, SCHEMA

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
RANKINGS_URL = (
    "https://raw.githubusercontent.com/JeffSackmann/"
    "tennis_atp/master/atp_rankings_current.csv"
)
CSV_COLUMNS  = ["ranking_date", "rank", "atp_id", "ranking_points"]


# ── Helpers ───────────────────────────────────────────────────

def make_player_hash(atp_id: str) -> str:
    return hashlib.md5(str(atp_id).encode()).hexdigest()


def make_ranking_hash(ranking_date: str, atp_id: str) -> str:
    return hashlib.md5(f"{ranking_date}_{atp_id}".encode()).hexdigest()


def get_last_ranking_date(cur, schema: str):
    """Returns last ingested ranking date or None if DB is empty."""
    cur.execute(f"SELECT MAX(ranking_date) FROM {schema}.rankings")
    return cur.fetchone()[0]


def get_valid_player_hashes(cur, schema: str) -> set:
    cur.execute(f"SELECT player_hash_id FROM {schema}.players")
    return {row[0] for row in cur.fetchall()}


def download_rankings(url: str) -> pd.DataFrame:
    log.info("Downloading rankings CSV from GitHub...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.text),
        header=0,
        names=CSV_COLUMNS,
        dtype=str,
        index_col=False,
    )
    log.info(f"Downloaded {len(df):,} ranking records.")
    return df


def clean_rankings(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Parse ranking date — format is YYYYMMDD
    df["ranking_date"] = pd.to_datetime(
        df["ranking_date"], format="%Y%m%d", errors="coerce"
    ).dt.date

    # Numeric fields
    df["rank"]           = pd.to_numeric(df["rank"],           errors="coerce")
    df["ranking_points"] = pd.to_numeric(df["ranking_points"], errors="coerce")

    # Drop rows missing critical fields
    df = df.dropna(subset=["ranking_date", "rank", "atp_id"])
    df = df[df["atp_id"].str.strip() != ""]

    # Generate hash PKs
    df["ranking_hash_id"] = df.apply(
        lambda r: make_ranking_hash(str(r["ranking_date"]), str(r["atp_id"])),
        axis=1,
    )
    df["player_hash_id"] = df["atp_id"].apply(make_player_hash)

    return df


def insert_rankings(cur, schema: str, df: pd.DataFrame) -> int:
    rows = [
        (
            str(row["ranking_hash_id"]),
            row["ranking_date"],
            int(row["rank"]),
            str(row["player_hash_id"]),
            int(row["ranking_points"]) if pd.notna(row["ranking_points"]) else None,
        )
        for _, row in df.iterrows()
    ]

    execute_values(
        cur,
        f"""
        INSERT INTO {schema}.rankings
            (ranking_hash_id, ranking_date, rank, player_hash_id, ranking_points)
        VALUES %s
        ON CONFLICT (ranking_date, player_hash_id) DO NOTHING
        """,
        rows,
        page_size=1000,
    )
    return len(rows)


# ── Main ──────────────────────────────────────────────────────

def main():
    conn = None  # Initialize to None so conn always exists
    try:
        log.info("=" * 50)
        log.info("Ingest Rankings — starting")
        start = datetime.now()

        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # Check last ingested date
        last_date = get_last_ranking_date(cur, SCHEMA)

        if last_date is None:
            log.info("No rankings in DB — first run, ingesting full 2020s history.")
        else:
            log.info(f"Last ingested ranking date: {last_date} — ingesting new records only.")

        # Download
        df = download_rankings(RANKINGS_URL)

        # Clean
        df = clean_rankings(df)
        log.info(f"After cleaning: {len(df):,} valid ranking records.")

        # Filter — new records only if weekly, all if first run
        if last_date:
            df = df[df["ranking_date"] > last_date]
            log.info(f"New records since {last_date}: {len(df):,}")
        
        if df.empty:
            log.info("No new rankings to insert.")
            sys.exit(0)

        # FK safety — skip rankings with no matching player
        valid_hashes = get_valid_player_hashes(cur, SCHEMA)
        before = len(df)
        df = df[df["player_hash_id"].isin(valid_hashes)]
        skipped = before - len(df)
        if skipped:
            log.info(f"Skipped {skipped:,} rankings with no matching player.")
        log.info(f"Records to insert: {len(df):,}")

        # Chunked inserts with progress logging
        chunk_size  = 50_000
        total       = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i: i + chunk_size]
            total += insert_rankings(cur, SCHEMA, chunk)
            conn.commit()
            log.info(f"  Inserted {total:,} / {len(df):,} records...")

        log.info(f"Total inserted: {total:,} ranking records.")

        elapsed = int((datetime.now() - start).total_seconds())
        log.info(f"Ingest Rankings — complete ({elapsed}s)")
        log.info("=" * 50)

    except Exception as e:
        # Handle database-specific or general errors
        log.info("Error Occurred!", e)
        if conn:
            conn.rollback() # Rollback in case of transaction failure

    finally:
        # Ensure resources are closed
        if cur:
            cur.close()

        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()

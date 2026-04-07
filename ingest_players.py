"""
Tennis Pipeline — Step 2: Ingest Players
Downloads atp_players.csv from Jeff Sackmann's GitHub repo
and inserts new players into tennis.players.

- First run : inserts all players
- Weekly run: inserts only new players (by atp_id)

Usage:
    python ingest_players.py
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
PLAYERS_URL = (
    "https://raw.githubusercontent.com/JeffSackmann/"
    "tennis_atp/master/atp_players.csv"
)

CSV_COLUMNS = ["atp_id", "first_name", "last_name", "hand", "dob", "country_code", "height_cm", "wikidata_id"]


# ── Helpers ───────────────────────────────────────────────────

def make_player_hash(atp_id: str) -> str:
    """MD5 hash of atp_id — stable, deterministic PK."""
    return hashlib.md5(str(atp_id).encode()).hexdigest()


def download_players(url: str) -> pd.DataFrame:
    log.info("Downloading players CSV from GitHub...")
    response = requests.get(url, timeout=30)

    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.text),
        header=None,
        names=CSV_COLUMNS,
        dtype=str,
        skiprows=1,
        index_col=False,
    )
    log.info(f"Downloaded {len(df):,} player records.")
    return df


def clean_players(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Parse DOB — Sackmann format is YYYYMMDD
    df["date_of_birth"] = pd.to_datetime(
        df["dob"], format="%Y%m%d", errors="coerce"
    ).dt.date
    df.drop(columns=["dob"], inplace=True)

    # Height — numeric, nullable
    df["height_cm"] = pd.to_numeric(df["height_cm"], errors="coerce")

    # Hand — keep only R/L/U
    df["hand"] = df["hand"].where(df["hand"].isin(["R", "L", "U"]), other=None)

    # Truncate country_code to 3 chars           ← add this
    df["country_code"] = df["country_code"].str[:3]

    # Drop rows missing identity fields
    df = df.dropna(subset=["atp_id", "first_name", "last_name"])
    df = df[df["atp_id"].str.strip() != ""]

    # Drop unnecessary columns
    df.drop(columns=["wikidata_id"], inplace=True)

    # Generate hash PK
    df["player_hash_id"] = df["atp_id"].apply(make_player_hash)

    return df


def get_existing_atp_ids(cur, schema: str) -> set:
    cur.execute(f"SELECT atp_id FROM {schema}.players")
    return {row[0] for row in cur.fetchall()}


def insert_players(cur, schema: str, df: pd.DataFrame) -> int:
    rows = [
        (
            str(row["player_hash_id"]),
            str(row["atp_id"]),
            str(row["first_name"]),
            str(row["last_name"]),
            row["date_of_birth"] if pd.notna(row["date_of_birth"]) else None,
            str(row["country_code"])[:3] if pd.notna(row["country_code"]) else None,
            int(row["height_cm"]) if pd.notna(row["height_cm"]) else None,
        )
        for _, row in df.iterrows()   # iterrows gives named access, not positional
    ]

    execute_values(
        cur,
        f"""
        INSERT INTO {schema}.players
            (player_hash_id, atp_id, first_name, last_name,
             date_of_birth, country_code, height_cm)
        VALUES %s
        ON CONFLICT (atp_id) DO NOTHING
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
        log.info("Ingest Players — starting")
        start = datetime.now()

        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # 1. Download
        df = download_players(PLAYERS_URL)

        # 2. Clean
        df = clean_players(df)
        log.info(f"After cleaning: {len(df):,} valid player records.")

        existing = get_existing_atp_ids(cur, SCHEMA)
        if existing:
            log.info(f"DB has {len(existing):,} existing players.")
            df = df[~df["atp_id"].isin(existing)]
            log.info(f"New players to insert: {len(df):,}")
        else:
            log.info("DB empty — first run, inserting all players.")

        if df.empty:
            log.info("No new players to insert.")
        else:
            count = insert_players(cur, SCHEMA, df)
            conn.commit()
            log.info(f"Inserted {count:,} new players.")

        elapsed = int((datetime.now() - start).total_seconds())
        log.info(f"Ingest Players — complete ({elapsed}s)")
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

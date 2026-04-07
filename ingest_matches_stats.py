"""
Tennis Pipeline — Ingest Matches and Stats
Downloads atp_matches_<year>.csv from Jeff Sackmann's GitHub
and inserts matches into tennis.matches.

Insert stats into tennis.match_stats

- First run : inserts all matches data from configured start_year
- Weekly run: inserts only new data since last ingested date

Usage:
    python ingest_matches_stats.py
"""

import pandas as pd
import requests
import logging
import sys
import io
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import hashlib
from config import DB_CONFIG, SCHEMA

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
START_YEAR = 2020
GITHUB_BASE_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_"

# -----------------------------
# Columns
# -----------------------------
MATCH_COLUMNS = [
    "match_hash_id", "match_date", "tourney_name", "tourney_level", "surface",
    "draw_size", "round", "best_of", "winner_hash_id", "loser_hash_id",
    "score", "minutes", "winner_rank", "loser_rank"
]

MATCH_STATS_COLUMNS = [
    "match_hash_id", "player_hash_id", "result", "aces", "double_faults",
    "serve_points", "first_serves_in", "first_serve_points_won",
    "second_serve_points_won", "serve_games", "break_points_saved",
    "break_points_faced"
]

# -----------------------------
# Helper functions
# -----------------------------
def safe_int(val):
    try:
        if pd.isna(val):
            return None
        return int(val)
    except:
        return None

def safe_str(val):
    if pd.isna(val):
        return None
    return str(val).strip()

def safe_date(val):
    try:
        dt = pd.to_datetime(val, errors='coerce')
        return dt.date() if not pd.isna(dt) else None
    except:
        return None

def generate_match_hash(tourney_date, match_num):
    s = f"{tourney_date}_{match_num}"
    return hashlib.md5(s.encode()).hexdigest()

def generate_player_hash(player_id: str) -> str:
    return hashlib.md5(str(player_id).encode()).hexdigest()

def get_valid_player_hashes(cur, schema: str) -> set:
    cur.execute(f"SELECT player_hash_id FROM {schema}.players")
    return {row[0] for row in cur.fetchall()}

# ── Main ──────────────────────────────────────────────────────
def main():
    conn = None  # Initialize to None so conn always exists
    try:
        log.info("=" * 50)
        log.info("Ingest Matches and Stats — starting")
        start = datetime.now()

        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # -----------------------------
        # Determine start year
        # -----------------------------
        cur.execute(f"SELECT EXTRACT(YEAR FROM MAX(match_date)) FROM {SCHEMA}.matches;")
        result = cur.fetchone()
        db_max_year = int(result[0]) if result[0] is not None else None
        start_year = START_YEAR if db_max_year is None else db_max_year
        current_year = datetime.now().year
        log.info(f"Processing years from {start_year} to {current_year}")

        # -----------------------------
        # Loop through years
        # -----------------------------
        for year in range(start_year, current_year + 1):
            csv_url = f"{GITHUB_BASE_URL}{year}.csv"
            log.info(f"Downloading {csv_url}...")

            try:
                r = requests.get(csv_url)
                r.raise_for_status()
            except requests.RequestException as e:
                log.info(f"File for year {year} does not exist or error occurred. Skipping.")
                continue

            df = pd.read_csv(
                io.StringIO(r.text),
                header=0,   # first row as header
                dtype=str,  # read everything as string initially
                index_col=False
            )
            df.columns = df.columns.str.strip()  # remove leading/trailing spaces

            # Drop rows with missing tourney_date or match_num
            df = df.dropna(subset=['tourney_date', 'match_num'])
            if df.empty:
                log.info(f"No valid rows for {year}, skipping...")
                continue

            # Generate match_hash_id column
            df['match_hash_id'] = df.apply(
                lambda row: generate_match_hash(safe_date(row['tourney_date']), safe_str(row['match_num'])), axis=1
            )

            # -----------------------------
            # Apply player hash to dataframe
            # -----------------------------
            df['winner_hash_id'] = df['winner_id'].apply(generate_player_hash)
            df['loser_hash_id'] = df['loser_id'].apply(generate_player_hash)

            # FK safety — skip rankings with no matching player
            valid_hashes = get_valid_player_hashes(cur, SCHEMA)
            before = len(df)
            df = df[df["winner_hash_id"].isin(valid_hashes) & df["loser_hash_id"].isin(valid_hashes)]
            skipped = before - len(df)
            if skipped:
                log.info(f"Skipped {skipped:,} Matches with no matching player.")

            # -----------------------------
            # Deduplication for current year only
            # -----------------------------
            if db_max_year is not None and year >= db_max_year:
                # Matches table
                hash_list = tuple(df['match_hash_id'].unique())
                if hash_list:
                    cur.execute(
                        f"SELECT match_hash_id FROM {SCHEMA}.matches WHERE match_hash_id IN %s", (hash_list,)
                    )
                    existing_matches = {row[0] for row in cur.fetchall()}
                    initial_count = len(df)
                    df = df[~df['match_hash_id'].isin(existing_matches)]
                    removed_count = initial_count - len(df)
                    if removed_count > 0:
                        log.info(f"Skipped {removed_count} match rows for {year} already in DB")
                if df.empty:
                    log.info(f"No new matches for {year}. Skipping...")
                    continue

            # -----------------------------
            # Insert matches
            # -----------------------------
            match_tuples = [
                (
                    row['match_hash_id'],
                    safe_date(row['tourney_date']),
                    safe_str(row.get('tourney_name')),
                    safe_str(row.get('tourney_level')),
                    safe_str(row.get('surface')),
                    safe_int(row.get('draw_size')),
                    safe_str(row.get('round')),
                    safe_int(row.get('best_of')),
                    safe_str(row.get('winner_hash_id')),
                    safe_str(row.get('loser_hash_id')),
                    safe_str(row.get('score')),
                    safe_int(row.get('minutes')),
                    safe_int(row.get('winner_rank')),
                    safe_int(row.get('loser_rank'))
                )
                for _, row in df.iterrows()
            ]

            match_query = f"""
                INSERT INTO {SCHEMA}.matches ({', '.join(MATCH_COLUMNS)})
                VALUES %s
                ON CONFLICT (match_hash_id) DO NOTHING
            """
            execute_values(cur, match_query, match_tuples)
            conn.commit()
            log.info(f"Inserted {len(match_tuples)} matches for {year}")

            # -----------------------------
            # Deduplication for match_stats for current year only
            # -----------------------------
            if year == current_year:
                stats_candidate_rows = []
                for _, row in df.iterrows():
                    match_hash = row['match_hash_id']
                    stats_candidate_rows.append((match_hash, safe_str(row['winner_hash_id'])))
                    stats_candidate_rows.append((match_hash, safe_str(row['loser_hash_id'])))
                if stats_candidate_rows:
                    cur.execute(
                        f"""
                        SELECT match_hash_id, player_hash_id FROM {SCHEMA}.match_stats
                        WHERE (match_hash_id, player_hash_id) IN %s
                        """,
                        (tuple(stats_candidate_rows),)
                    )
                    existing_stats = set(cur.fetchall())
                else:
                    existing_stats = set()
            else:
                existing_stats = set()

            # -----------------------------
            # Build stats tuples
            # -----------------------------
            stats_tuples = []
            for _, row in df.iterrows():
                match_hash = row['match_hash_id']

                winner_row = (
                    match_hash,
                    safe_str(row['winner_hash_id']),
                    'W',
                    safe_int(row.get('w_ace')),
                    safe_int(row.get('w_df')),
                    safe_int(row.get('w_svpt')),
                    safe_int(row.get('w_1stIn')),
                    safe_int(row.get('w_1stWon')),
                    safe_int(row.get('w_2ndWon')),
                    safe_int(row.get('w_SvGms')),
                    safe_int(row.get('w_bpSaved')),
                    safe_int(row.get('w_bpFaced'))
                )
                if (winner_row[0], winner_row[1]) not in existing_stats:
                    stats_tuples.append(winner_row)

                loser_row = (
                    match_hash,
                    safe_str(row['loser_hash_id']),
                    'L',
                    safe_int(row.get('l_ace')),
                    safe_int(row.get('l_df')),
                    safe_int(row.get('l_svpt')),
                    safe_int(row.get('l_1stIn')),
                    safe_int(row.get('l_1stWon')),
                    safe_int(row.get('l_2ndWon')),
                    safe_int(row.get('l_SvGms')),
                    safe_int(row.get('l_bpSaved')),
                    safe_int(row.get('l_bpFaced'))
                )
                if (loser_row[0], loser_row[1]) not in existing_stats:
                    stats_tuples.append(loser_row)

            if stats_tuples:
                stats_query = f"""
                    INSERT INTO {SCHEMA}.match_stats ({', '.join(MATCH_STATS_COLUMNS)})
                    VALUES %s
                    ON CONFLICT (match_hash_id, player_hash_id) DO NOTHING
                """
                execute_values(cur, stats_query, stats_tuples)
                conn.commit()
                log.info(f"Inserted {len(stats_tuples)} match stats rows for {year}")
            else:
                log.info(f"No new match_stats to insert for {year}")

        elapsed = int((datetime.now() - start).total_seconds())
        log.info(f"Ingest Matches and Stats — complete ({elapsed}s)")
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
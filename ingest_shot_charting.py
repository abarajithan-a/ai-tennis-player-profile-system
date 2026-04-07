"""
Tennis Pipeline — Ingest Shot Charting
Downloads charting-m-stats-Overview.csv and 
charting-m-stats-NetPoints.csv from Jeff Sackmann's GitHub
and Insert data into tennis.shot_charting

- First run : inserts all shots data.
- Weekly run: inserts only new data.

Usage:
    python ingest_shot_charting.py
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
OVERVIEW_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_MatchChartingProject/master/charting-m-stats-Overview.csv"
NET_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_MatchChartingProject/master/charting-m-stats-NetPoints.csv"

# -----------------------------
# Columns
# -----------------------------
OVERVIEW_CSV_COLUMNS = [
    'match_id', 'player', 'set', 'serve_pts', 'aces', 'dfs', 
    'first_in', 'first_won', 'second_in', 'second_won', 
    'bk_pts', 'bp_saved', 'return_pts', 'return_pts_won', 
    'winners', 'winners_fh', 'winners_bh', 'unforced', 
    'unforced_fh', 'unforced_bh'
]

NET_CSV_COLUMNS = [
    'match_id', 'player', 'row', 'net_pts', 'pts_won', 
    'net_winner', 'induced_forced', 'net_unforced', 
    'passed_at_net', 'passing_shot_induced_forced', 'total_shots'
]

SHOT_CHARTING_DB_COLUMNS = [
    'shot_charting_hash_id', 'match_date', 'player_name', 
    'aces', 'double_faults', 'serve_points',
    'first_serves_in', 'first_serve_points_won', 'second_serve_points_won',
    'break_points_faced', 'break_points_saved',
    'return_points', 'return_points_won', 'forehand_winners',
    'backhand_winners', 'forehand_errors', 'backhand_errors',
    'net_points_total', 'net_points_won', 'net_winners', 'net_errors'
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

def generate_shot_chart_hash(match_date: str, player: str) -> str:
    return hashlib.md5(f"{match_date}_{player}".encode()).hexdigest()

def get_last_shot_chart_date(cur, schema: str):
    """Returns last ingested shot chart date or None if DB is empty."""
    cur.execute(f"SELECT MAX(match_date) FROM {schema}.shot_charting")
    return cur.fetchone()[0]

def download_shots(url: str, csv_columns: str) -> pd.DataFrame:
    log.info("Downloading shots CSV from GitHub...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.text),
        header=0,
        names=csv_columns,
        dtype=str,
        index_col=False,
    )
    log.info(f"Downloaded {len(df):,} shot records.")
    return df

def clean_shots_overview(df: pd.DataFrame) -> pd.DataFrame:
    # Numeric fields
    df["serve_pts"]      = pd.to_numeric(df["serve_pts"],      errors="coerce")
    df["aces"]           = pd.to_numeric(df["aces"],           errors="coerce")
    df["dfs"]            = pd.to_numeric(df["dfs"],            errors="coerce")
    df["first_in"]       = pd.to_numeric(df["first_in"],       errors="coerce")
    df["first_won"]      = pd.to_numeric(df["first_won"],      errors="coerce")
    df["second_in"]      = pd.to_numeric(df["second_in"],      errors="coerce")
    df["second_won"]     = pd.to_numeric(df["second_won"],     errors="coerce")
    df["bk_pts"]         = pd.to_numeric(df["bk_pts"],         errors="coerce")
    df["bp_saved"]       = pd.to_numeric(df["bp_saved"],       errors="coerce")
    df["return_pts"]     = pd.to_numeric(df["return_pts"],     errors="coerce")
    df["return_pts_won"] = pd.to_numeric(df["return_pts_won"], errors="coerce")
    df["winners"]        = pd.to_numeric(df["winners"],        errors="coerce")
    df["winners_fh"]     = pd.to_numeric(df["winners_fh"],     errors="coerce")
    df["winners_bh"]     = pd.to_numeric(df["winners_bh"],     errors="coerce")
    df["unforced"]       = pd.to_numeric(df["unforced"],       errors="coerce")
    df["unforced_fh"]    = pd.to_numeric(df["unforced_fh"],    errors="coerce")
    df["unforced_bh"]    = pd.to_numeric(df["unforced_bh"],    errors="coerce")

    # Drop rows missing critical fields
    df = df.dropna(subset=["match_date", "player"])
    df = df[df["player"].str.strip() != ""]

    # Generate hash PKs
    df["shot_charting_hash_id"] = df.apply(
        lambda r: generate_shot_chart_hash(str(r["match_date"]), str(r["player"]).upper()),
        axis=1,
    )

    return df

def clean_net_shots(df: pd.DataFrame) -> pd.DataFrame:
    # Numeric fields
    df["net_pts"]                    = pd.to_numeric(df["net_pts"],                    errors="coerce")
    df["pts_won"]                    = pd.to_numeric(df["pts_won"],                    errors="coerce")
    df["net_winner"]                 = pd.to_numeric(df["net_winner"],                 errors="coerce")
    df["induced_forced"]             = pd.to_numeric(df["induced_forced"],             errors="coerce")
    df["net_unforced"]               = pd.to_numeric(df["net_unforced"],               errors="coerce")
    df["passed_at_net"]              = pd.to_numeric(df["passed_at_net"],              errors="coerce")
    df["passing_shot_induced_forced"] = pd.to_numeric(df["passing_shot_induced_forced"], errors="coerce")
    df["total_shots"]                = pd.to_numeric(df["total_shots"],                errors="coerce")

    # Drop rows missing critical fields
    df = df.dropna(subset=["match_date", "player"])
    df = df[df["player"].str.strip() != ""]

    # Generate hash PKs
    df["shot_charting_hash_id"] = df.apply(
        lambda r: generate_shot_chart_hash(str(r["match_date"]), str(r["player"]).upper()),
        axis=1,
    )

    return df

def insert_shot_charting(cur, schema: str, df: pd.DataFrame) -> int:
    shot_tuples = [
        (
            row['shot_charting_hash_id'],
            safe_date(row['match_date']),
            safe_str(row.get('player')),
            safe_int(row.get('aces')),
            safe_int(row.get('dfs')),
            safe_int(row.get('serve_pts')),
            safe_int(row.get('first_in')),
            safe_int(row.get('first_won')),
            safe_int(row.get('second_won')),
            safe_int(row.get('bk_pts')),
            safe_int(row.get('bp_saved')),
            safe_int(row.get('return_pts')),
            safe_int(row.get('return_pts_won')),
            safe_int(row.get('winners_fh')),
            safe_int(row.get('unforced_fh')),
            safe_int(row.get('winners_bh')),
            safe_int(row.get('unforced_bh')),
            safe_int(row.get('net_pts')),
            safe_int(row.get('pts_won')),
            safe_int(row.get('net_winner')),
            safe_int(row.get('net_unforced'))
        )
        for _, row in df.iterrows()
    ]

    shot_query = f"""
        INSERT INTO {SCHEMA}.shot_charting ({', '.join(SHOT_CHARTING_DB_COLUMNS)})
        VALUES %s
        ON CONFLICT (shot_charting_hash_id) DO NOTHING
    """
    execute_values(cur, shot_query, shot_tuples)

    return len(shot_tuples)

# ── Main ──────────────────────────────────────────────────────
def main():
    conn = None  # Initialize to None so conn always exists
    try:
        log.info("=" * 50)
        log.info("Ingest Shot charting — starting")
        start = datetime.now()

        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # Check last ingested date
        last_date = get_last_shot_chart_date(cur, SCHEMA)

        if last_date is None:
            log.info("No Shot charting data in DB — first run, ingesting all data.")
        else:
            log.info(f"Last ingested shot chart date: {last_date} — ingesting new records only.")

        # Download CSVs
        odf = download_shots(OVERVIEW_URL, OVERVIEW_CSV_COLUMNS)
        #odf = pd.read_csv(r'C:\Users\Abar\Downloads\charting-m-stats-Overview.csv', header=0, index_col=False)
        # Strip whitespace
        odf = odf.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        odf['match_date'] = pd.to_datetime(odf['match_id'].str.split('-').str[0], format='%Y%m%d')
        odf = odf[odf['set'].str.strip().str.upper() == 'TOTAL']

        ndf = download_shots(NET_URL, NET_CSV_COLUMNS)
        #ndf = pd.read_csv(r'C:\Users\Abar\Downloads\charting-m-stats-NetPoints.csv', header=0, index_col=False)
        # Strip whitespace
        ndf = ndf.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        ndf['match_date'] = pd.to_datetime(ndf['match_id'].str.split('-').str[0], format='%Y%m%d')
        ndf = ndf[ndf['row'].str.strip().str.upper() == 'NETPOINTS']

        # Filter — new records only if weekly, all if first run
        if last_date:
            last_date = pd.to_datetime(last_date)
            odf = odf[odf["match_date"] >= last_date]
            log.info(f"New Shot overview records since {last_date}: {len(odf):,}")

            ndf = ndf[ndf["match_date"] >= last_date]
            log.info(f"New Net Shots records since {last_date}: {len(ndf):,}")
        
        if odf.empty and ndf.empty:
            log.info("No new shot charting data to insert.")
            sys.exit(0)

        # Clean
        odf = clean_shots_overview(odf)
        log.info(f"After cleaning: {len(odf):,} valid shots overview records.")

        ndf = clean_net_shots(ndf)
        log.info(f"After cleaning: {len(ndf):,} valid net shots records.")

        # Merge the datasets on match date and player
        merged_df = pd.merge(odf,ndf, on=['shot_charting_hash_id'], how='outer')

        # Find all columns that were duplicated (ending in _x)
        cols_to_fix = [c[:-2] for c in merged_df.columns if c.endswith('_x')]

        #COALESCE identical columns
        for col in cols_to_fix:
            # Coalesce: take value from odf (_x), if NaN take from ndf (_y)
            merged_df[col] = merged_df[f'{col}_x'].combine_first(merged_df[f'{col}_y'])
            # Drop the temporary suffixed columns
            merged_df.drop([f'{col}_x', f'{col}_y'], axis=1, inplace=True)

        log.info(f"After merging: {len(merged_df):,} valid records.")

        # Chunked inserts with progress logging
        chunk_size  = 50_000
        total       = 0
        for i in range(0, len(merged_df), chunk_size):
            chunk = merged_df.iloc[i: i + chunk_size]
            total += insert_shot_charting(cur, SCHEMA, chunk)
            conn.commit()
            log.info(f"  Inserted {total:,} / {len(merged_df):,} records...")

        log.info(f"Total inserted: {total:,} Shot Chart records.")           

        elapsed = int((datetime.now() - start).total_seconds())
        log.info(f"Ingest Shot Charting — complete ({elapsed}s)")
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
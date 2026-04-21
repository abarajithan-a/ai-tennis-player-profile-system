import os
import psycopg2
import sys
import logging
from datetime import datetime
from psycopg2 import sql
from pathlib import Path
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
GRANTEE     = os.getenv("DB_GRANTEE", "postgres")   # change to app user if needed
SQL_FILE    = Path(__file__).parent / "tennis_schema.sql"

# ── Helpers ───────────────────────────────────────────────────

def db_exists(config) -> bool:
    conn = None  # Initialize to None so conn always exists
    cur = None
    """Check if database exists by connecting to the default 'postgres' db."""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname="postgres"
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config["dbname"],))
            return cur.fetchone() is not None

    except Exception as e:
        # Handle database-specific or general errors
        log.info("Error while connecting to PostgreSQL", e)

    finally:
        # Ensure resources are closed
        if cur:
            cur.close()

        if conn is not None:
            conn.close()


def create_db(config) -> None:
    """Create database — must run outside a transaction block."""
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        dbname="postgres"
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config["dbname"])))
    conn.close()


def schema_exists(cur, schema: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (schema,),
    )
    return cur.fetchone() is not None


def load_sql(path: Path, schema: str) -> str:
    return path.read_text(encoding="utf-8").format(schema=schema)


# ── Main ──────────────────────────────────────────────────────

def setup():

    # 1. Database
    if not db_exists(DB_CONFIG):
        create_db(DB_CONFIG)
        log.info(f"[+] Database '{DB_CONFIG['dbname']}' created.")
    else:
        log.info(f"[=] Database '{DB_CONFIG['dbname']}' already exists.")

    conn = None  # Initialize to None so conn always exists
    try:
        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

    # 2. Schema + tables (connect to target DB now)
        if not schema_exists(cur, SCHEMA):
            cur.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(SCHEMA))
            )
            log.info(f"[+] Schema '{SCHEMA}' created.")
        else:
            log.info(f"[=] Schema '{SCHEMA}' already exists.")

        # Grants
        for stmt in (
            sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {} TO {}"),
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON TABLES TO {}"),
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON SEQUENCES TO {}"),
        ):
            cur.execute(stmt.format(
                sql.Identifier(SCHEMA),
                sql.Identifier(GRANTEE)
            ))
        log.info(f"[+] Privileges granted to '{GRANTEE}'.")

        # Tables + indexes from SQL file
        cur.execute(load_sql(SQL_FILE, SCHEMA))
        log.info(f"[+] Schema SQL executed — all tables and indexes in place.")

        conn.commit()

        log.info("✓ Tennis DB initialization complete.")
        log.info(f"  Host     : {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        log.info(f"  Database : {DB_CONFIG['dbname']}")
        log.info(f"  Schema   : {SCHEMA}")
        log.info(f" Tables created:")
        tables = [
            "players", "rankings", "matches",
            "match_stats", "shot_charting", "player_profiles"
        ]
        for t in tables:
            log.info(f"  └── {SCHEMA}.{t}")

    except Exception as e:
        # Handle database-specific or general errors
        log.info("Error Occurred!", e)

    finally:
        # Ensure resources are closed
        if cur:
            cur.close()

        if conn is not None:
            conn.close()

if __name__ == "__main__":
    setup()

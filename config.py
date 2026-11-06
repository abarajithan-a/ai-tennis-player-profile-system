from dotenv import load_dotenv
import os

load_dotenv()

# ── Config ────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_NAME", "tennis"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

SCHEMA = os.getenv("POSTGRES_SCHEMA", "tennis")
GOOGLE_AI_API_KEY = os.getenv("GOOGLE_AI_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# Data Source Repository Details (Where the tennis CSVs are fetched from)
DS_GITHUB_USERNAME = "abarajithan-a"
DS_REPO_NAME = "ai-tennis-player-profile-system"
DS_BRANCH = "main"

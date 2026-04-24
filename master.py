"""
Tennis Pipeline — Master Orchestrator
Runs all pipeline steps in sequence.
Stops immediately on first failure.

Usage:
    python master.py
"""

import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Pipeline steps (in order) ─────────────────────────────────
BASE = Path(__file__).parent

STEPS = [
    {"name": "DB Initialization", "script": BASE / "db_init.py"},
    {"name": "DB Ingest Players", "script": BASE / "ingest_players.py"},
    {"name": "DB Ingest Player Rankings", "script": BASE / "ingest_rankings.py"},
    {"name": "DB Ingest Matches and Stats", "script": BASE / "ingest_matches_stats.py"},
    {"name": "DB Ingest Shot Chart", "script": BASE / "ingest_shot_charting.py"},
    {"name": "Invoke LLM", "script": BASE / "invoke_llm.py"},
]


# ── Runner ────────────────────────────────────────────────────


def run_step(step: dict) -> None:
    """Run a single pipeline step. Raises on failure."""
    script = step["script"]
    name = step["name"]

    if not script.exists():
        raise FileNotFoundError(f"Script not found: {script}")

    log.info(f"Starting step: {name}")
    start = datetime.now()

    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=False,  # stream stdout/stderr directly to console
        text=True,
    )

    elapsed = int((datetime.now() - start).total_seconds())
    if result.returncode != 0:
        raise RuntimeError(f"Step '{name}' failed with exit code {result.returncode}")

    log.info(f"Completed step: {name} ({elapsed}s)")
    log.info("#" * 50)


# ── Main ──────────────────────────────────────────────────────


def main():
    total = len(STEPS)
    log.info("=" * 60)
    log.info(f"Tennis AI Profiling Pipeline starting — {total} steps")
    log.info(f"Log file: {log_file}")
    log.info("=" * 60)

    pipeline_start = datetime.now()

    for i, step in enumerate(STEPS, start=1):
        log.info(f"[{i}/{total}] {step['name']}")
        try:
            run_step(step)
        except (FileNotFoundError, RuntimeError) as e:
            log.error(f"Pipeline stopped at step {i}/{total} — {e}")
            sys.exit(1)

    elapsed = int((datetime.now() - pipeline_start).total_seconds())
    log.info("=" * 60)
    log.info(f"Pipeline complete — all {total} steps succeeded ({elapsed}s)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

"""
Tennis Player Scoring Pipeline
- Fetches weekly summarized data for top N players from Postgres
- Scores players using LLaMA 3.1 8B (via Ollama) and Google Gemini Flash API
- Validates outputs with Pydantic, cleans with Polars, inserts into player_profiles table in DB
"""

import gc
import json
import hashlib
import logging
import sys
import time
import ollama
import polars as pl
import psycopg2
from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError
from datetime import date, datetime
from pathlib import Path
from psycopg2 import sql, extras
from pydantic import BaseModel, Field, ValidationError
from config import DB_CONFIG, SCHEMA, GOOGLE_AI_API_KEY

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SQL_FILE = "llm_input.sql"
LLM_MODELS = {
    "llama": {
        "name": "llama3.1:8b",
        "batch_size": 10,
        "options": {
            "temperature": 0.1,
            "num_ctx": 8192,      # input: prompt + all player data
            "num_predict": 3072   # output: JSON array for batch size
        }
    },
    "gemini": {
        "name": "gemini-3.1-flash-lite-preview", #"gemini-3-flash-preview",
        "batch_size": 10,
        "options": {        # Maps to generation config in Google Gemini API
            "temperature": 0.1,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 8192,
            "response_mime_type": "application/json",  # Forces JSON response            
        },
        "safety_settings" : {
            "HARM_CATEGORY_HARASSMENT":        "BLOCK_NONE",
            "HARM_CATEGORY_HATE_SPEECH":       "BLOCK_NONE",
            "HARM_CATEGORY_SEXUALLY_EXPLICIT": "BLOCK_NONE",
            "HARM_CATEGORY_DANGEROUS_CONTENT": "BLOCK_NONE",
        }
    }
}
TOP_N_PLAYERS = 100
# -----------------------------
# API Settings
# -----------------------------

GOOGLE_RPM = 10 # Requests per minute - Rate Limit
MAX_RETRIES = 3

# -----------------------------
# DB Columns
# -----------------------------
SCORE_COLUMNS = [
    "serve_quality_score", "return_quality_score", "forehand_quality_score", 
    "backhand_quality_score", "net_play_effectiveness_score", "clutch_score", 
    "efficiency_score", "overall_score",
]

REASONING_COLUMNS = ["overall_reasoning", "what_went_well", "areas_to_improve"]

PLAYER_PROFILE_COLUMNS = [
    "player_profile_hash_id", "player_hash_id", "match_week_monday", "llm_model"
] + SCORE_COLUMNS + REASONING_COLUMNS + ["generated_at"]

# ---------------------------------------------------------------------------
# Pydantic schema
# ---------------------------------------------------------------------------
class PlayerScore(BaseModel):
    player_hash_id: str
    player_name: str
    match_week_monday: date
    serve_quality_score: float = Field(..., ge=0.0, le=10.0)
    return_quality_score: float = Field(..., ge=0.0, le=10.0)
    forehand_quality_score: float = Field(..., ge=0.0, le=10.0)
    backhand_quality_score: float = Field(..., ge=0.0, le=10.0)
    net_play_effectiveness_score: float = Field(..., ge=0.0, le=10.0)
    clutch_score: float = Field(..., ge=0.0, le=10.0)
    efficiency_score: float = Field(..., ge=0.0, le=10.0)
    overall_score: float = Field(..., ge=0.0, le=10.0)
    overall_reasoning: str
    what_went_well: str
    areas_to_improve: str

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def fetch_top_players_weekly_data(cur, schema: str) -> list[dict]:
    """Fetch one week of aggregated match stats for the top N ranked players."""
    query_template = Path(SQL_FILE).read_text()

    # Inject schema (identifier)
    query = query_template.format(
        schema=sql.Identifier(schema).as_string(cur)
    )

    # Filter Top N players
    cur.execute(query, {"top_n_players": TOP_N_PLAYERS})
    rows = [dict(r) for r in cur.fetchall()]
    log.info("Fetched %d player records.", len(rows))
    return rows

def insert_player_profiles(
    cur, 
    records: list[dict], 
    model_label: str
) -> None:
    """Upsert scored player profiles into player_profile table."""
    if not records:
        log.warning("No records to insert for model=%s", model_label)
        return

    # Build column list and placeholders dynamically
    columns_str = ", ".join(PLAYER_PROFILE_COLUMNS)
    placeholders = ", ".join([f"%({col})s" for col in PLAYER_PROFILE_COLUMNS])

    upsert_sql = f"""
    INSERT INTO {SCHEMA}.player_profiles ({columns_str})
    VALUES ({placeholders})
    ON CONFLICT (player_profile_hash_id)
    DO UPDATE SET
        serve_quality_score             = EXCLUDED.serve_quality_score,
        return_quality_score            = EXCLUDED.return_quality_score,
        forehand_quality_score          = EXCLUDED.forehand_quality_score,
        backhand_quality_score          = EXCLUDED.backhand_quality_score,
        net_play_effectiveness_score    = EXCLUDED.net_play_effectiveness_score,
        clutch_score                    = EXCLUDED.clutch_score,
        efficiency_score                = EXCLUDED.efficiency_score,
        overall_score                   = EXCLUDED.overall_score,
        overall_reasoning               = EXCLUDED.overall_reasoning,
        what_went_well                  = EXCLUDED.what_went_well,
        areas_to_improve                = EXCLUDED.areas_to_improve,
        generated_at                    = EXCLUDED.generated_at
    """

    extras.execute_batch(cur, upsert_sql, records, page_size=50)

# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = "You are a tennis analytics engine. Return only valid JSON arrays."
SCORING_INSTRUCTIONS = """
You are an expert tennis analytics engine. You will receive a batch of tennis players
with their match statistics for the current week and trailing 4-week averages (prefixed w4_).
NULL values in w4_ fields indicate no prior data available for that player.

Score each player on the following dimensions using a 0.0 to 10.0 scale:

SCORING DIMENSIONS AND THEIR PRIMARY DRIVERS:

forehand_quality_score:
  - Primary: forehand_winners, forehand_errors
  - Context: winners_to_errors_ratio(forehand only), w4_forehand_winners, w4_forehand_errors

backhand_quality_score:
  - Primary: backhand_winners, backhand_errors
  - Context: winners_to_errors_ratio(backhand only), w4_backhand_winners, w4_backhand_errors

serve_quality_score:
  - Primary: first_serve_in_percent, aces, double_faults
  - Context: w4_first_serve_in_percent, w4_aces, w4_double_faults

return_quality_score:
  - Primary: return_points_won_percent
  - Context: w4_return_points_won_percent, break_points_saved_percent

net_play_effectiveness_score:
  - Primary: net_win_percent, net_winners, net_errors
  - Context: w4_net_win_percent, w4_net_winners, w4_net_errors

clutch_score:
  - Primary: break_points_saved_percent, break_points_saved, break_points_faced
  - Context: w4_break_points_saved_percent
  - A player who saves a high percentage of break points under pressure scores higher
  - Penalize players with very few break points faced (small sample)

efficiency_score:
  - Primary: winners_to_errors_ratio, first_serve_in_percent, 
  - Supporting: net_win_percent, return_points_won_percent, aces, double_faults
  - Reflects how cleanly and effectively a player wins points across all dimensions

overall_score:
  - Holistic rating across all dimensions
  - Is NOT a simple average
  - Weight serve_quality and return_quality more heavily as they drive most points
  - Factor in consistency between current week and w4_ trailing metrics
  - A player performing significantly above or below their w4_ baseline should
    have this reflected in the overall_score

GENERAL SCORING GUIDELINES:
  - Compare (w1_)current week metrics against w4_ trailing metrics for context.
  - Heavily penalize high double_faults regardless of other metrics
  - A high winners_to_errors_ratio is the single strongest positive signal
  - Small match sample (matches_played = 1) should moderate extreme scores
  - If all w4_ fields are NULL, score purely on current week data

REASONING GUIDELINES:
  - what_went_well must reference the single strongest metric or pattern this week
  - areas_to_improve must reference the single weakest metric or largest drop vs trailing average
  - Both fields must be specific to the data, never generic statements
  - Do not repeat the player name inside reasoning fields
  - Maximum 20 words per reasoning field, hard limit

IMPORTANT INSTRUCTIONS:
  - Return ONLY a valid JSON array
  - No markdown fences, no commentary, no explanation outside the JSON
  - Every numeric score must be a float between 0.0 and 10.0
  - Do not add any keys beyond those listed below
  - Each element must follow this exact shape:

{
  "player_hash_id": "<string>",
  "player_name": "<string>",
  "match_week_monday": "<date>",
  "serve_quality_score": <float>,
  "return_quality_score": <float>,
  "forehand_quality_score": <float>,
  "backhand_quality_score": <float>,
  "net_play_effectiveness_score": <float>,
  "clutch_score": <float>,
  "efficiency_score": <float>,
  "overall_score": <float>,
  "overall_reasoning": "<one concise sentence explaining the overall score, max 20 words>",
  "what_went_well": "<max 20 words, specific to this week's strongest metric or pattern>",
  "areas_to_improve": "<max 20 words, specific to this week's weakest metric or largest drop vs trailing average>"
}
"""

def build_prompt(player_batch: list[dict]) -> str:
    player_json = json.dumps(player_batch, indent=2, default=str)
    return (
        f"{SCORING_INSTRUCTIONS}\n\n"
        f"Player data:\n{player_json}\n\n"
        "Return ONLY the JSON array. No extra text."
    )

# ---------------------------------------------------------------------------
# Ollama inference
# ---------------------------------------------------------------------------
def check_ollama_model(model_name: str) -> None:
    """Verify the model is available locally; raise clearly if not."""
    try:
        available = [m.model for m in ollama.list().models]
        if model_name not in available:
            raise RuntimeError(
                f"Model '{model_name}' not found in Ollama.\n"
                f"Run:  ollama pull {model_name}\n"
                f"Available models: {available}"
            )
        log.info("Ollama model verified: %s", model_name)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Could not connect to Ollama. Is 'ollama serve' running? Error: {exc}"
        ) from exc

def invoke_ollama_chat(
    model_name: str, 
    model_options: dict,
    prompt: str, 
    batch_index: int
) -> str:
    """
    Send a prompt to Ollama via ollama.chat and return the response text.
    Ollama keeps the model warm in memory across consecutive calls.
    """
    log.info("Sending batch %d to Ollama model=%s...", batch_index, model_name)
    response = ollama.chat(
        model=model_name,
        messages=[
            {
                "role": "system",
                "content": SYSTEM_INSTRUCTION,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        options=model_options,
    )
    raw = response["message"]["content"]
    log.info("Batch %d response received — %d characters.", batch_index, len(raw))
    return raw


def unload_ollama_model(model_name: str) -> None:
    """
    Evict a model from Ollama's memory by sending a keep_alive=0 request.
    Call this between LLaMA and Mistral to free RAM on a 16GB machine.
    """
    try:
        ollama.chat(
            model=model_name,
            messages=[{"role": "user", "content": ""}],
            options={"num_predict": 1},
            keep_alive=0,
        )
        log.info("Ollama model evicted from memory: %s", model_name)
    except Exception as exc:
        log.warning("Could not evict Ollama model %s: %s", model_name, exc)
    finally:
        gc.collect()


# ---------------------------------------------------------------------------
# Parsing & Pydantic validation
# ---------------------------------------------------------------------------
def extract_json_array(raw: str, batch_index: int) -> list[dict]:
    """Extract the first JSON array from raw LLM output."""
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1:
        raise ValueError(
            f"Batch {batch_index}: No JSON array found in output. "
            f"Preview: {raw[:300]}"
        )
    return json.loads(raw[start: end + 1])


def validate_batch(raw_list: list[dict], batch_index: int) -> list[dict]:
    """Validate each item with Pydantic. Log and skip invalid records."""
    valid = []
    for item in raw_list:
        try:
            score = PlayerScore(**item)
            valid.append(score.model_dump())
        except ValidationError as exc:
            log.warning(
                "Validation failed — player_hash_id=%s batch=%d error=%s",
                item.get("player_hash_id", "?"),
                batch_index,
                exc,
            )
    log.info(
        "Batch %d validation: %d/%d records passed.",
        batch_index, len(valid), len(raw_list),
    )
    return valid


# ---------------------------------------------------------------------------
# Polars cleanup
# ---------------------------------------------------------------------------
def clean_with_polars(records: list[dict], model_label: str) -> list[dict]:
    """
    Load validated records into Polars, apply cleanup, return list of dicts
    ready for DB insertion.
    Steps:
      - Cast score columns to Float64
      - Clamp all scores to [0.0, 10.0] and round to 2dp
      - Truncate reasoning to 500 characters
      - Sort by overall_score desc before dedup (keep highest score per player)
      - Drop duplicate player_hash_ids (keep first occurrence)
      - Drop rows where overall_score == 0 (likely bad generations)
      - Add llm_model and generated_at columns
      - Compute md5_hash from player_hash_id + match_week_monday + llm_model
      - Sort by player_hash_id
    """
    if not records:
        log.warning("clean_with_polars: no records to clean for model=%s", model_label)
        return []

    log.info("Cleaning %d records with Polars for model=%s...", len(records), model_label)

    generated_at = datetime.now()

    df = pl.DataFrame(records)
    df = (
        df
        # Ensure date column is typed correctly before any operations
        .with_columns(pl.col("match_week_monday").cast(pl.Date))
        # Cast, clamp and round all score columns
        .with_columns([
            pl.col(c).cast(pl.Float64).clip(0.0, 10.0).round(2).alias(c)
            for c in SCORE_COLUMNS
        ])
        # Truncate reasoning
        .with_columns([
            pl.col(c).str.slice(0, 500).alias(c)
            for c in REASONING_COLUMNS
        ])
        # Sort before dedup so we deterministically keep the highest overall_score
        .sort("overall_score", descending=True)
        .unique(subset=["player_hash_id"], keep="first")
        # Drop likely bad generations
        .filter(pl.col("overall_score") > 0.0)
        # Add metadata columns
        .with_columns(pl.lit(model_label).alias("llm_model"))
        .with_columns(pl.lit(generated_at).alias("generated_at"))
        # Compute md5 hash — build the raw string natively in Polars,
        # then apply hashlib only on the single concatenated string column
        .with_columns(
            pl.concat_str([
                pl.col("player_hash_id"),
                pl.lit("_"),
                pl.col("match_week_monday").cast(pl.String),
                pl.lit("_"),
                pl.col("llm_model"),
            ])
            .map_elements(lambda s: hashlib.md5(s.encode()).hexdigest(), return_dtype=pl.String)
            .alias("player_profile_hash_id")
        )
        .sort("player_profile_hash_id")
    )

    log.info("Polars cleanup done — %d rows remaining.", df.height)
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------
def make_batches(players: list[dict], batch_size: int) -> list[list[dict]]:
    return [players[i: i + batch_size] for i in range(0, len(players), batch_size)]


def invoke_api_with_retry(client, model_name, prompt, model_options):
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=model_options,
            )

            return response

        except ClientError as e:
            code = getattr(e, "code", None) \
                    or getattr(e, "status_code", None) \
                    or getattr(e, "response", {}).get("status")

            if code == 429:  # Rate limit / quota exceeded
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt * 10
                print(f"[429] Rate limited. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)

            elif code == 400:  # Bad request — not retryable
                log.info(f"[400] Bad request (check your prompt/parameters): {e}")
                raise

            elif code == 401 or code == 403:  # Auth errors — not retryable
                log.info(f"[{code}] Auth error (check your API key/permissions): {e}")
                raise

            elif code == 404:  # Model not found — not retryable
                log.info(f"[404] Model not found (check model name): {e}")
                raise

            else:
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt * 5
                log.info(f"[{code}] Client error. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)

        except ServerError as e:
            code = getattr(e, "code", None) \
                    or getattr(e, "status_code", None) \
                    or getattr(e, "response", {}).get("status")

            if code == 503:  # High demand / unavailable — retryable
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt * 5  # 5s, 10s, 20s
                log.info(f"[503] Model unavailable (high demand). Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)

            elif code == 500:  # Internal server error — retryable
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt * 5
                log.info(f"[500] Internal server error. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)

            else:
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt * 5
                log.info(f"[{code}] Server error. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)

        except Exception as e:  # Unexpected errors (network, timeout, etc.)
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt * 5
            log.info(f"[UNKNOWN] Unexpected error: {e}. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)


def run_model_pipeline(
    model_key: str,
    model_name: str,
    model_options: dict,
    safety_settings: dict,
    batches: list[list[dict]],
) -> list[dict]:
    """
    Run all batches through llm models.
    Ollama keeps the model warm across calls — no manual load needed.
    Returns all validated records across all batches.
    """
    log.info("=== Starting %s (%s) — %d batches ===", model_key, model_name, len(batches))
    all_records: list[dict] = []

    if model_key == "llama":
        check_ollama_model(model_name)
    elif model_key == "gemini":
        client = genai.Client(api_key=GOOGLE_AI_API_KEY)

        GENERATION_CONFIG = types.GenerateContentConfig(
            **model_options,
            safety_settings=[
                types.SafetySetting(category=k, threshold=v)
                for k, v in safety_settings.items()
            ],
            system_instruction=SYSTEM_INSTRUCTION,
        )

    for idx, batch in enumerate(batches, start=1):
        log.info(
            "[%s] Batch %d/%d — %d players...",
            model_key, idx, len(batches), len(batch),
        )
        try:
            prompt = build_prompt(batch)
            if model_key == "llama":
                raw_output = invoke_ollama_chat(model_name, model_options, prompt, idx)
            elif model_key == "gemini":
                response = invoke_api_with_retry(client, model_name, prompt, model_options)
                raw_output = response.text
                time.sleep(60.0 / GOOGLE_RPM) # Manage rate limit

            raw_list = extract_json_array(raw_output, idx)
            valid_records = validate_batch(raw_list, idx)
            all_records.extend(valid_records)
            log.info(
                "[%s] Batch %d complete. Running total: %d records.",
                model_key, idx, len(all_records),
            )
        except Exception as exc:
            log.error(
                "[%s] Batch %d failed — skipping. Error: %s",
                model_key, idx, exc, exc_info=True,
            )

    log.info("=== %s finished. Total valid records: %d ===", model_key, len(all_records))
    return all_records

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Tennis Player Scoring Pipeline START ===")

    conn = None
    cur = None
    try:
        log.info("=" * 50)
        log.info("Invoke LLMs — starting")
        start = datetime.now()

        # -----------------------------
        # Connect to PostgreSQL
        # -----------------------------
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        # 1. Fetch weekly data from Postgres
        players = fetch_top_players_weekly_data(cur, SCHEMA)

        if not players:
            log.error("No player data returned from DB. Exiting.")
            return

        # ----------------------------------------------------------------
        # 2. LLaMA 3.1 8B — run all batches via Ollama
        # ----------------------------------------------------------------
        llm_model_key = "llama"
        llm_model_name = LLM_MODELS[llm_model_key]["name"]
        llm_batch_size = LLM_MODELS[llm_model_key]["batch_size"]
        llm_model_options = LLM_MODELS[llm_model_key]["options"]
        # Split based on batch size
        batches = make_batches(players, llm_batch_size)
        log.info(
            "Players: %d | Batch size: %d | Total batches: %d",
            len(players), llm_batch_size, len(batches),
        )

        llama_raw = run_model_pipeline(
            model_key=llm_model_key,
            model_name=llm_model_name,
            model_options=llm_model_options,
            safety_settings=None,
            batches=batches,
        )

        # Evict LLaMA from RAM
        unload_ollama_model(llm_model_name)

        # Polars cleanup + DB insert for LLaMA
        llama_clean = clean_with_polars(llama_raw, model_label=llm_model_name)
        insert_player_profiles(cur, llama_clean, model_label=llm_model_name)
        log.info("Inserted/updated %d rows for model=%s", len(llama_clean), llm_model_name)

        # ----------------------------------------------------------------
        # 3. Gemini Flash — run all batches via Gemini API
        # ----------------------------------------------------------------
        llm_model_key = "gemini"
        llm_model_name = LLM_MODELS[llm_model_key]["name"]
        llm_batch_size = LLM_MODELS[llm_model_key]["batch_size"]
        llm_model_options = LLM_MODELS[llm_model_key]["options"]
        llm_model_safety_settings = LLM_MODELS[llm_model_key]["safety_settings"]
        # Split based on batch size
        batches = make_batches(players, llm_batch_size)
        log.info(
            "Players: %d | Batch size: %d | Total batches: %d",
            len(players), llm_batch_size, len(batches),
        )

        gemini_raw = run_model_pipeline(
            model_key=llm_model_key,
            model_name=llm_model_name,
            model_options=llm_model_options,
            safety_settings=llm_model_safety_settings,
            batches=batches,
        )

        # Polars cleanup + DB insert for Gemini
        gemini_clean = clean_with_polars(gemini_raw, model_label=llm_model_name)
        insert_player_profiles(cur, gemini_clean, model_label=llm_model_name)

        conn.commit()
        log.info("Inserted/updated %d rows for model=%s", len(gemini_clean), llm_model_name)

        elapsed = int((datetime.now() - start).total_seconds())
        log.info(f"Invoke LLMs — complete ({elapsed}s)")
        log.info("=" * 50)

    except psycopg2.Error as db_err:
        log.error("Database error: %s", db_err, exc_info=True)
        if conn:
            conn.rollback()
    except RuntimeError as rt_err:
        # Covers Ollama not running / model not found
        log.error("Runtime error: %s", rt_err)
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        if conn:
            conn.rollback()
    finally:
        # Ensure resources are freed and closed
        if cur:
            cur.close()

        if conn is not None:
            conn.close()
        gc.collect()

if __name__ == "__main__":
    main()

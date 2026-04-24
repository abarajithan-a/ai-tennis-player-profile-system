"""
Tennis Player Scoring Pipeline
- Fetches weekly summarized data for top N players from Postgres
- Scores players using LLaMA 3.1 8B (via Ollama), Google Gemini Flash API and Groq OSS GPT
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
import asyncio
from typing import Any
from groq import Groq
from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError
from datetime import date, datetime
from pathlib import Path
from psycopg2 import sql, extras
from pydantic import BaseModel, Field, ValidationError
from config import DB_CONFIG, SCHEMA, GOOGLE_AI_API_KEY, GROQ_API_KEY

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
PYTHON_MAX_CONCURRENCY = 1
SEMAPHORE = asyncio.Semaphore(PYTHON_MAX_CONCURRENCY)
SQL_FILE = "llm_input.sql"
LLM_MODELS = {
    "ollama": {
        "name": "llama3.1:8b",
        "batch_size": None,  # concurrency handled by asyncio, not batching
        "options": {
            "temperature": 0.1,
            "num_ctx": 2048,  # 900 token prompt + 450 output + 698 buffer
            "num_predict": 500,  # 450 expected output tokens + 50 margin
            "num_batch": 512,  # Number of tokens to process internally in 1 sweep
        },
        "safety_settings": None,
    },
    "gemini": {
        "name": "gemini-3-flash-preview",  # "gemini-3.1-flash-lite-preview",
        "batch_size": 10,
        "options": {  # Maps to generation config in Google Gemini API
            "temperature": 0.1,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 15000,  # To accomodate rare thought tokens spike to 8000
            "response_mime_type": "application/json",  # Forces JSON response,
            "thinking_config": {
                "thinking_level": "medium"  # thought tokens on avg between 1500 - 2800,
            },
        },
        "safety_settings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ],
    },
    "groq": {
        "name": "openai/gpt-oss-120b",  # "meta-llama/llama-4-scout-17b-16e-instruct",
        "batch_size": 4,
        "options": {
            "temperature": 0.1,
            "reasoning_effort": "low",
            "max_completion_tokens": 2400,
            "top_p": 0.95,
            "response_format": {"type": "json_object"},
        },
        "safety_settings": None,
        # Formula: Prompt at batch size N ≈ 900 + (N × 550)
        # max_completion_tokens = 1100 (i.e. Reasoning effort budget for Low) + (N × 220) + 200 (i.e. Buffer)
        # SUM of input prompt + max_completion_tokens < TPM
    },
}
TOP_N_PLAYERS = 100
# -----------------------------
# API Settings
# -----------------------------

GOOGLE_RPM = 3  # Requests per minute - Rate Limit
GROQ_RPM = 1  # conservative free-tier estimate RPM and TPM
MAX_RETRIES = 3

# -----------------------------
# DB Columns
# -----------------------------
SCORE_COLUMNS = [
    "serve_quality_score",
    "return_quality_score",
    "forehand_quality_score",
    "backhand_quality_score",
    "net_play_effectiveness_score",
    "clutch_score",
    "efficiency_score",
    "overall_score",
]

REASONING_COLUMNS = ["overall_reasoning", "what_went_well", "areas_to_improve"]

PLAYER_PROFILE_COLUMNS = (
    ["player_profile_hash_id", "player_hash_id", "match_week_monday", "llm_model"]
    + SCORE_COLUMNS
    + REASONING_COLUMNS
    + ["generated_at"]
)


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
    query = query_template.format(schema=sql.Identifier(schema).as_string(cur))

    # Filter Top N players
    cur.execute(query, {"top_n_players": TOP_N_PLAYERS})
    rows = [dict(r) for r in cur.fetchall()]
    log.info("Fetched %d player records.", len(rows))
    return rows


def insert_player_profiles(cur, records: list[dict], model_name: str) -> None:
    """Upsert scored player profiles into player_profile table."""
    if not records:
        log.warning("No records to insert DB for model=%s", model_name)
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
You are a tennis analytics engine. You will receive players with current week stats and trailing 4-week averages (prefixed w4_). NULL w4_ means no prior data.

Score each player from 0.0 to 10.0 on the following:

SCORING DIMENSIONS:

forehand_quality_score:
- Use: forehand_winners, forehand_errors
- Context: winners_to_errors_ratio (forehand), w4_forehand_winners/errors

backhand_quality_score:
- Use: backhand_winners, backhand_errors
- Context: winners_to_errors_ratio (backhand), w4_backhand_winners/errors

serve_quality_score:
- Use: first_serve_in_percent, aces, double_faults
- Context: w4_first_serve_in_percent, w4_aces, w4_double_faults

return_quality_score:
- Use: return_points_won_percent
- Context: w4_return_points_won_percent, break_points_saved_percent

net_play_effectiveness_score:
- Use: net_win_percent, net_winners, net_errors
- Context: w4_net_win_percent, w4_net_winners, w4_net_errors

clutch_score:
- Use: break_points_saved_percent, break_points_saved, break_points_faced
- Context: w4_break_points_saved_percent
- High save %% increases score; penalize very low break_points_faced (small sample)

efficiency_score:
- Primary: winners_to_errors_ratio, first_serve_in_percent
- Supporting: net_win_percent, return_points_won_percent, aces, double_faults
- Reflects overall point-winning efficiency

overall_score:
- Holistic rating (NOT average)
- Weight serve and return more heavily
- Compare current vs w4_ baseline for consistency
- Adjust if performance deviates significantly from baseline

GENERAL GUIDELINES:
- Always compare current vs w4_ where available
- Penalize high double_faults heavily
- winners_to_errors_ratio is strongest positive signal
- If matches_played = 1, moderate extreme scores
- If all w4_ are NULL, use current data only

REASONING:
- Provide:
  - overall_reasoning (one sentence)
  - what_went_well (strongest metric/pattern)
  - areas_to_improve (weakest metric or biggest drop vs w4_)
- Must reference specific metrics (values or clear signals)
- Do NOT repeat player name
- Max 20 words per field

OUTPUT:
- Return ONLY a valid JSON array
- No extra text, markdown, or explanation
- All scores must be floats between 0.0 and 10.0
- Do not add extra keys

FORMAT:
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
  "overall_reasoning": "<max 20 words>",
  "what_went_well": "<max 20 words>",
  "areas_to_improve": "<max 20 words>"
}
"""


def build_prompt(player_batch: list[dict]) -> str:
    player_json = json.dumps(player_batch, indent=2, default=str)
    return (
        f"{SCORING_INSTRUCTIONS}\n\n"
        f"Player data:\n{player_json}\n\n"
        f"You MUST return a JSON array with exactly {len(player_batch)} objects.\n"
        f"Do not stop until all {len(player_batch)} items are processed.\n"
        f"Return ONLY the JSON array. No extra text."
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


def call_ollama_chat(model_name, model_options, player_data):
    """
    Send a prompt to Ollama via ollama.chat and return the response text.
    Ollama keeps the model warm in memory across consecutive calls.
    """
    prompt = build_prompt([player_data])
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
        keep_alive=-1,  # -1 keeps the model pinned in RAM permanently until unloaded
    )
    raw = response["message"]["content"]
    return raw


async def score_thread(model_name, model_options, player_data):
    async with SEMAPHORE:
        try:
            raw = await asyncio.to_thread(
                call_ollama_chat,
                model_name,
                model_options,
                player_data,
            )

            # Only parsing belongs here (NOT normalization)
            parsed = json.loads(raw)
            return parsed

        except json.JSONDecodeError:
            log.error(
                "[%s] JSON decode failed for player_hash_id=%s | raw=%s",
                model_name,
                player_data.get("player_hash_id"),
                raw[:300] if "raw" in locals() else "N/A",
                exc_info=True,
            )
            return None

        except Exception as e:
            log.error(
                "[%s] LLM call failed for player_hash_id=%s",
                model_name,
                player_data.get("player_hash_id"),
                exc_info=True,
            )
            return None


async def invoke_ollama(model_name, model_options, players):
    tasks = [score_thread(model_name, model_options, p) for p in players]

    results = await asyncio.gather(*tasks)
    return results


def unload_ollama_model(model_name: str) -> None:
    """
    Evict a model from Ollama's memory by sending a keep_alive=0 request.
    Call this between LLaMA and Mistral to free RAM.
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
    """Extract JSON array from raw LLM output.
    Handles both array [...] and single object {...} responses."""

    # Try array first
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1:
        return json.loads(raw[start : end + 1])

    # Fall back to single object
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1:
        obj = json.loads(raw[start : end + 1])
        return [obj]  # wrap in list for consistent downstream handling

    raise ValueError(
        f"Batch {batch_index}: No JSON array or object found in output. "
        f"Preview: {raw[:300]}"
    )


def validate_records(
    raw_output: Any,
    input_records: list[dict],
    model_name: str,
    batch_index: int = 1,
    strict: bool = True,
) -> list[dict]:
    """
    Universal validator for all LLM outputs.

    Ensures:
    - Output maps to input size
    - Each record is valid PlayerScore
    - Handles dict, list, None safely
    """

    expected_size = len(input_records)

    # -----------------------------
    # Normalize output shape
    # -----------------------------
    if raw_output is None:
        log.error("[%s] Batch %d: raw_output is None", model_name, batch_index)
        return []

    if isinstance(raw_output, dict):
        parsed_list = [raw_output]

    elif isinstance(raw_output, list):
        parsed_list = raw_output

    else:
        raise TypeError(
            f"[{model_name}] Batch {batch_index}: Unexpected output type {type(raw_output)}"
        )

    received_size = len(parsed_list)

    # -----------------------------
    # Size mismatch handling
    # -----------------------------
    if received_size != expected_size:
        log.warning(
            "[%s] Batch %d: size mismatch (expected=%d, received=%d)",
            model_name,
            batch_index,
            expected_size,
            received_size,
        )

        if strict:
            raise ValueError(
                f"[{model_name}] Batch {batch_index}: expected {expected_size}, got {received_size}"
            )

    # -----------------------------
    # Validation loop
    # -----------------------------
    valid_records = []
    invalid_count = 0

    for item in parsed_list:
        try:
            score = PlayerScore(**item)
            valid_records.append(score.model_dump())

        except ValidationError as exc:
            invalid_count += 1
            log.warning(
                "[%s] Batch %d: validation failed for player=%s | %s",
                model_name,
                batch_index,
                item.get("player_hash_id", "?") if isinstance(item, dict) else "?",
                exc,
            )

    # -----------------------------
    # Final sanity checks
    # -----------------------------
    if len(valid_records) == 0:
        log.error(
            "[%s] Batch %d: all records failed validation", model_name, batch_index
        )

    log.info(
        "[%s] Batch %d: valid=%d invalid=%d expected=%d received=%d",
        model_name,
        batch_index,
        len(valid_records),
        invalid_count,
        expected_size,
        received_size,
    )

    return valid_records


# ---------------------------------------------------------------------------
# Polars cleanup
# ---------------------------------------------------------------------------
def clean_with_polars(records: list[dict], model_name: str) -> list[dict]:
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
        log.warning("clean_with_polars: no records to clean for model=%s", model_name)
        return []

    log.info(
        "Cleaning %d records with Polars for model=%s...", len(records), model_name
    )

    generated_at = datetime.now()

    df = pl.DataFrame(records)
    df = (
        df
        # Ensure date column is typed correctly before any operations
        .with_columns(pl.col("match_week_monday").cast(pl.Date))
        # Cast, clamp and round all score columns
        .with_columns(
            [
                pl.col(c).cast(pl.Float64).clip(0.0, 10.0).round(2).alias(c)
                for c in SCORE_COLUMNS
            ]
        )
        # Truncate reasoning
        .with_columns([pl.col(c).str.slice(0, 500).alias(c) for c in REASONING_COLUMNS])
        # Sort before dedup so we deterministically keep the highest overall_score
        .sort("overall_score", descending=True)
        .unique(subset=["player_hash_id"], keep="first")
        # Drop likely bad generations
        .filter(pl.col("overall_score") > 0.0)
        # Add metadata columns
        .with_columns(pl.lit(model_name).alias("llm_model"))
        .with_columns(pl.lit(generated_at).alias("generated_at"))
        # Compute md5 hash — build the raw string natively in Polars,
        # then apply hashlib only on the single concatenated string column
        .with_columns(
            pl.concat_str(
                [
                    pl.col("player_hash_id"),
                    pl.lit("_"),
                    pl.col("match_week_monday").cast(pl.String),
                    pl.lit("_"),
                    pl.col("llm_model"),
                ]
            )
            .map_elements(
                lambda s: hashlib.md5(s.encode()).hexdigest(), return_dtype=pl.String
            )
            .alias("player_profile_hash_id")
        )
        .sort("player_profile_hash_id")
    )

    log.info("Polars cleanup done — %d rows", df.height)
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------
def make_batches(players: list[dict], batch_size: int) -> list[list[dict]]:
    return [players[i : i + batch_size] for i in range(0, len(players), batch_size)]


def invoke_api_with_retry(client, model_key, model_name, prompt, model_options):
    def _extract_error_info(e):
        """Normalize error into (code, message)."""
        code = (
            getattr(e, "code", None)
            or getattr(e, "status_code", None)
            or getattr(e, "status", None)
        )

        message = str(e)

        # Try deeper extraction if available
        if hasattr(e, "response"):
            try:
                message = str(e.response)
                code = code or e.response.get("status")
            except Exception:
                pass

        return code, message

    def _call_model():
        """Dispatch per provider."""
        if model_key == "gemini":
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=model_options,
            )

            return response.text  # response.usage_metadata for tokens consumed

        elif model_key == "groq":
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_INSTRUCTION},
                    {"role": "user", "content": prompt},
                ],
                temperature=model_options.get("temperature", 0.1),
                max_completion_tokens=model_options.get("max_completion_tokens", 2048),
                top_p=model_options.get("top_p", 0.95),
                reasoning_effort=model_options.get("reasoning_effort", "low"),
                response_format=model_options.get(
                    "response_format", {"type": "json_object"}
                ),
            )
            return response.choices[0].message.content

        else:
            raise ValueError(f"Unsupported model_key: {model_key}")

    for attempt in range(MAX_RETRIES + 1):
        try:
            return _call_model()

        except Exception as e:
            code, message = _extract_error_info(e)

            # -----------------------------
            # Retryable errors
            # -----------------------------
            if code in (429, 500, 503) or any(
                k in message.lower()
                for k in ["rate limit", "quota", "timeout", "temporarily unavailable"]
            ):
                if attempt == MAX_RETRIES:
                    raise

                wait = 2**attempt * 5
                log.info(
                    "[%s] Retryable error (code=%s). Retrying in %ss... (%d/%d)",
                    model_key,
                    code,
                    wait,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            # -----------------------------
            # Non-retryable errors
            # -----------------------------
            if code in (400, 401, 403, 404):
                log.error(
                    "[%s] Non-retryable error (code=%s): %s", model_key, code, message
                )
                raise

            # -----------------------------
            # Unknown errors → retry
            # -----------------------------
            if attempt == MAX_RETRIES:
                raise

            wait = 2**attempt * 5
            log.warning(
                "[%s] Unknown error (code=%s). Retrying in %ss... (%d/%d) | %s",
                model_key,
                code,
                wait,
                attempt + 1,
                MAX_RETRIES,
                message,
            )
            time.sleep(wait)


async def run_model_pipeline(
    model_key: str,
    model_cfg: dict,
    input_data: list[dict],
) -> list[dict]:
    """
    Run all batches through llm models.
    Ollama keeps the model warm across calls — no manual load needed.
    Returns all validated records across all batches.
    """
    all_records: list[dict] = []

    # Extract LLM Config Info
    model_name = model_cfg["name"]
    batch_size = model_cfg["batch_size"]
    model_options = model_cfg.get("options")
    model_safety_settings = model_cfg.get("safety_settings")

    log.info("=== Starting %s (%s) ===", model_key, model_name)

    if model_key == "ollama":
        check_ollama_model(model_name)
        raw_output = await invoke_ollama(model_name, model_options, input_data)
        raw_output = [r for r in raw_output if r is not None]
        # Flatten the list of lists (since the prompt is  batch of 1)
        raw_output = [
            item
            for sublist in raw_output
            if isinstance(sublist, list)
            for item in sublist
            if isinstance(item, dict)
        ]
        all_records = validate_records(
            raw_output=raw_output,
            input_records=input_data,
            model_name=model_name,
            strict=False,  # individual failures already handled in score_thread
        )
        unload_ollama_model(model_name)
    else:
        if model_key == "gemini":
            client = genai.Client(api_key=GOOGLE_AI_API_KEY)

            GENERATION_CONFIG = types.GenerateContentConfig(
                temperature=model_options["temperature"],
                top_p=model_options["top_p"],
                top_k=model_options["top_k"],
                max_output_tokens=model_options["max_output_tokens"],
                response_mime_type=model_options["response_mime_type"],
                thinking_config=types.ThinkingConfig(
                    thinking_level=model_options.get("thinking_config", {})
                    .get("thinking_level", "low")
                    .lower()
                ),
                safety_settings=[
                    types.SafetySetting(
                        category=s["category"], threshold=s["threshold"]
                    )
                    for s in model_safety_settings
                ],
                system_instruction=SYSTEM_INSTRUCTION,
            )

        elif model_key == "groq":
            client = Groq(api_key=GROQ_API_KEY)

        # Split based on batch size
        batches = make_batches(input_data, batch_size)

        log.info(
            "Model: %s | Players: %d | Batch size: %d | Total batches: %d",
            model_name,
            len(input_data),
            batch_size,
            len(batches),
        )

        for idx, batch in enumerate(batches, start=1):
            log.info(
                "[%s] Batch %d/%d — %d players...",
                model_key,
                idx,
                len(batches),
                len(batch),
            )
            try:
                prompt = build_prompt(batch)
                if model_key == "gemini":
                    raw_output = invoke_api_with_retry(
                        client, model_key, model_name, prompt, GENERATION_CONFIG
                    )
                    time.sleep(60.0 / GOOGLE_RPM)  # Manage rate limit
                elif model_key == "groq":
                    raw_output = invoke_api_with_retry(
                        client, model_key, model_name, prompt, model_options
                    )
                    time.sleep(60.0 / GROQ_RPM)  # Manage rate limit

                raw_list = extract_json_array(raw_output, idx)
                valid_records = validate_records(
                    raw_output=raw_list,
                    input_records=batch,
                    model_name=model_name,
                    batch_index=idx,
                )

                all_records.extend(valid_records)
                log.info(
                    "[%s] Batch %d complete. Running total: %d records.",
                    model_key,
                    idx,
                    len(all_records),
                )
            except Exception as exc:
                log.error(
                    "[%s] Batch %d failed — skipping. Error: %s",
                    model_key,
                    idx,
                    exc,
                    exc_info=True,
                )

    log.info(
        "=== Completed %s (%s). Total valid records: %d ===",
        model_key,
        model_name,
        len(all_records),
    )
    return all_records


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
async def main() -> None:
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
        players_data = fetch_top_players_weekly_data(cur, SCHEMA)

        if not players_data:
            log.error("No player data returned from DB. Exiting.")
            return

        for llm_model_key, llm_model_cfg in LLM_MODELS.items():
            llm_model_name = llm_model_cfg["name"]
            validated_output = await run_model_pipeline(
                model_key=llm_model_key,
                model_cfg=llm_model_cfg,
                input_data=players_data,
            )

            # Polars cleanup + DB insert
            clean_df = clean_with_polars(validated_output, model_name=llm_model_name)
            insert_player_profiles(cur, clean_df, model_name=llm_model_name)

            log.info(
                "DB Insert/Update completed - %d rows for model=%s",
                len(clean_df),
                llm_model_name,
            )

            time.sleep(15)  # Wait to free up resources before starting next model
            log.info("=" * 50)

        conn.commit()

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
    asyncio.run(main())

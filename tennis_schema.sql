-- =============================================================
--  TENNIS PLAYER PROFILE PIPELINE
--  Schema: tennis
--  PostgreSQL 14+
--  All statements idempotent (IF NOT EXISTS)
-- =============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "citext";

-- ── Players (from Sackmann atp_players.csv) ───────────────────
CREATE TABLE IF NOT EXISTS {schema}.players (
    player_hash_id  CHAR(32)        PRIMARY KEY,
    atp_id          VARCHAR(10)     NOT NULL UNIQUE,   -- Sackmann player_id
    first_name      TEXT            NOT NULL,
    last_name       TEXT            NOT NULL,
    date_of_birth   DATE,
    country_code    CHAR(3),                           -- ITF 3-letter code
    height_cm       SMALLINT,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Weekly Rankings (from Sackmann atp_rankings_*.csv) ────────
CREATE TABLE IF NOT EXISTS {schema}.rankings (
    ranking_hash_id CHAR(32)        PRIMARY KEY,
    ranking_date    DATE            NOT NULL,
    rank            SMALLINT        NOT NULL,
    player_hash_id  CHAR(32)        NOT NULL REFERENCES {schema}.players(player_hash_id) ON DELETE CASCADE,
    ranking_points  INT,
    UNIQUE (ranking_date, player_hash_id)
);


-- ── Matches (from Sackmann atp_matches_*.csv) ─────────────────
CREATE TABLE IF NOT EXISTS {schema}.matches (
    match_hash_id   CHAR(32)        PRIMARY KEY,
    match_date      DATE            NOT NULL,
    tourney_name    VARCHAR(100),
    tourney_level   CHAR(1),    -- A - ATP, M - Masters, G - Grand Slam
    surface         VARCHAR(10),
    draw_size       SMALLINT,         
    round           VARCHAR(5),                        -- R128, R64, QF, SF, F etc
    best_of         SMALLINT,                          -- 3 or 5
    winner_hash_id  CHAR(32)        NOT NULL REFERENCES {schema}.players(player_hash_id),
    loser_hash_id   CHAR(32)        NOT NULL REFERENCES {schema}.players(player_hash_id),
    score           TEXT,
    minutes         SMALLINT,
    winner_rank     SMALLINT,
    loser_rank      SMALLINT
);

-- ── Match Stats (serve/return, from Sackmann match files) ─────
CREATE TABLE IF NOT EXISTS {schema}.match_stats (
    match_hash_id               CHAR(32)    NOT NULL REFERENCES {schema}.matches(match_hash_id) ON DELETE CASCADE,
    player_hash_id              CHAR(32)    NOT NULL REFERENCES {schema}.players(player_hash_id),
    result                      CHAR(1)     CHECK (result IN ('W', 'L')), -- W - Win, L - Loss

    -- serve stats
    aces                      SMALLINT,
    double_faults             SMALLINT,
    serve_points              SMALLINT,
    first_serves_in           SMALLINT,
    first_serve_points_won    SMALLINT,
    second_serve_points_won   SMALLINT,
    serve_games               SMALLINT,
    break_points_saved        SMALLINT,
    break_points_faced        SMALLINT,

    -- Composite primary key
    PRIMARY KEY (match_hash_id, player_hash_id)
);

-- ── Shot Charting (from MatchChartingProject) ─────────────────
CREATE TABLE IF NOT EXISTS {schema}.shot_charting (
    shot_charting_hash_id   CHAR(32)    PRIMARY KEY,
    match_date              DATE        NOT NULL,
    player_name             TEXT        NOT NULL,

    -- serve stats
    aces                      SMALLINT,
    double_faults             SMALLINT,
    serve_points              SMALLINT,
    first_serves_in           SMALLINT,
    first_serve_points_won    SMALLINT,
    second_serve_points_won   SMALLINT,
    break_points_faced        SMALLINT,
    break_points_saved        SMALLINT,
    return_points             SMALLINT,
    return_points_won         SMALLINT,

    -- Shot counts
    forehand_winners        SMALLINT,
    backhand_winners        SMALLINT,
    forehand_errors         SMALLINT,
    backhand_errors         SMALLINT,
    net_points_total        SMALLINT,
    net_points_won          SMALLINT,
    net_winners             SMALLINT,
    net_errors              SMALLINT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── LLM Generated Profiles ────────────────────────────────────
CREATE TABLE IF NOT EXISTS {schema}.player_profiles (
    player_profile_hash_id CHAR(32) PRIMARY KEY,
    player_hash_id  CHAR(32)        NOT NULL REFERENCES {schema}.players(player_hash_id),
    profile_compute_date DATE       NOT NULL,
    llm_model       TEXT            NOT NULL,   -- e.g. 'llama3.1:8b'
    serve_quality_score             NUMERIC(3,1)    CHECK (serve_quality_score BETWEEN 0 AND 10),
    forehand_quality_score          NUMERIC(3,1)    CHECK (forehand_quality_score BETWEEN 0 AND 10),
    backhand_quality_score          NUMERIC(3,1)    CHECK (backhand_quality_score BETWEEN 0 AND 10),
    game_variety_score              NUMERIC(3,1)    CHECK (game_variety_score BETWEEN 0 AND 10),
    clutch_score                    NUMERIC(3,1)    CHECK (clutch_score BETWEEN 0 AND 10),
    efficiency_score                NUMERIC(3,1)    CHECK (efficiency_score BETWEEN 0 AND 10),   
    generated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
--  INDEXES
-- =============================================================
CREATE INDEX IF NOT EXISTS idx_rankings_date        ON {schema}.rankings (ranking_date);
CREATE INDEX IF NOT EXISTS idx_matches_date         ON {schema}.matches (match_date);
CREATE INDEX IF NOT EXISTS idx_matches_winner       ON {schema}.matches (winner_hash_id);
CREATE INDEX IF NOT EXISTS idx_matches_loser        ON {schema}.matches (loser_hash_id);
CREATE INDEX IF NOT EXISTS idx_match_stats_player   ON {schema}.match_stats (player_hash_id);
CREATE INDEX IF NOT EXISTS idx_match_stats_result   ON {schema}.match_stats (result);
CREATE INDEX IF NOT EXISTS idx_shot_charting_date   ON {schema}.shot_charting (match_date);
CREATE INDEX IF NOT EXISTS idx_profiles_atp_id      ON {schema}.player_profiles (player_hash_id);
CREATE INDEX IF NOT EXISTS idx_profiles_week        ON {schema}.player_profiles (profile_compute_date);

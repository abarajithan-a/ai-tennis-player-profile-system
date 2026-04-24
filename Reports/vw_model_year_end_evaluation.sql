CREATE OR REPLACE VIEW tennis.vw_model_year_end_evaluation AS
WITH year_end_player_reasoning AS (
    SELECT
        TO_CHAR(pp.match_week_monday, 'IYYY')::INT AS year_number,
        pp.player_hash_id,
        pp.llm_model,
        pp.what_went_well,
        pp.areas_to_improve,
        pp.overall_reasoning,
        ROW_NUMBER() OVER (
            PARTITION BY TO_CHAR(pp.match_week_monday, 'IYYY'), pp.player_hash_id, pp.llm_model
            ORDER BY pp.match_week_monday DESC
        ) AS year_end_reasoning_rank
    FROM tennis.player_profiles AS pp
),

player_list AS (
    SELECT DISTINCT
        player_hash_id,
        TRIM(first_name || ' ' || last_name) AS player_name
    FROM tennis.players
)

SELECT
    yp.year_number,
    pl.player_name,
    yp.llm_model,
    yp.what_went_well,
    yp.areas_to_improve,
    yp.overall_reasoning
FROM year_end_player_reasoning AS yp
INNER JOIN player_list AS pl
    ON yp.player_hash_id = pl.player_hash_id
WHERE yp.year_end_reasoning_rank = 1;

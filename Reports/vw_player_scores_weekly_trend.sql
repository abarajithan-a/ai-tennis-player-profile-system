CREATE OR REPLACE VIEW tennis.vw_player_scores_weekly_trend AS
WITH week_series AS (
    SELECT GENERATE_SERIES(
        DATE_TRUNC('week', DATE '2020-01-04'),
        DATE_TRUNC('week', CURRENT_DATE),
        INTERVAL '1 week'
    )::DATE AS week_start
),
player_list AS (
    SELECT DISTINCT
        player_hash_id,
        TRIM(first_name || ' ' || last_name) AS player_name
    FROM tennis.players
),
player_week_series AS (
    SELECT
        p.player_hash_id,
        p.player_name,
        w.week_start
    FROM player_list p
    CROSS JOIN week_series w
)
SELECT
    TO_CHAR(pws.week_start, 'IYYY')::INT AS year_number,
    TO_CHAR(pws.week_start, 'IW')::INT AS week_number,
    pws.week_start,
    pws.player_hash_id,
    pws.player_name,
    pp.llm_model,
    pp.serve_quality_score,
    pp.return_quality_score,
    pp.forehand_quality_score,
    pp.backhand_quality_score,
    pp.net_play_effectiveness_score,
    pp.clutch_score,
    pp.efficiency_score,
    pp.overall_score
FROM player_week_series pws
LEFT JOIN tennis.player_profiles pp
    ON pp.player_hash_id = pws.player_hash_id
   AND pp.match_week_monday = pws.week_start;

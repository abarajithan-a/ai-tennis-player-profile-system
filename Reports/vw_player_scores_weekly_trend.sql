CREATE OR REPLACE VIEW tennis.vw_player_scores_weekly_trend AS
WITH player_names AS (
    SELECT DISTINCT
        player_hash_id,
        (TRIM(first_name) || ' ' || TRIM(last_name))::TEXT AS player_name
    FROM tennis.players
)

SELECT
    EXTRACT(ISOYEAR FROM pp.match_week_monday)::INT AS trend_year,
    ('Week ' || TO_CHAR(pp.match_week_monday, 'IW')) AS week_number,
    pn.player_name,
    pp.serve_quality_score,
    pp.return_quality_score,
    pp.forehand_quality_score,
    pp.backhand_quality_score,
    pp.net_play_effectiveness_score,
    pp.clutch_score,
    pp.efficiency_score,
    pp.overall_score
FROM tennis.player_profiles AS pp
INNER JOIN player_names AS pn
    ON pp.player_hash_id = pn.player_hash_id;

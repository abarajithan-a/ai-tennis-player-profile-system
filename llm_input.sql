WITH profiler_date AS (
    -- Get last processed profile week (NULL if no profiles exist)
    SELECT MAX(pp.match_week_monday) AS max_profile_week_monday
    FROM {schema}.player_profiles pp
),
shot_charting_weeks AS (
    -- Compute the Monday of each shot_charting row
    SELECT
        sc.*,
        date_trunc('week', sc.match_date)::DATE AS week_monday
    FROM {schema}.shot_charting sc
),
next_monday AS (
    -- Find the next available Monday based on reference
    SELECT MIN(scw.week_monday) AS next_match_week_monday
    FROM shot_charting_weeks scw
    CROSS JOIN profiler_date pd
    WHERE scw.week_monday > COALESCE(pd.max_profile_week_monday, DATE '2020-01-06' - INTERVAL '7 days')
),
player_ranking AS (
    SELECT
        p.player_hash_id,
        UPPER(TRIM(p.first_name)) || ' ' || UPPER(TRIM(p.last_name)) as player_full_name,
        r.rank,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(p.first_name)) || ' ' || UPPER(TRIM(p.last_name))
                            ORDER BY r.ranking_date DESC, r.rank ASC
        ) as player_name_ranking
    FROM {schema}.players p
    INNER JOIN {schema}.rankings r
        ON p.player_hash_id = r.player_hash_id
    WHERE r.ranking_date <= (SELECT next_match_week_monday FROM next_monday)
),
player_name_ranking AS (
    SELECT
        pr.player_hash_id,
        pr.player_full_name,
        pr.rank as player_rank
    FROM player_ranking pr
    WHERE pr.player_name_ranking = 1 
),
match_week_metrics AS (
    SELECT
        week_monday,
        UPPER(TRIM(scw.player_name)) as player_full_name,
        COUNT(DISTINCT shot_charting_hash_id) as matches_played,
        SUM(aces) as aces,
        SUM(double_faults) as double_faults,
        SUM(serve_points) as serve_points,
        ROUND(
            (SUM(first_serves_in)*100.0/NULLIF(SUM(serve_points),0)), 2
        )::NUMERIC as first_serve_in_percent,
        SUM(break_points_faced) as break_points_faced,
        SUM(break_points_saved) as break_points_saved,
        ROUND(
            (SUM(break_points_saved)*100.0/NULLIF(SUM(break_points_faced),0)), 2
        )::NUMERIC as break_points_saved_percent,
        SUM(return_points) as return_points,
        SUM(return_points_won) as return_points_won,
        ROUND(
            (SUM(return_points_won)*100.0/NULLIF(SUM(return_points),0)), 2
        )::NUMERIC as return_win_percent,
        SUM(forehand_winners) as forehand_winners,
        SUM(backhand_winners) as backhand_winners,
        SUM(forehand_errors) as forehand_errors,
        SUM(backhand_errors) as backhand_errors,
        SUM(net_points_total) as net_points_total,
        SUM(net_points_won) as net_points_won,
        SUM(net_winners) as net_winners,
        SUM(net_errors) as net_errors,
        ROUND(
            (SUM(net_points_won)*100.0/NULLIF(SUM(net_points_total),0)), 2
        )::NUMERIC as net_win_percent,
        ROUND(((SUM(forehand_winners) + SUM(backhand_winners) + SUM(net_winners))*1.0/
            NULLIF((SUM(forehand_errors) + SUM(backhand_errors) + SUM(net_errors)), 0)), 2
        )::NUMERIC as winners_to_errors_ratio
    FROM shot_charting_weeks scw
    CROSS JOIN next_monday nm
    WHERE scw.week_monday = nm.next_match_week_monday
    GROUP BY 1, 2
),
trailing_4_week_metrics AS (
    SELECT
        UPPER(TRIM(scw.player_name)) as player_full_name,
        COUNT(DISTINCT shot_charting_hash_id) as w4_matches_played,
        SUM(aces) as w4_aces,
        SUM(double_faults) as w4_double_faults,
        SUM(serve_points) as w4_serve_points,
        ROUND(
            (SUM(first_serves_in)*100.0/NULLIF(SUM(serve_points),0)), 2
        )::NUMERIC as w4_first_serve_in_percent,
        SUM(break_points_faced) as w4_break_points_faced,
        SUM(break_points_saved) as w4_break_points_saved,
        ROUND(
            (SUM(break_points_saved)*100.0/NULLIF(SUM(break_points_faced),0)), 2
        )::NUMERIC as w4_break_points_saved_percent,
        SUM(return_points) as w4_return_points,
        SUM(return_points_won) as w4_return_points_won,
        ROUND(
            (SUM(return_points_won)*100.0/NULLIF(SUM(return_points),0)), 2
        )::NUMERIC as w4_return_win_percent,
        SUM(forehand_winners) as w4_forehand_winners,
        SUM(backhand_winners) as w4_backhand_winners,
        SUM(forehand_errors) as w4_forehand_errors,
        SUM(backhand_errors) as w4_backhand_errors,
        SUM(net_points_total) as w4_net_points_total,
        SUM(net_points_won) as w4_net_points_won,
        SUM(net_winners) as w4_net_winners,
        SUM(net_errors) as w4_net_errors,
        ROUND(
            (SUM(net_points_won)*100.0/NULLIF(SUM(net_points_total),0)), 2
        )::NUMERIC as w4_net_win_percent,
        ROUND(((SUM(forehand_winners) + SUM(backhand_winners) + SUM(net_winners))*1.0/
            NULLIF((SUM(forehand_errors) + SUM(backhand_errors) + SUM(net_errors)), 0)), 2
        )::NUMERIC as w4_winners_to_errors_ratio
    FROM shot_charting_weeks scw
    CROSS JOIN next_monday nm
    WHERE scw.week_monday >= nm.next_match_week_monday - INTERVAL '4 weeks'
            AND scw.week_monday <  nm.next_match_week_monday
    GROUP BY 1
)
SELECT
    w1.week_monday as processing_week_monday,
    pn.player_hash_id,
    pn.player_full_name,
    pn.player_rank,
    w1.matches_played,
    w1.aces,
    w1.double_faults,
    w1.serve_points,
    w1.first_serve_in_percent,
    w1.break_points_faced,
    w1.break_points_saved,
    w1.break_points_saved_percent,
    w1.return_points,
    w1.return_points_won,
    w1.return_win_percent,
    w1.forehand_winners,
    w1.backhand_winners,
    w1.forehand_errors,
    w1.backhand_errors,
    w1.net_points_total,
    w1.net_points_won,
    w1.net_winners,
    w1.net_errors,
    w1.net_win_percent,
    w1.winners_to_errors_ratio,
    w4.w4_matches_played,
    w4.w4_aces,
    w4.w4_double_faults,
    w4.w4_serve_points,
    w4.w4_first_serve_in_percent,
    w4.w4_break_points_faced,
    w4.w4_break_points_saved,
    w4.w4_break_points_saved_percent,
    w4.w4_return_points,
    w4.w4_return_points_won,
    w4.w4_return_win_percent,
    w4.w4_forehand_winners,
    w4.w4_backhand_winners,
    w4.w4_forehand_errors,
    w4.w4_backhand_errors,
    w4.w4_net_points_total,
    w4.w4_net_points_won,
    w4.w4_net_winners,
    w4.w4_net_errors,
    w4.w4_net_win_percent,
    w4.w4_winners_to_errors_ratio
FROM match_week_metrics w1
LEFT OUTER JOIN trailing_4_week_metrics w4
    ON w1.player_full_name = w4.player_full_name
INNER JOIN player_name_ranking pn
    ON w1.player_full_name = pn.player_full_name
WHERE pn.player_rank <= %(top_n_players)s
;
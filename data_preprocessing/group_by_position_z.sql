DROP TABLE IF EXISTS position_group_stats_z;

CREATE TABLE position_group_stats_z AS
WITH base AS (
    SELECT
        ps.match_id,
        ps.season,
        ps.round,
        ps.home,
        ps.away,
        ps.is_home,

        CASE
            WHEN ps.position IN ('Prop', 'Lock') THEN 'middles'
            WHEN ps.position = 'Hooker' THEN 'hooker'
            WHEN ps.position IN ('Halfback', 'Five-Eighth') THEN 'halves'
            WHEN ps.position IN ('2nd Row') THEN 'second_rows'
            WHEN ps.position = 'Centre' THEN 'centres'
            WHEN ps.position = 'Winger' THEN 'wingers'
            WHEN ps.position = 'Fullback' THEN 'fullback'
            ELSE NULL
        END AS position_group,

        SUM(ps.mins_played) AS mins_played,
        SUM(ps.stint_one) AS stint_one,
        SUM(ps.stint_two) AS stint_two,

        SUM(ps.points) AS points,
        SUM(ps.tries) AS tries,
        SUM(ps.conversions) AS conversions,
        SUM(ps.conversion_attempts) AS conversion_attempts,
        SUM(ps.penalty_goals) AS penalty_goals,
        SUM(ps.goal_conversion_rate) AS goal_conversion_rate,
        SUM(ps.one_point_field_goals) AS one_point_field_goals,
        SUM(ps.two_point_field_goals) AS two_point_field_goals,
        SUM(ps.fantasy_points) AS fantasy_points,

        SUM(ps.all_runs) AS all_runs,
        SUM(ps.all_run_metres) AS all_run_metres,
        SUM(ps.hit_ups) AS hit_ups,
        SUM(ps.post_contact_metres) AS post_contact_metres,
        SUM(ps.kick_return_metres) AS kick_return_metres,
        SUM(ps.line_engaged_runs) AS line_engaged_runs,

        SUM(ps.line_breaks) AS line_breaks,
        SUM(ps.line_break_assists) AS line_break_assists,
        SUM(ps.try_assists) AS try_assists,
        SUM(ps.tackle_breaks) AS tackle_breaks,

        SUM(ps.play_the_ball) AS play_the_ball,
        SUM(ps.average_play_the_ball_speed) AS average_play_the_ball_speed,
        SUM(ps.receipts) AS receipts,
        SUM(ps.passes) AS passes,
        SUM(ps.dummy_passes) AS dummy_passes,
        SUM(ps.offloads) AS offloads,
        SUM(ps.passes_to_run_ratio) AS passes_to_run_ratio,
        SUM(ps.dummy_half_runs) AS dummy_half_runs,
        SUM(ps.dummy_half_run_metres) AS dummy_half_run_metres,

        SUM(ps.tackles_made) AS tackles_made,
        SUM(ps.missed_tackles) AS missed_tackles,
        SUM(ps.ineffective_tackles) AS ineffective_tackles,
        SUM(ps.tackle_efficiency) AS tackle_efficiency,
        SUM(ps.intercepts) AS intercepts,
        SUM(ps.one_on_one_steal) AS one_on_one_steal,
        SUM(ps.one_on_one_lost) AS one_on_one_lost,

        SUM(ps.errors) AS errors,
        SUM(ps.handling_errors) AS handling_errors,
        SUM(ps.penalties) AS penalties,
        SUM(ps.ruck_infringements) AS ruck_infringements,
        SUM(ps.inside_10_metres) AS inside_10_metres,
        SUM(ps.on_report) AS on_report,
        SUM(ps.sin_bins) AS sin_bins,
        SUM(ps.send_offs) AS send_offs,

        SUM(ps.kicks) AS kicks,
        SUM(ps.kicking_metres) AS kicking_metres,
        SUM(ps.forced_drop_outs) AS forced_drop_outs,
        SUM(ps.bomb_kicks) AS bomb_kicks,
        SUM(ps.grubbers) AS grubbers,
        SUM(ps.forty_twenty) AS forty_twenty,
        SUM(ps.twenty_forty) AS twenty_forty,
        SUM(ps.cross_field_kicks) AS cross_field_kicks,
        SUM(ps.kicked_dead) AS kicked_dead,
        SUM(ps.kicks_defused) AS kicks_defused

    FROM player_stats_z ps
    GROUP BY
        ps.match_id,
        ps.season,
        ps.round,
        ps.home,
        ps.away,
        ps.is_home,
        position_group
)
SELECT *
FROM base
WHERE position_group IS NOT NULL;

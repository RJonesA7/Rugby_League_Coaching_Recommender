DROP TABLE IF EXISTS position_group_stats;

CREATE TABLE position_group_stats AS
WITH base AS (
    SELECT
        ps.match_id,
        ps.season,
        ps.round,
        ps.home,
        ps.away,
        ps.is_home,

        ps.mins_played,
        ps.stint_one,
        ps.stint_two,

        ps.points,
        ps.tries,
        ps.conversions,
        ps.conversion_attempts,
        ps.penalty_goals,
        ps.goal_conversion_rate,
        ps.one_point_field_goals,
        ps.two_point_field_goals,
        ps.fantasy_points,

        ps.all_runs,
        ps.all_run_metres,
        ps.hit_ups,
        ps.post_contact_metres,
        ps.kick_return_metres,
        ps.line_engaged_runs,

        ps.line_breaks,
        ps.line_break_assists,
        ps.try_assists,
        ps.tackle_breaks,

        ps.play_the_ball,
        ps.average_play_the_ball_speed,
        ps.receipts,
        ps.passes,
        ps.dummy_passes,
        ps.offloads,
        ps.passes_to_run_ratio,
        ps.dummy_half_runs,
        ps.dummy_half_run_metres,

        ps.tackles_made,
        ps.missed_tackles,
        ps.ineffective_tackles,
        ps.tackle_efficiency,
        ps.intercepts,
        ps.one_on_one_steal,
        ps.one_on_one_lost,

        ps.errors,
        ps.handling_errors,
        ps.penalties,
        ps.ruck_infringements,
        ps.inside_10_metres,
        ps.on_report,
        ps.sin_bins,
        ps.send_offs,

        ps.kicks,
        ps.kicking_metres,
        ps.forced_drop_outs,
        ps.bomb_kicks,
        ps.grubbers,
        ps.forty_twenty,
        ps.twenty_forty,
        ps.cross_field_kicks,
        ps.kicked_dead,
        ps.kicks_defused,

        CASE
            WHEN lower(ps.position) IN ('fullback') THEN 'fullback'
            WHEN lower(ps.position) IN ('winger') THEN 'wingers'
            WHEN lower(ps.position) IN ('centre') THEN 'centres'
            WHEN lower(ps.position) IN ('halfback', 'five-eighth') THEN 'halves'
            WHEN lower(ps.position) IN ('hooker') THEN 'hooker'
            WHEN lower(ps.position) IN ('2nd row') THEN 'second_rows'
            WHEN lower(ps.position) IN ('prop', 'lock') THEN 'middles'
            ELSE NULL
        END AS position_group,

        CASE
            WHEN lower(ps.position) IN ('prop', 'lock', 'middle', 'front row', 'front-row', 'front_row', 'middles') THEN 3.0
            WHEN lower(ps.position) IN ('hooker', 'fullback') THEN 1.0
            ELSE 2.0
        END AS div

    FROM player_stats ps
),
g AS (
    SELECT
        match_id,
        season,
        round,
        home,
        away,
        is_home,
        position_group,
        MAX(div) AS div,

        SUM(mins_played) AS mins_played,
        SUM(stint_one) AS stint_one,
        SUM(stint_two) AS stint_two,

        SUM(points) AS points,
        SUM(tries) AS tries,
        SUM(conversions) AS conversions,
        SUM(conversion_attempts) AS conversion_attempts,
        SUM(penalty_goals) AS penalty_goals,
        SUM(one_point_field_goals) AS one_point_field_goals,
        SUM(two_point_field_goals) AS two_point_field_goals,
        SUM(fantasy_points) AS fantasy_points,

        SUM(all_runs) AS all_runs,
        SUM(all_run_metres) AS all_run_metres,
        SUM(hit_ups) AS hit_ups,
        SUM(post_contact_metres) AS post_contact_metres,
        SUM(kick_return_metres) AS kick_return_metres,
        SUM(line_engaged_runs) AS line_engaged_runs,

        SUM(line_breaks) AS line_breaks,
        SUM(line_break_assists) AS line_break_assists,
        SUM(try_assists) AS try_assists,
        SUM(tackle_breaks) AS tackle_breaks,

        SUM(play_the_ball) AS play_the_ball,
        SUM(receipts) AS receipts,
        SUM(passes) AS passes,
        SUM(dummy_passes) AS dummy_passes,
        SUM(offloads) AS offloads,
        SUM(dummy_half_runs) AS dummy_half_runs,
        SUM(dummy_half_run_metres) AS dummy_half_run_metres,

        SUM(tackles_made) AS tackles_made,
        SUM(missed_tackles) AS missed_tackles,
        SUM(ineffective_tackles) AS ineffective_tackles,
        SUM(intercepts) AS intercepts,
        SUM(one_on_one_steal) AS one_on_one_steal,
        SUM(one_on_one_lost) AS one_on_one_lost,

        SUM(errors) AS errors,
        SUM(handling_errors) AS handling_errors,
        SUM(penalties) AS penalties,
        SUM(ruck_infringements) AS ruck_infringements,
        SUM(inside_10_metres) AS inside_10_metres,
        SUM(on_report) AS on_report,
        SUM(sin_bins) AS sin_bins,
        SUM(send_offs) AS send_offs,

        SUM(kicks) AS kicks,
        SUM(kicking_metres) AS kicking_metres,
        SUM(forced_drop_outs) AS forced_drop_outs,
        SUM(bomb_kicks) AS bomb_kicks,
        SUM(grubbers) AS grubbers,
        SUM(forty_twenty) AS forty_twenty,
        SUM(twenty_forty) AS twenty_forty,
        SUM(cross_field_kicks) AS cross_field_kicks,
        SUM(kicked_dead) AS kicked_dead,
        SUM(kicks_defused) AS kicks_defused,

        SUM(goal_conversion_rate) AS goal_conversion_rate_sum,
        SUM(average_play_the_ball_speed) AS avg_ptb_speed_sum,
        SUM(passes_to_run_ratio) AS passes_to_run_ratio_sum,
        SUM(tackle_efficiency) AS tackle_efficiency_sum

    FROM base
    WHERE position_group IS NOT NULL
    GROUP BY
        match_id,
        season,
        round,
        home,
        away,
        is_home,
        position_group
)
SELECT
    match_id,
    season,
    round,
    home,
    away,
    is_home,
    position_group,

    mins_played,
    stint_one,
    stint_two,

    points,
    tries,
    conversions,
    conversion_attempts,
    penalty_goals,
    (goal_conversion_rate_sum / NULLIF(div, 0))::numeric AS goal_conversion_rate,
    one_point_field_goals,
    two_point_field_goals,
    fantasy_points,

    all_runs,
    all_run_metres,
    hit_ups,
    post_contact_metres,
    kick_return_metres,
    line_engaged_runs,

    line_breaks,
    line_break_assists,
    try_assists,
    tackle_breaks,

    play_the_ball,
    (avg_ptb_speed_sum / NULLIF(div, 0))::numeric AS average_play_the_ball_speed,
    receipts,
    passes,
    dummy_passes,
    offloads,
    (passes_to_run_ratio_sum / NULLIF(div, 0))::numeric AS passes_to_run_ratio,
    dummy_half_runs,
    dummy_half_run_metres,

    tackles_made,
    missed_tackles,
    ineffective_tackles,
    (tackle_efficiency_sum / NULLIF(div, 0))::numeric AS tackle_efficiency,
    intercepts,
    one_on_one_steal,
    one_on_one_lost,

    errors,
    handling_errors,
    penalties,
    ruck_infringements,
    inside_10_metres,
    on_report,
    sin_bins,
    send_offs,

    kicks,
    kicking_metres,
    forced_drop_outs,
    bomb_kicks,
    grubbers,
    forty_twenty,
    twenty_forty,
    cross_field_kicks,
    kicked_dead,
    kicks_defused
FROM g;

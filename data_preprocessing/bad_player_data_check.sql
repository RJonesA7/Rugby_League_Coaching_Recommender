\o /dcs/23/u5503037/CS344/data_preprocessing/bad_player_data_log.txt

SELECT now();

-- Basic identity check
SELECT COUNT(*) AS bad_name
FROM player_stats
WHERE name IS NULL OR name = '';

SELECT COUNT(*) AS bad_number
FROM player_stats
WHERE (number < 0 OR number > 99) AND number IS NOT NULL;

-- Match identifiers present
SELECT COUNT(*) AS bad_season
FROM player_stats
WHERE (season < 1900 OR season > EXTRACT(YEAR FROM now())::int + 1) AND season IS NOT NULL;

SELECT COUNT(*) AS bad_round
FROM player_stats
WHERE (round < 0 OR round > 40) AND round IS NOT NULL;

SELECT COUNT(*) AS bad_home_away
FROM player_stats
WHERE home IS NULL OR home = '' OR away IS NULL OR away = '';

-- Minutes / stints (stored as seconds)
-- NRL: 0–80 mins, allow buffer to 90 in case of golden point
SELECT COUNT(*) AS bad_mins_played
FROM player_stats
WHERE (mins_played < 0 OR mins_played > 5400) AND mins_played IS NOT NULL;

SELECT (season, round, home, away, name, number, mins_played) AS bad_mins_played_rows
FROM player_stats
WHERE (mins_played < 0 OR mins_played > 5400) AND mins_played IS NOT NULL;

-- Stints: each should be <= mins_played and <= 90 mins
SELECT COUNT(*) AS bad_stint_one
FROM player_stats
WHERE (stint_one < 0 OR stint_one > 5400) AND stint_one IS NOT NULL;

SELECT COUNT(*) AS bad_stint_two
FROM player_stats
WHERE (stint_two < 0 OR stint_two > 5400) AND stint_two IS NOT NULL;

-- This is the one that's throing up errors, but the data is all correct from the NRL match centres. They just have some bad data, stints that don't quite sum as they should
SELECT COUNT(*) AS bad_stints_sum
FROM player_stats
WHERE mins_played IS NOT NULL
  AND stint_one IS NOT NULL
  AND stint_two IS NOT NULL
  AND (stint_one + stint_two > mins_played + 60);  -- allow 1 min buffer for rounding error

-- Scoring
-- Player points
SELECT COUNT(*) AS bad_points
FROM player_stats
WHERE (points < 0 OR points > 60) AND points IS NOT NULL;

SELECT COUNT(*) AS bad_tries
FROM player_stats
WHERE (tries < 0 OR tries > 8) AND tries IS NOT NULL;

SELECT COUNT(*) AS bad_conversions
FROM player_stats
WHERE (conversions < 0 OR conversions > 12) AND conversions IS NOT NULL;

SELECT COUNT(*) AS bad_conversion_attempts
FROM player_stats
WHERE (conversion_attempts < 0 OR conversion_attempts > 20) AND conversion_attempts IS NOT NULL;

SELECT COUNT(*) AS bad_conv_attempts_lt_conversions
FROM player_stats
WHERE conversion_attempts IS NOT NULL
  AND conversions IS NOT NULL
  AND conversion_attempts < conversions;

SELECT COUNT(*) AS bad_penalty_goals
FROM player_stats
WHERE (penalty_goals < 0 OR penalty_goals > 10) AND penalty_goals IS NOT NULL;

-- Allow to 100.1, a couple of rogue datapoints are (ineffectually) 100.02
SELECT COUNT(*) AS bad_goal_conversion_rate
FROM player_stats
WHERE (goal_conversion_rate < 0 OR goal_conversion_rate > 100.1) AND goal_conversion_rate IS NOT NULL;

SELECT COUNT(*) AS bad_one_point_field_goals
FROM player_stats
WHERE (one_point_field_goals < 0 OR one_point_field_goals > 5) AND one_point_field_goals IS NOT NULL;

SELECT COUNT(*) AS bad_two_point_field_goals
FROM player_stats
WHERE (two_point_field_goals < 0 OR two_point_field_goals > 5) AND two_point_field_goals IS NOT NULL;

-- Not checking fantasy points. They're pretty irrelevant and the range is massive.

-- Basic internal scoring consistency

-- Running / metres
SELECT COUNT(*) AS bad_all_runs
FROM player_stats
WHERE (all_runs < 0 OR all_runs > 50) AND all_runs IS NOT NULL;

SELECT COUNT(*) AS bad_all_run_metres
FROM player_stats
WHERE (all_run_metres < 0 OR all_run_metres > 500) AND all_run_metres IS NOT NULL;

SELECT COUNT(*) AS bad_hit_ups
FROM player_stats
WHERE (hit_ups < 0 OR hit_ups > 40) AND hit_ups IS NOT NULL;

SELECT COUNT(*) AS bad_post_contact_metres
FROM player_stats
WHERE (post_contact_metres < 0 OR post_contact_metres > 250) AND post_contact_metres IS NOT NULL;

SELECT COUNT(*) AS bad_kick_return_metres
FROM player_stats
WHERE (kick_return_metres < 0 OR kick_return_metres > 300) AND kick_return_metres IS NOT NULL;

SELECT COUNT(*) AS bad_line_engaged_runs
FROM player_stats
WHERE (line_engaged_runs < 0 OR line_engaged_runs > 50) AND line_engaged_runs IS NOT NULL;

-- Attack creation
SELECT COUNT(*) AS bad_line_breaks
FROM player_stats
WHERE (line_breaks < 0 OR line_breaks > 6) AND line_breaks IS NOT NULL;

SELECT COUNT(*) AS bad_line_break_assists
FROM player_stats
WHERE (line_break_assists < 0 OR line_break_assists > 8) AND line_break_assists IS NOT NULL;

SELECT COUNT(*) AS bad_try_assists
FROM player_stats
WHERE (try_assists < 0 OR try_assists > 8) AND try_assists IS NOT NULL;

SELECT COUNT(*) AS bad_tackle_breaks
FROM player_stats
WHERE (tackle_breaks < 0 OR tackle_breaks > 30) AND tackle_breaks IS NOT NULL;

-- Ruck / handling / distribution
SELECT COUNT(*) AS bad_play_the_ball
FROM player_stats
WHERE (play_the_ball < 0 OR play_the_ball > 60) AND play_the_ball IS NOT NULL;

SELECT COUNT(*) AS bad_avg_ptb_speed
FROM player_stats
WHERE (average_play_the_ball_speed < 0.5 OR average_play_the_ball_speed > 15)
  AND average_play_the_ball_speed IS NOT NULL;

SELECT COUNT(*) AS bad_receipts
FROM player_stats
WHERE (receipts < 0 OR receipts > 200) AND receipts IS NOT NULL;

SELECT COUNT(*) AS bad_passes
FROM player_stats
WHERE (passes < 0 OR passes > 200) AND passes IS NOT NULL;

SELECT COUNT(*) AS bad_dummy_passes
FROM player_stats
WHERE (dummy_passes < 0 OR dummy_passes > 120) AND dummy_passes IS NOT NULL;

SELECT COUNT(*) AS bad_offloads
FROM player_stats
WHERE (offloads < 0 OR offloads > 15) AND offloads IS NOT NULL;

SELECT COUNT(*) AS bad_passes_to_run_ratio
FROM player_stats
WHERE (passes_to_run_ratio < 0 OR passes_to_run_ratio > 150 OR passes_to_run_ratio < (passes::numeric / NULLIF(all_runs, 0)) - 1 OR passes_to_run_ratio > (passes::numeric / NULLIF(all_runs, 0)) + 1) AND passes_to_run_ratio IS NOT NULL;

SELECT COUNT(*) AS bad_dummy_half_runs
FROM player_stats
WHERE (dummy_half_runs < 0 OR dummy_half_runs > 25) AND dummy_half_runs IS NOT NULL;

SELECT COUNT(*) AS bad_dummy_half_run_metres
FROM player_stats
WHERE (dummy_half_run_metres < 0 OR dummy_half_run_metres > 250) AND dummy_half_run_metres IS NOT NULL;

-- Defence
SELECT COUNT(*) AS bad_tackles_made
FROM player_stats
WHERE (tackles_made < 0 OR tackles_made > 90) AND tackles_made IS NOT NULL;

SELECT COUNT(*) AS bad_missed_tackles
FROM player_stats
WHERE (missed_tackles < 0 OR missed_tackles > 20) AND missed_tackles IS NOT NULL;

SELECT COUNT(*) AS bad_ineffective_tackles
FROM player_stats
WHERE (ineffective_tackles < 0 OR ineffective_tackles > 25) AND ineffective_tackles IS NOT NULL;

SELECT COUNT(*) AS bad_tackle_efficiency
FROM player_stats
WHERE (tackle_efficiency < 0 OR tackle_efficiency > 101) AND tackle_efficiency IS NOT NULL;

SELECT COUNT(*) AS bad_intercepts
FROM player_stats
WHERE (intercepts < 0 OR intercepts > 5) AND intercepts IS NOT NULL;

SELECT COUNT(*) AS bad_one_on_one_steal
FROM player_stats
WHERE (one_on_one_steal < 0 OR one_on_one_steal > 10) AND one_on_one_steal IS NOT NULL;

SELECT COUNT(*) AS bad_one_on_one_lost
FROM player_stats
WHERE (one_on_one_lost < 0 OR one_on_one_lost > 10) AND one_on_one_lost IS NOT NULL;

-- Errors / discipline
SELECT COUNT(*) AS bad_errors
FROM player_stats
WHERE (errors < 0 OR errors > 10) AND errors IS NOT NULL;

SELECT COUNT(*) AS bad_handling_errors
FROM player_stats
WHERE (handling_errors < 0 OR handling_errors > 10) AND handling_errors IS NOT NULL;

SELECT COUNT(*) AS bad_penalties
FROM player_stats
WHERE (penalties < 0 OR penalties > 10) AND penalties IS NOT NULL;

SELECT COUNT(*) AS bad_ruck_infringements
FROM player_stats
WHERE (ruck_infringements < 0 OR ruck_infringements > 10) AND ruck_infringements IS NOT NULL;

SELECT COUNT(*) AS bad_inside_10_metres
FROM player_stats
WHERE (inside_10_metres < 0 OR inside_10_metres > 10) AND inside_10_metres IS NOT NULL;

SELECT COUNT(*) AS bad_on_report
FROM player_stats
WHERE (on_report < 0 OR on_report > 3) AND on_report IS NOT NULL;

SELECT COUNT(*) AS bad_sin_bins
FROM player_stats
WHERE (sin_bins < 0 OR sin_bins > 3) AND sin_bins IS NOT NULL;

SELECT COUNT(*) AS bad_send_offs
FROM player_stats
WHERE (send_offs < 0 OR send_offs > 1) AND send_offs IS NOT NULL;

-- Kicking
SELECT COUNT(*) AS bad_kicks
FROM player_stats
WHERE (kicks < 0 OR kicks > 40) AND kicks IS NOT NULL;

SELECT COUNT(*) AS bad_kicking_metres
FROM player_stats
WHERE (kicking_metres < 0 OR kicking_metres > 2000) AND kicking_metres IS NOT NULL;

SELECT COUNT(*) AS bad_forced_drop_outs
FROM player_stats
WHERE (forced_drop_outs < 0 OR forced_drop_outs > 10) AND forced_drop_outs IS NOT NULL;

SELECT COUNT(*) AS bad_bomb_kicks
FROM player_stats
WHERE (bomb_kicks < 0 OR bomb_kicks > 25) AND bomb_kicks IS NOT NULL;

SELECT COUNT(*) AS bad_grubbers
FROM player_stats
WHERE (grubbers < 0 OR grubbers > 25) AND grubbers IS NOT NULL;

SELECT COUNT(*) AS bad_forty_twenty
FROM player_stats
WHERE (forty_twenty < 0 OR forty_twenty > 5) AND forty_twenty IS NOT NULL;

SELECT COUNT(*) AS bad_twenty_forty
FROM player_stats
WHERE (twenty_forty < 0 OR twenty_forty > 5) AND twenty_forty IS NOT NULL;

SELECT COUNT(*) AS bad_cross_field_kicks
FROM player_stats
WHERE (cross_field_kicks < 0 OR cross_field_kicks > 20) AND cross_field_kicks IS NOT NULL;

SELECT COUNT(*) AS bad_kicked_dead
FROM player_stats
WHERE (kicked_dead < 0 OR kicked_dead > 10) AND kicked_dead IS NOT NULL;

SELECT COUNT(*) AS bad_kicks_defused
FROM player_stats
WHERE (kicks_defused < 0 OR kicks_defused > 25) AND kicks_defused IS NOT NULL;

\o

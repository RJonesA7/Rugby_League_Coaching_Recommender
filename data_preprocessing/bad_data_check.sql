\o /dcs/23/u5503037/CS344/data_preprocessing/bad_data_log.txt

SELECT now();

-- home (text) – optional presence check
SELECT COUNT(*) AS bad_home FROM matches WHERE home IS NULL OR home = '';

-- Scores / time
SELECT COUNT(*) AS bad_home_score FROM matches WHERE (home_score < 0 OR home_score > 120) AND home_score IS NOT NULL;
-- assume seconds; 20–60 minutes per team with buffer
SELECT COUNT(*) AS bad_time_in_possession_h FROM matches WHERE (time_in_possession_h < 600 OR time_in_possession_h > 4800) AND time_in_possession_h IS NOT NULL;
SELECT (home, away, time_in_possession_h, season) AS bad_time_in_possession_h FROM matches WHERE (time_in_possession_h < 600 OR time_in_possession_h > 4800) AND time_in_possession_h IS NOT NULL;


-- Running & metres
SELECT COUNT(*) AS bad_all_runs_h FROM matches WHERE (all_runs_h < 0 OR all_runs_h > 400) AND all_runs_h IS NOT NULL;
SELECT COUNT(*) AS bad_all_run_metres_h FROM matches WHERE (all_run_metres_h < 0 OR all_run_metres_h > 4000) AND all_run_metres_h IS NOT NULL;
SELECT COUNT(*) AS bad_post_contact_metres_h FROM matches WHERE (post_contact_metres_h < 0 OR post_contact_metres_h > 2000) AND post_contact_metres_h IS NOT NULL;

-- Attack events
SELECT COUNT(*) AS bad_line_breaks_h FROM matches WHERE (line_breaks_h < 0 OR line_breaks_h > 40) AND line_breaks_h IS NOT NULL;
SELECT COUNT(*) AS bad_tackle_breaks_h FROM matches WHERE (tackle_breaks_h < 0 OR tackle_breaks_h > 100) AND tackle_breaks_h IS NOT NULL;
-- metres gained per set (very conservative band)
SELECT COUNT(*) AS bad_average_set_distance_h FROM matches WHERE (average_set_distance_h < 10 OR average_set_distance_h > 80) AND average_set_distance_h IS NOT NULL;
SELECT COUNT(*) AS bad_kick_return_metres_h FROM matches WHERE (kick_return_metres_h < 0 OR kick_return_metres_h > 1000) AND kick_return_metres_h IS NOT NULL;
SELECT COUNT(*) AS bad_offloads_h FROM matches WHERE (offloads_h < 0 OR offloads_h > 80) AND offloads_h IS NOT NULL;
SELECT COUNT(*) AS bad_receipts_h FROM matches WHERE (receipts_h < 0 OR receipts_h > 800) AND receipts_h IS NOT NULL;
SELECT COUNT(*) AS bad_total_passes_h FROM matches WHERE (total_passes_h < 0 OR total_passes_h > 1000) AND total_passes_h IS NOT NULL;
SELECT COUNT(*) AS bad_dummy_passes_h FROM matches WHERE (dummy_passes_h < 0 OR dummy_passes_h > 400) AND dummy_passes_h IS NOT NULL;

-- Kicking
SELECT COUNT(*) AS bad_kicks_h FROM matches WHERE (kicks_h < 0 OR kicks_h > 80) AND kicks_h IS NOT NULL;
SELECT COUNT(*) AS bad_kicking_metres_h FROM matches WHERE (kicking_metres_h < 0 OR kicking_metres_h > 4000) AND kicking_metres_h IS NOT NULL;
SELECT COUNT(*) AS bad_forced_drop_outs_h FROM matches WHERE (forced_drop_outs_h < 0 OR forced_drop_outs_h > 20) AND forced_drop_outs_h IS NOT NULL;
SELECT COUNT(*) AS bad_bombs_h FROM matches WHERE (bombs_h < 0 OR bombs_h > 40) AND bombs_h IS NOT NULL;
SELECT COUNT(*) AS bad_grubbers_h FROM matches WHERE (grubbers_h < 0 OR grubbers_h > 60) AND grubbers_h IS NOT NULL;

-- Defence
SELECT COUNT(*) AS bad_tackles_made_h FROM matches WHERE (tackles_made_h < 0 OR tackles_made_h > 800) AND tackles_made_h IS NOT NULL;
SELECT COUNT(*) AS bad_missed_tackles_h FROM matches WHERE (missed_tackles_h < 0 OR missed_tackles_h > 120) AND missed_tackles_h IS NOT NULL;
SELECT count(*) AS bad_intercepts_h FROM matches WHERE (intercepts_h < 0 OR intercepts_h > 10) AND intercepts_h IS NOT NULL;
SELECT (home, away, season) AS bad_intercepts_h_match FROM matches WHERE (intercepts_h < 0 OR intercepts_h > 10) AND intercepts_h IS NOT NULL;
SELECT COUNT(*) AS bad_ineffective_tackles_h FROM matches WHERE (ineffective_tackles_h < 0 OR ineffective_tackles_h > 150) AND ineffective_tackles_h IS NOT NULL;

-- Errors & discipline
SELECT COUNT(*) AS bad_errors_h FROM matches WHERE (errors_h < 0 OR errors_h > 50) AND errors_h IS NOT NULL;
SELECT COUNT(*) AS bad_penalties_conceded_h FROM matches WHERE (penalties_conceded_h < 0 OR penalties_conceded_h > 40) AND penalties_conceded_h IS NOT NULL;
SELECT COUNT(*) AS bad_ruck_infringements_h FROM matches WHERE (ruck_infringements_h < 0 OR ruck_infringements_h > 30) AND ruck_infringements_h IS NOT NULL;
SELECT COUNT(*) AS bad_inside_ten_metres_h FROM matches WHERE (inside_ten_metres_h < 0 OR inside_ten_metres_h > 40) AND inside_ten_metres_h IS NOT NULL;
SELECT COUNT(*) AS bad_interchanges_used_h FROM matches WHERE (interchanges_used_h < 0 OR interchanges_used_h > 12) AND interchanges_used_h IS NOT NULL;


SELECT COUNT(*) AS bad_completion_rate_h FROM matches WHERE (completion_rate_h < 0 OR completion_rate_h > 100) AND completion_rate_h IS NOT NULL;
SELECT COUNT(*) AS bad_average_play_ball_speed_h FROM matches WHERE (average_play_ball_speed_h < 1 OR average_play_ball_speed_h > 6) AND average_play_ball_speed_h IS NOT NULL;
SELECT COUNT(*) AS bad_kick_defusal_h FROM matches WHERE (kick_defusal_h < 0 OR kick_defusal_h > 100) AND kick_defusal_h IS NOT NULL;
SELECT COUNT(*) AS bad_effective_tackle_h FROM matches WHERE (effective_tackle_h < 0 OR effective_tackle_h > 100) AND effective_tackle_h IS NOT NULL;

-- Scoring events
SELECT COUNT(*) AS bad_tries_h FROM matches WHERE (tries_h < 0 OR tries_h > 30) AND tries_h IS NOT NULL;
SELECT COUNT(*) AS bad_conversions_h FROM matches WHERE (conversions_h < 0 OR conversions_h > 15) AND conversions_h IS NOT NULL;
SELECT COUNT(*) AS bad_conversions_missed_h FROM matches WHERE (conversions_missed_h < 0 OR conversions_missed_h > 15) AND conversions_missed_h IS NOT NULL;
SELECT COUNT(*) AS bad_penalty_goals_h FROM matches WHERE (penalty_goals_h < 0 OR penalty_goals_h > 10) AND penalty_goals_h IS NOT NULL;
SELECT COUNT(*) AS bad_penalty_goals_missed_h FROM matches WHERE (penalty_goals_missed_h < 0 OR penalty_goals_missed_h > 10) AND penalty_goals_missed_h IS NOT NULL;
SELECT COUNT(*) AS bad_sin_bins_h FROM matches WHERE (sin_bins_h < 0 OR sin_bins_h > 10) AND sin_bins_h IS NOT NULL;
SELECT COUNT(*) AS bad_one_point_field_goals_h FROM matches WHERE (one_point_field_goals_h < 0 OR one_point_field_goals_h > 5) AND one_point_field_goals_h IS NOT NULL;
SELECT COUNT(*) AS bad_one_point_field_goals_missed_h FROM matches WHERE (one_point_field_goals_missed_h < 0 OR one_point_field_goals_missed_h > 10) AND one_point_field_goals_missed_h IS NOT NULL;
SELECT COUNT(*) AS bad_two_point_field_goals_h FROM matches WHERE (two_point_field_goals_h < 0 OR two_point_field_goals_h > 5) AND two_point_field_goals_h IS NOT NULL;
SELECT COUNT(*) AS bad_two_point_field_goals_missed_h FROM matches WHERE (two_point_field_goals_missed_h < 0 OR two_point_field_goals_missed_h > 6) AND two_point_field_goals_missed_h IS NOT NULL;

-- Half-time score (team points by HT)
SELECT COUNT(*) AS bad_half_time_h FROM matches WHERE (half_time_h < 0 OR half_time_h > 80) AND half_time_h IS NOT NULL;


-- Identify bad years by checking the above for season > y, with y = 2016, then 17, …


-- Duplicate all queries to check both home and away stats

\o             
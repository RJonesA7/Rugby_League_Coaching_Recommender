-- SQL file to copy over data from the "matches" table I was originally using to the more normalised tables from normalised_schema.sql

-- copy match level data
INSERT INTO match_data(
  match_id, season, round, venue, match_date_utc, match_centre_url,
  overall_first_try_scorer, overall_first_try_minute, overall_first_try_round,
  ref_names, ref_positions, main_ref, ground_condition, weather_condition
)
SELECT
  match_id, season, round, venue, match_date_utc, match_centre_url,
  overall_first_try_scorer, overall_first_try_minute, overall_first_try_round,
  ref_names, ref_positions, main_ref, ground_condition, weather_condition
FROM matches;

-- insert rows for home teams
INSERT INTO team_stats (
  match_id, team, is_home, score, half_time,
  time_in_possession, all_runs, all_run_metres, post_contact_metres,
  line_breaks, tackle_breaks, average_set_distance, kick_return_metres,
  offloads, receipts, total_passes, dummy_passes, kicks, kicking_metres,
  forced_drop_outs, bombs, grubbers, forty_twenty, tackles_made,
  missed_tackles, intercepts, ineffective_tackles, errors,
  penalties_conceded, ruck_infringements, inside_ten_metres,
  interchanges_used, completion_rate, average_play_ball_speed,
  kick_defusal, effective_tackle, tries, conversions, conversions_missed,
  penalty_goals, penalty_goals_missed, sin_bins, on_reports,
  one_point_field_goals, one_point_field_goals_missed,
  two_point_field_goals, two_point_field_goals_missed
)
SELECT
  match_id, home, TRUE, home_score, half_time_h,
  time_in_possession_h, all_runs_h, all_run_metres_h, post_contact_metres_h,
  line_breaks_h, tackle_breaks_h, average_set_distance_h, kick_return_metres_h,
  offloads_h, receipts_h, total_passes_h, dummy_passes_h, kicks_h, kicking_metres_h,
  forced_drop_outs_h, bombs_h, grubbers_h, forty_twenty_h, tackles_made_h,
  missed_tackles_h, intercepts_h, ineffective_tackles_h, errors_h,
  penalties_conceded_h, ruck_infringements_h, inside_ten_metres_h,
  interchanges_used_h, completion_rate_h, average_play_ball_speed_h,
  kick_defusal_h, effective_tackle_h, tries_h, conversions_h, conversions_missed_h,
  penalty_goals_h, penalty_goals_missed_h, sin_bins_h, on_reports_h,
  one_point_field_goals_h, one_point_field_goals_missed_h,
  two_point_field_goals_h, two_point_field_goals_missed_h
FROM matches;

-- 2.3 Insert AWAY rows
INSERT INTO team_stats (
  match_id, team, is_home, score, half_time,
  time_in_possession, all_runs, all_run_metres, post_contact_metres,
  line_breaks, tackle_breaks, average_set_distance, kick_return_metres,
  offloads, receipts, total_passes, dummy_passes, kicks, kicking_metres,
  forced_drop_outs, bombs, grubbers, forty_twenty, tackles_made,
  missed_tackles, intercepts, ineffective_tackles, errors,
  penalties_conceded, ruck_infringements, inside_ten_metres,
  interchanges_used, completion_rate, average_play_ball_speed,
  kick_defusal, effective_tackle, tries, conversions, conversions_missed,
  penalty_goals, penalty_goals_missed, sin_bins, on_reports,
  one_point_field_goals, one_point_field_goals_missed,
  two_point_field_goals, two_point_field_goals_missed
)
SELECT
  match_id, away, FALSE, away_score, half_time_a,
  time_in_possession_a, all_runs_a, all_run_metres_a, post_contact_metres_a,
  line_breaks_a, tackle_breaks_a, average_set_distance_a, kick_return_metres_a,
  offloads_a, receipts_a, total_passes_a, dummy_passes_a, kicks_a, kicking_metres_a,
  forced_drop_outs_a, bombs_a, grubbers_a, forty_twenty_a, tackles_made_a,
  missed_tackles_a, intercepts_a, ineffective_tackles_a, errors_a,
  penalties_conceded_a, ruck_infringements_a, inside_ten_metres_a,
  interchanges_used_a, completion_rate_a, average_play_ball_speed_a,
  kick_defusal_a, effective_tackle_a, tries_a, conversions_a, conversions_missed_a,
  penalty_goals_a, penalty_goals_missed_a, sin_bins_a, on_reports_a,
  one_point_field_goals_a, one_point_field_goals_missed_a,
  two_point_field_goals_a, two_point_field_goals_missed_a
FROM matches;

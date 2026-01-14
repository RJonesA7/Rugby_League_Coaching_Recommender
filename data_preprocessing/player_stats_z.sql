-- Exactly the same data as player_stats, but taking z-scores of each stat
-- for each season to remove seasonal effects
CREATE TABLE player_stats_z (
  match_id integer NOT NULL REFERENCES match_data(match_id) ON DELETE CASCADE,

  -- Player identifiers
  name text NOT NULL,
  number int,
  position text,

  -- Time on field
  mins_played double precision,
  stint_one double precision,
  stint_two double precision,

  -- Scoring
  points double precision,
  tries double precision,
  conversions double precision,
  conversion_attempts double precision,
  penalty_goals double precision,
  goal_conversion_rate double precision,
  one_point_field_goals double precision,
  two_point_field_goals double precision,
  fantasy_points double precision,

  -- Running / metres
  all_runs double precision,
  all_run_metres double precision,
  hit_ups double precision,
  post_contact_metres double precision,
  kick_return_metres double precision,
  line_engaged_runs double precision,

  -- Attacking creation
  line_breaks double precision,
  line_break_assists double precision,
  try_assists double precision,
  tackle_breaks double precision,

  -- Ruck / handling / distribution
  play_the_ball double precision,
  average_play_the_ball_speed double precision,
  receipts double precision,
  passes double precision,
  dummy_passes double precision,
  offloads double precision,
  passes_to_run_ratio double precision,
  dummy_half_runs double precision,
  dummy_half_run_metres double precision,

  -- Defence
  tackles_made double precision,
  missed_tackles double precision,
  ineffective_tackles double precision,
  tackle_efficiency double precision,
  intercepts double precision,
  one_on_one_steal double precision,
  one_on_one_lost double precision,

  -- Errors / discipline
  errors double precision,
  handling_errors double precision,
  penalties double precision,
  ruck_infringements double precision,
  inside_10_metres double precision,
  on_report double precision,
  sin_bins double precision,
  send_offs double precision,

  -- Kicking
  kicks double precision,
  kicking_metres double precision,
  forced_drop_outs double precision,
  bomb_kicks double precision,
  grubbers double precision,
  forty_twenty double precision,
  twenty_forty double precision,
  cross_field_kicks double precision,
  kicked_dead double precision,
  kicks_defused double precision,

  -- A player should appear once per match
  PRIMARY KEY (match_id, name, number)
);

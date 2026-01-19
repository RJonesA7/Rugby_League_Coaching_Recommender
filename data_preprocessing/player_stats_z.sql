-- Exact replica of player_stats, but all stat columns stored as z-scores (double precision)
CREATE TABLE player_stats_z (
    -- Match identifiers
    season int,
    round int,
    home text,
    away text,
    match_key text, -- e.g. '2025-1-Raiders-v-Warriors', as found in JSONs
    match_id int REFERENCES matches(match_id),

    -- Player identifiers
    name text,
    number int,
    position text,

    -- Time on field (z-scored)
    mins_played double precision,
    stint_one double precision,
    stint_two double precision,

    -- Scoring (z-scored)
    points double precision,
    tries double precision,
    conversions double precision,
    conversion_attempts double precision,
    penalty_goals double precision,
    goal_conversion_rate double precision,
    one_point_field_goals double precision,
    two_point_field_goals double precision,
    fantasy_points double precision,

    -- Running / run metres (z-scored)
    all_runs double precision,
    all_run_metres double precision,
    hit_ups double precision,
    post_contact_metres double precision,
    kick_return_metres double precision,
    line_engaged_runs double precision,

    -- Attacking creation (z-scored)
    line_breaks double precision,
    line_break_assists double precision,
    try_assists double precision,
    tackle_breaks double precision,

    -- Ruck / handling / distribution (z-scored)
    play_the_ball double precision,
    average_play_the_ball_speed double precision,
    receipts double precision,
    passes double precision,
    dummy_passes double precision,
    offloads double precision,
    passes_to_run_ratio double precision,
    dummy_half_runs double precision,
    dummy_half_run_metres double precision,

    -- Defence (z-scored)
    tackles_made double precision,
    missed_tackles double precision,
    ineffective_tackles double precision,
    tackle_efficiency double precision,
    intercepts double precision,
    one_on_one_steal double precision,
    one_on_one_lost double precision,

    -- Discipline / errors (z-scored)
    errors double precision,
    handling_errors double precision,
    penalties double precision,
    ruck_infringements double precision,
    inside_10_metres double precision,
    on_report double precision,
    sin_bins double precision,
    send_offs double precision,

    -- Kicking (z-scored)
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

    player_match_id serial primary key
);

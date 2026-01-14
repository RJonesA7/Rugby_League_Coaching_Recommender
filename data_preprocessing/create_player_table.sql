CREATE TABLE player_stats (
    -- Match identifiers (temporary, for linking to match table)
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

    -- Time on field (store MM:SS as seconds)
    mins_played int,
    stint_one int,
    stint_two int,

    -- Scoring
    points int,
    tries int,
    conversions int,
    conversion_attempts int,
    penalty_goals int,
    goal_conversion_rate numeric(5,2),
    one_point_field_goals int,
    two_point_field_goals int,
    fantasy_points int,

    -- Running / run metres
    all_runs int,
    all_run_metres int,
    hit_ups int,
    post_contact_metres int,
    kick_return_metres int,
    line_engaged_runs int,

    -- Attacking creation
    line_breaks int,
    line_break_assists int,
    try_assists int,
    tackle_breaks int,

    -- Ruck / handling / distribution
    play_the_ball int,
    average_play_the_ball_speed numeric(5,2),
    receipts int,
    passes int,
    dummy_passes int,
    offloads int,
    passes_to_run_ratio numeric(6,3),
    dummy_half_runs int,
    dummy_half_run_metres int,

    -- Defence
    tackles_made int,
    missed_tackles int,
    ineffective_tackles int,
    tackle_efficiency numeric(5,2),
    intercepts int,
    one_on_one_steal int,
    one_on_one_lost int,

    -- Discipline / errors
    errors int,
    handling_errors int,
    penalties int,
    ruck_infringements int,
    inside_10_metres int,
    on_report int,
    sin_bins int,
    send_offs int,

    -- Kicking
    kicks int,
    kicking_metres int,
    forced_drop_outs int,
    bomb_kicks int,
    grubbers int,
    forty_twenty int,
    twenty_forty int,
    cross_field_kicks int,
    kicked_dead int,
    kicks_defused int,

    player_match_id serial primary key
);

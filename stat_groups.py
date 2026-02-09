# To negate collinearity, it can be useful to sum across groups of correlated statistics
# to make SHAP values more interpretable. This is a temporary fix rather than a
# replacement for full PCA.

# Each group now has a multiplier per stat:
#   +1 = "more of this is good" (desirable direction)
#   -1 = "more of this is bad"  (undesirable direction)

# This multiplier is a *presentation/interpretation*convention for grouped
# recommendations. It is not changing the underlying model.

STAT_GROUPS = {
    # Carry workload
    "workload_volume": {
        "all_runs": 1,
        "hit_ups": 1,
        "play_the_ball": 1,
        "line_engaged_runs": 1,
    },

    # Running metres
    "running_metres": {
        "all_run_metres": 1,
        "kick_return_metres": 1, #This often overlaps for backs so is hard to keep separate, but could be joined with kick_defusal or separated
    },

    "post_contact_effectiveness": {
        "post_contact_metres": 1,
        "tackle_breaks": 1,
        "offloads": 1,
    },

    # Ruck speed
    "ruck_speed": {
        "average_play_the_ball_speed": -1,
    },

    # Ball distribution involvement, both passing and number of times receiving the ball
    "distribution_involvement": {
        "receipts": 1,
        "passes": 1,
        "passes_to_run_ratio": 1,
    },

    # Dummy half running involvement
    "dummy_half_running": {
        "dummy_half_runs": 1,
        "dummy_half_run_metres": 1,
    },

    # Line breaks / chance creation
    "chance_creation": {
        "line_break_assists": 1,
        "try_assists": 1,
    },

    # Kicking for territory / game management (kicks kept with metres as requested)
    "territory_kicking": {
        "kicks": 1,
        "kicking_metres": 1,
        "forty_twenty": 1,
        "twenty_forty": 1,
    },

    # Volume of positive, attacking kicks on the opposition tryline. Often a mark of good defence, 
    # but this is a broadly positive metric for the halfback's individual play as it shows control in kicking on the final tackle and applying pressure
    "positive_attacking_kick_volume": {
        "forced_drop_outs": 1,
        "cross_field_kicks": 1,
        "grubbers": 1,
        "bomb_kicks": 1,
        "kicked_dead": -1,
    },

    # Kick defence / aerial management
    "aerial_kick_defence": {
        "kicks_defused": 1,
    },

    # Defence split as requested
    "defence_volume": {
        "tackles_made": 1,
    },
    "defence_quality": {
        "missed_tackles": -1,
        "ineffective_tackles": -1,
        "tackle_efficiency": 1,
    },

    # Defensive disruption / contests
    "defensive_events": {
        "intercepts": 1,
        "one_on_one_steal": 1,
        "one_on_one_lost": -1,
    },

    # Ball security
    "ball_security": {
        "errors": -1,
        "handling_errors": -1,
    },

    # Discipline / infringements
    "discipline": {
        "penalties": -1,
        "on_report": -1,
        "sin_bins": -1,
        "send_offs": -1,
    },

    "technical_discipline": {
        "inside_10_metres": -1,
        "ruck_infringements": -1,
    },

    # Scoring proxies
    "scoring_proxies": {
        "line_breaks": 1,
        "points": 1,
        "tries": 1,
        "conversions": 1,
        "conversion_attempts": 1,
        "penalty_goals": 1,
        "goal_conversion_rate": 1,
        "one_point_field_goals": 1,
        "two_point_field_goals": 1,
    },
}

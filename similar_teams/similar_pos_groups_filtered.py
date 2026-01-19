import pandas
import numpy as np
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

position_keep_stats = {
    "middles": [
        "mins_played",
        "hit_ups",
        "all_runs",
        "all_run_metres",
        "post_contact_metres",
        "line_engaged_runs",
        "average_play_the_ball_speed",
        "tackles_made",
        "tackle_efficiency",
        "passes",
    ],
    "hooker": [
        "mins_played",
        "receipts",
        "passes",
        "passes_to_run_ratio",
        "dummy_half_runs",
        "dummy_half_run_metres",
        "average_play_the_ball_speed",
        "tackles_made",
        "tackle_efficiency",
    ],
    "fullback": [
        "mins_played",
        "kick_return_metres",
        "all_run_metres",
        "line_breaks",
        "line_break_assists",
        "tackle_breaks",
        "kicking_metres",
        "errors",
        "tackle_efficiency",
    ],
    "second_rows": [
        "mins_played",
        "line_engaged_runs",
        "all_run_metres",
        "post_contact_metres",
        "offloads",
        "tackle_breaks",
        "line_breaks",
        "tackles_made",
        "tackle_efficiency",
        "errors",
    ],
    "halves": [
        "mins_played",
        "passes",
        "passes_to_run_ratio",
        "try_assists",
        "line_break_assists",
        "kicks",
        "kicking_metres",
        "forced_drop_outs",
        "errors",
    ],
    "centres": [
        "mins_played",
        "all_run_metres",
        "post_contact_metres",
        "line_breaks",
        "try_assists",
        "tackle_breaks",
        "offloads",
        "intercepts",
        "missed_tackles",
        "errors",
    ],
    "wingers": [
        "mins_played",
        "kick_return_metres",
        "all_run_metres",
        "tackle_breaks",
        "line_breaks",
        "tries",
        "kicks_defused",
        "handling_errors",
        "missed_tackles",
        "errors",
    ],
}


"""
Replica of similar_teams_z_sum_filtered, but operating on position groups rather than whole teams

stats_dict represents an opposition position group such as middles, halves, wingerss
"""

def similar_pos_groups_filtered(stats_dict, position_group, num_of_matches):
    # Pass 1: reject any unexpected stats
    FULL_STATS = [
        # Time on field
        "mins_played", "stint_one", "stint_two",

        # Scoring
        "points", "tries", "conversions", "conversion_attempts", "penalty_goals",
        "goal_conversion_rate", "one_point_field_goals", "two_point_field_goals", "fantasy_points",

        # Running / metres
        "all_runs", "all_run_metres", "hit_ups", "post_contact_metres", "kick_return_metres", "line_engaged_runs",

        # Attacking creation
        "line_breaks", "line_break_assists", "try_assists", "tackle_breaks",

        # Ruck / handling / distribution
        "play_the_ball", "average_play_the_ball_speed", "receipts", "passes", "dummy_passes", "offloads",
        "passes_to_run_ratio", "dummy_half_runs", "dummy_half_run_metres",

        # Defence
        "tackles_made", "missed_tackles", "ineffective_tackles", "tackle_efficiency", "intercepts",
        "one_on_one_steal", "one_on_one_lost",

        # Discipline / errors
        "errors", "handling_errors", "penalties", "ruck_infringements", "inside_10_metres",
        "on_report", "sin_bins", "send_offs",

        # Kicking
        "kicks", "kicking_metres", "forced_drop_outs", "bomb_kicks", "grubbers", "forty_twenty", "twenty_forty",
        "cross_field_kicks", "kicked_dead", "kicks_defused",
    ]

    for key in stats_dict.keys():
        if key not in FULL_STATS:
            raise ValueError("invalid statistic " + key)

    # Pass 2: Filter to stats correlated with victory for use in KNN. These are different for different positions
    keep_stats = position_keep_stats[position_group]
    stats_dict = {k: v for k, v in stats_dict.items() if k in keep_stats}

    if len(stats_dict) == 0:
        raise ValueError("no stats left after filtering to keep_stats")

    # Get all database units for that position_group
    stats_select_query = """
    SELECT match_id, is_home, {columns}
    FROM position_group_stats_z
    WHERE position_group = %(position_group)s
    """.format(
        columns=", ".join(stats_dict.keys()),
        not_nulls=" AND ".join([c + " IS NOT NULL" for c in stats_dict.keys()])
    )

    stat_df = pandas.read_sql_query(stats_select_query, conn, params={"position_group": position_group})

    stat_df = stat_df.set_index(["match_id", "is_home"])

    # Absolute z-score difference, sum across chosen dimensions
    stat_df = abs(stat_df - pandas.Series(stats_dict))
    stat_df["z_sum"] = stat_df.sum(axis=1)
    stat_df = stat_df.sort_values(by="z_sum")

    # Return the z_sum series of closest matches
    stat_df = stat_df["z_sum"]
    similar_matches = stat_df.head(num_of_matches)

    return similar_matches

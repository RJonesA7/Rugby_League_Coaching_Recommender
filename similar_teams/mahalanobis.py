import pandas
import numpy as np
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

def similar_teams_mahalanobis_filtered(stats_dict, num_of_matches):
    
    for key in stats_dict.keys():
        if key not in ["score", "half_time", "time_in_possession",
            "all_runs", "all_run_metres", "post_contact_metres",
            "line_breaks", "tackle_breaks", "average_set_distance",
            "kick_return_metres", "offloads", "receipts",
            "total_passes", "dummy_passes", "kicks", "kicking_metres",
            "forced_drop_outs", "bombs", "grubbers", "forty_twenty",
            "tackles_made", "missed_tackles", "intercepts", "ineffective_tackles",
            "errors", "penalties_conceded", "ruck_infringements", "inside_ten_metres",
            "interchanges_used", "completion_rate", "average_play_ball_speed",
            "kick_defusal", "effective_tackle", "tries", "conversions",
            "conversions_missed", "penalty_goals", "penalty_goals_missed", "sin_bins", "on_reports"
        ]:
            raise ValueError("invalid statistic " + key)

    # Feature subset to capture all the variance and dimensions without excessive noise
    keep_stats = [
        "all_run_metres",            # territory / volume
        "post_contact_metres",       # physical dominance
        "offloads",                 # expansiveness
        "total_passes",             # structure vs direct play
        "kicking_metres",           # territorial strategy
        "line_breaks",              # attacking effectiveness
        "completion_rate",          # control / discipline
        "average_play_ball_speed",  # tempo
        "penalties_conceded",       # discipline
        "errors",                   # discipline (but a different kind)
        "missed_tackles",           # defensive weakness
        "inside_ten_metres"         # attacking pressure / field position
    ]
    
    stats_dict = {k: v for k, v in stats_dict.items() if k in keep_stats}
    cols = list(stats_dict.keys())

    # Pull data
    stats_select_query = """
    select match_id, is_home, {columns}
    from team_stats_z
    where {not_nulls}
    """.format(
        columns=", ".join(cols),
        not_nulls=" is not null and ".join(cols) + " is not null"
    )

    stat_df = pandas.read_sql_query(stats_select_query, conn)
    stat_df = stat_df.set_index(["match_id", "is_home"])

    # --- MAHALANOBIS PART ---

    # Convert to matrix
    X = stat_df[cols].values

    # Covariance matrix and inverse
    cov_matrix = np.cov(X, rowvar=False)
    inv_cov_matrix = np.linalg.inv(cov_matrix)

    # Input vector
    x0 = np.array([stats_dict[col] for col in cols])

    # Compute Mahalanobis distance for each row
    diffs = X - x0
    left = np.dot(diffs, inv_cov_matrix)
    mahal = np.sqrt(np.sum(left * diffs, axis=1))

    # Store results (using "z_sum" because that is the variable name used throughout the system now. Update when possible)
    stat_df['z_sum'] = mahal
    stat_df = stat_df.sort_values(by='z_sum')

    similar_matches = stat_df['z_sum'].head(num_of_matches)

    return similar_matches
import pandas
import numpy as np
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

"""
Function which takes in a stat_dict representing an opposition team and returns a dataframe with the most similar teams in the database,
and a total z-score difference show how similar they are.
The only difference to the base model is that not all stats are considered, only those which are either correlated with victory, 
as found in the base model, and considered useful statistics by domain knowledge. FIrst iteration uses principally domain knowledge
"""

def similar_teams_z_sum_filtered(stats_dict, num_of_matches):
    for key in stats_dict.keys():
        if key not in ["score",  "half_time",  "time_in_possession",
            "all_runs",  "all_run_metres",  "post_contact_metres",
            "line_breaks",  "tackle_breaks",  "average_set_distance",
            "kick_return_metres",  "offloads",  "receipts",
            "total_passes",  "dummy_passes",  "kicks",  "kicking_metres",
            "forced_drop_outs",  "bombs",  "grubbers",  "forty_twenty",
            "tackles_made",  "missed_tackles",  "intercepts",  "ineffective_tackles",
            "errors",  "penalties_conceded",  "ruck_infringements",  "inside_ten_metres",
            "interchanges_used",  "completion_rate",  "average_play_ball_speed",
            "kick_defusal",  "effective_tackle",  "tries",  "conversions",
            "conversions_missed",  "penalty_goals",  "penalty_goals_missed",  "sin_bins",  "on_reports"
            ]:
            raise ValueError("invalid statistic " + key)

    #Filter the stats to include only those we want, minimising dimensionality to improve effectiveness of KNN. This set can be freely updated
    keep_stats = [
        "all_run_metres", "post_contact_metres", "line_breaks", "offloads", 
        "total_passes", "kicking_metres", "penalties_conceded", "effective_tackle", "completion_rate", "average_play_ball_speed"
    ]
    stats_dict = {k: v for k, v in stats_dict.items() if k in keep_stats}
    
    #Get all database teams
    stats_select_query = """
    select match_id, is_home, {columns}
    from team_stats_z
    where {not_nulls}
    """.format(columns=", ".join(stats_dict.keys()), not_nulls = " is not null and ".join(stats_dict.keys()) + " is not null")
    stat_df = pandas.read_sql_query(stats_select_query, conn)

    #Flip is_home (for the functions we will pass this to), since we are interested in teams playing against similar teams found
    #Join the match_id and home_away columns to have a unique identifier, and set this as the index
    stat_df = stat_df.set_index(["match_id", "is_home"])

    """
    Subtract the manufactured column to get a z-score difference, 
    make another column which is the difference sum, 
    and take those with the lowest difference sum
    """
    stat_df = abs(stat_df - pandas.Series(stats_dict))
    stat_df['z_sum'] = stat_df.sum(axis=1)
    stat_df = stat_df.sort_values(by = 'z_sum')

    #We now only need the z_sum
    stat_df = stat_df['z_sum']
    similar_matches = stat_df.head(num_of_matches)

    return(similar_matches)

#teams = similar_teams_z_sum_filtered({"time_in_possession": 1, "average_play_ball_speed": -1, "completion_rate": 1}, 10)

from similar_teams.similar_teams_z_sum_filtered import similar_teams_z_sum_filtered
from effective_stats.pcr import principal_component_regression, pcr_shap_reconstructed, principal_component_regression_65
from effective_stats.svc_scikit import svc_scikit
from evaluation_metrics.spearman_cor import spearman_cor

import pandas, numpy
import psycopg2

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

import pandas as pd
import numpy as np
from sklearn.decomposition import PCA


def build_principal_components(opposition_sides, conn):

    # --------------------------------------------------
    # Build SQL conditions
    # --------------------------------------------------

    keys = list(opposition_sides.index)
    keys = [list(k) for k in keys]

    conditions = "".join(
        f"(team_stats_z.match_id = {k[0]} and team_stats_z.is_home = {k[1]}) or "
        for k in keys
    )[:-4]

    query = f"""
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z
    join team_stats_z opp_stats_z
      on team_stats_z.match_id = opp_stats_z.match_id
     and team_stats_z.is_home != opp_stats_z.is_home
    where {conditions}
    """

    # --------------------------------------------------
    # Load stats
    # --------------------------------------------------

    stat_df = pd.read_sql_query(query, conn)
    stat_df = stat_df.set_index(["match_id", "is_home"])

    stat_df = stat_df.reset_index()

    # --------------------------------------------------
    # Build feature matrix
    # --------------------------------------------------

    X = stat_df.drop(columns=[
        "team",
        "is_home",
        "match_id",
        "final_margin"
    ])

    X = X.fillna(0)
    X = X.loc[:, X.nunique() > 1]

    X = X.drop(columns=[
        "score",
        "half_time",
        "tries",
        "conversions",
        "conversions_missed"
    ], errors="ignore")

    feature_cols = list(X.columns)

    # --------------------------------------------------
    # Fit PCA
    # --------------------------------------------------

    pca = PCA()
    Z = pca.fit_transform(X)

    pc_cols = [f"PC{i+1}" for i in range(Z.shape[1])]

    Z_df = pd.DataFrame(Z, columns=pc_cols, index=stat_df.index)

    # --------------------------------------------------
    # Attach metadata for later use
    # --------------------------------------------------

    Z_df["match_id"] = stat_df["match_id"]
    Z_df["is_home"] = stat_df["is_home"]
    Z_df["final_margin"] = stat_df["final_margin"]

    return pca, Z_df, feature_cols, pc_cols

opposition_representation = {
  "score": 0,
  "half_time": 0,
  "time_in_possession": 0,
  "all_runs": 0,
  "all_run_metres": 0,
  "post_contact_metres": 0,
  "line_breaks": 0,
  "tackle_breaks": 0,
  "average_set_distance": 0,
  "kick_return_metres": 0,
  "offloads": 0,
  "receipts": 0,
  "total_passes": 0,
  "dummy_passes": 0,
  "kicks": 0,
  "kicking_metres": 0,
  "forced_drop_outs": 0,
  "bombs": 0,
  "grubbers": 0,
  #"forty_twenty": 0,
  "tackles_made": 0,
  "missed_tackles": 0,
  "intercepts": 0,
  "ineffective_tackles": 0,
  "errors": 0,
  "penalties_conceded": 0,
  "ruck_infringements": 0,
  "inside_ten_metres": 0,
  "interchanges_used": 0,
  "completion_rate": 0,
  "average_play_ball_speed": 0,
  "kick_defusal": 0,
  "effective_tackle": 0,
  "tries": 0,
  "conversions": 0,
  "conversions_missed": 0,
  "penalty_goals": 0,
  "penalty_goals_missed": 0,
  "sin_bins": 0,
  "on_reports": 0,
  #"one_point_field_goals": 0,
  #"one_point_field_goals_missed": 0,
  #"two_point_field_goals": 0,
  #"two_point_field_goals_missed": 0,
}




opposition_sides = similar_teams_z_sum_filtered({**opposition_representation, "missed_tackles": -1, "completion_rate": 1, "kicking_metres": 1, "post_contact_metres": 1, "total_passes": -1}, 8000)
opposition_sides = similar_teams_z_sum_filtered(opposition_representation, 8000)

#Build the PCs, to ensure they are consistent across the folds
pca, Z_df, feature_cols, pc_cols = build_principal_components(
    opposition_sides,
    conn
)

# Split opposition_sides for K-Fold validation
opposition_sides_chunks = numpy.array_split(opposition_sides.to_frame(), 5)

tot_avg = None          # dict: feature -> float
tot_box = None          # dict: feature -> list[6]
for i in range(0, 5):
    curr_chunk = opposition_sides_chunks[i].copy()
    other_chunks = pandas.concat([opposition_sides_chunks[j] for j in range(0, 5) if j != i])

    curr_chunk["validation"] = True
    other_chunks["validation"] = False
    curr_input = pandas.concat([curr_chunk, other_chunks])

    #fold_res = principal_component_regression(curr_input)
    fold_res = principal_component_regression(
        curr_input,
        Z_df,
        pc_cols
    )
    # fold_res: feature -> [mean_shap, [min, q1, mean, q3, max]]

    # Initialise accumulators on first fold
    if tot_avg is None:
        tot_avg = {feat: fold_res[feat][0] for feat in fold_res}
        tot_box = {feat: fold_res[feat][1][:] for feat in fold_res}  # copy list
    else:
        for feat in fold_res:
            tot_avg[feat] += fold_res[feat][0]
            # elementwise add stats list
            for k in range(5):
                tot_box[feat][k] += fold_res[feat][1][k]

# Average across folds
for feat in tot_avg:
    tot_avg[feat] /= 5.0
    for k in range(5):
        tot_box[feat][k] /= 5.0

# Keep separate for now
# tot_avg: feature -> avg mean SHAP across folds
# tot_box: feature -> [avg min, avg q1, avg mean, avg q3, avg max] across folds


print(tot_avg)



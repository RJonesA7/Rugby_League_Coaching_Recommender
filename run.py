from similar_teams.similar_teams_z_sum import similar_teams_z_sum
from similar_teams.similar_teams_z_sum_filtered import similar_teams_z_sum_filtered
from effective_stats.first_model_regression import first_model_regression
from effective_stats.first_model_no_scipy import first_model_no_scipy
from effective_stats.multilinear_regression import multilinear_regression
from effective_stats.svc_scikit import svc_scikit
from evaluation_metrics.spearman_cor import spearman_cor

import pandas, numpy
import psycopg2

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

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

#Split opposition_sides for K-Fold validation to get SHAP values
opposition_sides_chunks = numpy.array_split(opposition_sides.to_frame(), 5)

tot_res = None
for i in range(0,5):
  curr_chunk = opposition_sides_chunks[i]
  other_chunks = pandas.concat([opposition_sides_chunks[j] for j in range(0,5) if j != i])
  curr_chunk["validation"] = True
  other_chunks["validation"] = False
  curr_input = pandas.concat([curr_chunk, other_chunks])
  if tot_res is None:
    tot_res =  multilinear_regression(curr_input)
  else:
    tot_res = tot_res + multilinear_regression(curr_input)

tot_res = tot_res / 5
print(tot_res)



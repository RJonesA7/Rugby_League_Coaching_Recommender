import pandas as pd
import numpy as np

from similar_teams.similar_teams_z_sum_filtered import similar_teams_z_sum_filtered
from effective_stats.multilinear_regression import multilinear_regression
from effective_stats.random_forest import rf_scikit
from effective_stats.svc_scikit import svc_scikit

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

def _kfold_mean_accuracy(opposition_sides: pd.Series, model_fn, folds: int = 5) -> float:
    """
    Splits into folds, for each fold marks validation True for that fold, False otherwise. Then tests and averages across folds
    """
    # ensure a Series with an index (match_id, is_home) etc
    if isinstance(opposition_sides, pd.DataFrame):
        # if the Series was passed as a 1-col dataframe, squeeze it
        if opposition_sides.shape[1] == 1:
            opposition_sides = opposition_sides.iloc[:, 0]
        else:
            raise ValueError("opposition_sides must be a Series or 1-column DataFrame.")

    chunks = np.array_split(opposition_sides.to_frame(), folds)

    tot_res = 0.0
    for i in range(folds):
        curr_chunk = chunks[i].copy()
        other_chunks = pd.concat([chunks[j] for j in range(folds) if j != i], axis=0)

        curr_chunk["validation"] = True
        other_chunks["validation"] = False

        curr_input = pd.concat([curr_chunk, other_chunks], axis=0)

        tot_res += float(model_fn(curr_input))

    return tot_res / folds


def compare_svc_vs_regression(
    opposition_representations: dict,
    k_values=(200, 400, 800, 1200),
    folds: int = 5,
    include_models=("rf", "svc", "regression"),
) -> pd.DataFrame:
    """
    Parameters:
    opposition_representations:
        dict[str, dict] mapping a name -> opposition_representation dict (like yours).
        Example: {"all_zero": opposition_representation, "high_errors": {...}}
    k_values:
        iterable of ints - number of oppositions to select (passed to similar_teams_z_sum_filtered(..., k)).
    folds:
        number of folds to use for validation
    include_models:
        which of the so far implemented models to include in the comparison

    Returns:
    DataFrame with one row per (representation_name, k).
    Columns include svc_accuracy and regression_accuracy depending on include_models.

    Can also be run with return values changed to RMSE rather than accuracy.
    """
    rows = []

    # map model keys to the actual functions
    model_map = {
        "svc": svc_scikit,
        "regression": multilinear_regression,
        "rf": rf_scikit,
    }

    for rep_name, rep_dict in opposition_representations.items():
        for k in k_values:
            opposition_sides = similar_teams_z_sum_filtered(rep_dict, int(k))

            row = {
                "representation": rep_name,
                "k": int(k),
                "n_selected": int(len(opposition_sides)),
                "folds": int(folds),
            }

            if "svc" in include_models:
                row["svc_accuracy"] = _kfold_mean_accuracy(opposition_sides, model_map["svc"], folds=folds)
            
            if "rf" in include_models:
                row["rf_accuracy"] = _kfold_mean_accuracy(opposition_sides, model_map["rf"], folds=folds)

            if "regression" in include_models:
                row["regression_accuracy"] = _kfold_mean_accuracy(opposition_sides, model_map["regression"], folds=folds)

            if ("rf" in include_models) and ("regression" in include_models):
                row["rf_minus_regression"] = row["rf_accuracy"] - row["regression_accuracy"]

            if ("svc" in include_models) and ("regression" in include_models):
                row["svc_minus_regression"] = row["svc_accuracy"] - row["regression_accuracy"]

            rows.append(row)

    return pd.DataFrame(rows).sort_values(["representation", "k"]).reset_index(drop=True)

# Make a set of randomised opposition teams, z-scores between -2 and 2, to test accuracy.

EXCLUDED_STATS = {
    "score",
    "half_time",
    "tries",
    "conversions",
    "conversions_missed",
}

opposition_representations = {}

for i in range(10):
    rep = {}

    for stat in opposition_representation.keys():
        if stat in EXCLUDED_STATS:
            rep[stat] = 0.0
        else:
            rep[stat] = float(np.random.uniform(-2, 2))

    opposition_representations[f"random_{i+1}"] = rep

df = compare_svc_vs_regression(opposition_representations)
print(df)

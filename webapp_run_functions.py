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


# Put into function to run with webapp
def multilinear_regression_run(oppo_rep):
  opposition_sides = similar_teams_z_sum_filtered(oppo_rep, 1000)

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

  return tot_res



from similar_teams.similar_pos_groups_filtered import similar_pos_groups_filtered
from effective_stats.player_level_multi_regression import player_level_multi_regression
from effective_stats.static_data.matchup_graph import MATCHUP_WEIGHTS
from stat_groups import STAT_GROUPS

positions = [
    "middles",
    "hooker",
    "second_rows",
    "halves",
    "centres",
    "wingers",
    "fullback",
]

def player_level_multi_regression_run(oppo_rep):
    results_by_position = {our_pos: {} for our_pos in positions}

    for opp_pos in positions:

        opposition_players = similar_pos_groups_filtered(oppo_rep[opp_pos], opp_pos, 1000)
        print(f"\nOpposition group: {opp_pos}  (n={len(opposition_players)})")

        # Split once per opposition group (same folds reused for all our_pos)
        chunks = numpy.array_split(opposition_players.to_frame(), 5)

        for our_pos in positions:
            tot_res = None

            for i in range(5):
                curr_chunk = chunks[i].copy()
                other_chunks = pandas.concat([chunks[j] for j in range(5) if j != i])

                curr_chunk["validation"] = True
                other_chunks["validation"] = False

                curr_input = pandas.concat([curr_chunk, other_chunks])

                res = player_level_multi_regression(curr_input, our_pos)
                tot_res = res if tot_res is None else tot_res + res

            results_by_position[our_pos][opp_pos] = tot_res


    final_recs = {}

    for our_pos, opp_dict in results_by_position.items():
        total = None

        for opp_pos, rec in opp_dict.items():
            w = MATCHUP_WEIGHTS[our_pos][opp_pos] / 100.0
            contrib = rec * w

            total = contrib if total is None else total.add(contrib, fill_value=0)

        final_recs[our_pos] = total

    # To negate feature collinearity, sum across groups of features which come under a similar umbrella

    EXCLUDE_GROUPS = {}

    grouped_final_recs = {}

    for pos, shap_series in final_recs.items():  # final_recs[pos] is a pandas Series
        s = shap_series.copy().fillna(0.0)
        grouped = {}

        for gname, feats in STAT_GROUPS.items():
            if gname in EXCLUDE_GROUPS:
                continue

            # feats is now {feature_name: multiplier}
            feat_names = list(feats.keys())
            multipliers = pandas.Series(feats)

            # align SHAP values, apply directionality, sum
            group_value = (
                s.reindex(feat_names)
                .fillna(0.0)
                .mul(multipliers)
                .sum()
            )

            grouped[gname] = float(group_value)

        grouped_final_recs[pos] = pandas.Series(grouped).sort_values(ascending=False)
    return grouped_final_recs


    



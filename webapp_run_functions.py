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

        fold_res = multilinear_regression(curr_input)
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
    return tot_avg, tot_box



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

def _add_feature_outputs(a, b):
    """
    a and b: dict feature -> [avg, [min,q1,avg,q3,max]]
    returns elementwise sum (not averaged)
    """
    if a is None:
        # copy b
        return {k: [float(v[0]), [float(x) for x in v[1]]] for k, v in b.items()}

    for feat, (avg, stats) in b.items():
        if feat not in a:
            a[feat] = [0.0, [0.0, 0.0, 0.0, 0.0, 0.0]]
        a[feat][0] += float(avg)
        for i in range(5):
            a[feat][1][i] += float(stats[i])
    return a


def _scale_feature_outputs(d, w):
    """
    Multiply dict feature -> [avg, stats] by scalar w.
    """
    out = {}
    for feat, (avg, stats) in d.items():
        out[feat] = [float(avg) * w, [float(x) * w for x in stats]]
    return out


def _combine_feature_outputs_sum(outputs):
    """
    outputs: list of dict feature -> [avg, stats]
    returns sum across dicts
    """
    total = None
    for d in outputs:
        total = _add_feature_outputs(total, d)
    return total


def _transform_stats_for_multiplier(stats, m):
    """
    stats = [min, q1, mean, q3, max] for a feature.
    Multiply by m, and if m<0 swap endpoints and quartiles to preserve ordering.
    """
    mn, q1, mean, q3, mx = stats
    if m >= 0:
        return [m * mn, m * q1, m * mean, m * q3, m * mx]
    else:
        # multiplying by negative flips order
        return [m * mx, m * q3, m * mean, m * q1, m * mn]


def player_level_multi_regression_run(oppo_rep):
    # Store per our_pos, per opp_pos: dict feature -> [avg, stats] (already K-fold averaged)
    results_by_position = {our_pos: {} for our_pos in positions}

    for opp_pos in positions:
        opposition_players = similar_pos_groups_filtered(oppo_rep[opp_pos], opp_pos, 1000)
        print(f"\nOpposition group: {opp_pos}  (n={len(opposition_players)})")

        chunks = numpy.array_split(opposition_players.to_frame(), 5)

        for our_pos in positions:
            tot_fold = None

            for i in range(5):
                curr_chunk = chunks[i].copy()
                other_chunks = pandas.concat([chunks[j] for j in range(5) if j != i])

                curr_chunk["validation"] = True
                other_chunks["validation"] = False
                curr_input = pandas.concat([curr_chunk, other_chunks])

                # res: dict feature -> [avg, [min,q1,avg,q3,max]]
                res = player_level_multi_regression(curr_input, our_pos)
                tot_fold = _add_feature_outputs(tot_fold, res)

            # average across folds
            for feat in tot_fold:
                tot_fold[feat][0] /= 5.0
                for k in range(5):
                    tot_fold[feat][1][k] /= 5.0

            results_by_position[our_pos][opp_pos] = tot_fold

    # Combine across opposition groups with matchup weights (still feature-level)
    final_recs = {}
    final_box = {}

    for our_pos, opp_dict in results_by_position.items():
        # sum w * per-opp_pos dicts
        total = None
        for opp_pos, rec_dict in opp_dict.items():
            w = MATCHUP_WEIGHTS[our_pos][opp_pos] / 100.0
            contrib = _scale_feature_outputs(rec_dict, w)
            total = _add_feature_outputs(total, contrib)

        final_recs[our_pos] = total  # dict feature -> [avg, stats]
        final_box[our_pos] = total   # same object shape; kept separate for clarity

    # Grouping (produce grouped averages Series as before, and grouped box data)
    EXCLUDE_GROUPS = {}

    grouped_final_recs = {}
    box_plot_grouped_final_recs = {}

    for pos, feat_dict in final_recs.items():
        # feat_dict: feature -> [avg, stats]
        # Build Series of feature avg values
        feat_avg = pandas.Series({f: v[0] for f, v in feat_dict.items()}).fillna(0.0)

        grouped_avg = {}
        grouped_box = {}

        for gname, feats in STAT_GROUPS.items():
            if gname in EXCLUDE_GROUPS:
                continue

            # feats is {feature_name: multiplier}
            group_avg_val = 0.0
            group_stats = [0.0, 0.0, 0.0, 0.0, 0.0]  # [min,q1,mean,q3,max]

            for feature_name, mult in feats.items():
                # avg contribution
                v_avg = float(feat_avg.get(feature_name, 0.0))
                group_avg_val += v_avg * float(mult)

                # box stats contribution (approx, linear)
                if feature_name in feat_dict:
                    stats = feat_dict[feature_name][1]  # [min,q1,mean,q3,max]
                    tstats = _transform_stats_for_multiplier(stats, float(mult))
                    for k in range(5):
                        group_stats[k] += float(tstats[k])
                else:
                    # missing feature -> contributes 0
                    pass

            grouped_avg[gname] = float(group_avg_val)

            # Output format: [avg, [min, LQ, avg, UQ, max]] (avg repeated)
            # Here avg should match the grouped mean (group_stats[2]) if everything is consistent.
            g_avg = grouped_avg[gname]
            grouped_box[gname] = [
                g_avg,
                [
                    float(group_stats[0]),
                    float(group_stats[1]),
                    float(g_avg),
                    float(group_stats[3]),
                    float(group_stats[4]),
                ],
            ]

        grouped_final_recs[pos] = pandas.Series(grouped_avg).sort_values(ascending=False)
        # Keep the box plot data ordered the same way as the grouped series:
        ordered_groups = list(grouped_final_recs[pos].index)
        box_plot_grouped_final_recs[pos] = {g: grouped_box[g] for g in ordered_groups}

    return grouped_final_recs, box_plot_grouped_final_recs

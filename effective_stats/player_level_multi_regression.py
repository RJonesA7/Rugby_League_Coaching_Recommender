import pandas, numpy
import psycopg2
from effective_stats.static_data.positional_target_stats import TARGET_STATS_WEIGHTS

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

# Stats to exclude from the feature space for each position group
# These are stats that are structurally irrelevant for recommendations for that role
# Fantasy points are excluded everywhere because they are damaging to SHAP values, since they correlate with pretty much every positive stat
position_drop_stats = {
    "middles": [
        "kicks",
        "kicking_metres",
        "forced_drop_outs",
        "bomb_kicks",
        "grubbers",
        "forty_twenty",
        "twenty_forty",
        "cross_field_kicks",
        "kicked_dead",
        "dummy_half_runs",
        "dummy_half_run_metres",
        "dummy_passes",
        "fantasy_points",
    ],

    "second_rows": [
        "kicks",
        "kicking_metres",
        "forced_drop_outs",
        "bomb_kicks",
        "grubbers",
        "forty_twenty",
        "twenty_forty",
        "cross_field_kicks",
        "kicked_dead",
        "dummy_half_runs",
        "dummy_half_run_metres",
        "dummy_passes",
        "fantasy_points",
    ],

    "hooker": [
        "fantasy_points",
    ],

    "fullback": [
        "fantasy_points",
    ],

    "halves": [
        "dummy_half_runs",
        "dummy_half_run_metres",
        "dummy_passes",
        "fantasy_points",
    ],

    "centres": [
        "dummy_half_runs",
        "dummy_half_run_metres",
        "dummy_passes",
        "fantasy_points",
    ],

    "wingers": [
        "dummy_half_runs",
        "dummy_half_run_metres",
        "dummy_passes",
        "fantasy_points",
    ],
}


"""
Fits a weighted linear model for a given position group and returns
average feature contributions for high-impact cases.

weights is indexed by (match_id, is_home) and contains similarity scores
and a validation flag indicating fold membership.

position_group specifies the unit for which recommendations are generated.
"""

def player_level_multi_regression(weights, position_group):
    keys = list(weights.index)
    keys = [list(k) for k in keys]

    TABLE = "position_group_stats_z"

    # Build a match-level filter from the supplied keys
    conditions = ["({t}.match_id = {mid} and {t}.is_home = {ih} and position_group = '{pg}') or ".format(
        pg=position_group,
        t=TABLE,
        mid=key[0],
        ih=str(key[1]).lower() if isinstance(key[1], bool) else key[1]
    ) for key in keys]
    conditions = "".join(conditions)[:-4]

    # Load all rows for the requested position group in the selected matches
    stats_select_query = """
    select {t}.*
    from {t}
    where {conds} and {t}.position_group = %(pg)s
    """.format(t=TABLE, conds=conditions)

    stat_df = pandas.read_sql_query(stats_select_query, conn, params={"pg": position_group})
    stat_df = stat_df.set_index(["match_id", "is_home"])
    stat_df = stat_df.fillna(0)

    # Remove position-irrelevant stats from the feature space
    stat_df.drop(columns=position_drop_stats[position_group], errors="ignore", inplace=True)

    # Convert similarity distance into training weights and attach validation flags
    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    stat_df["weight"] = weights["z_sum"]
    stat_df["validation"] = weights["validation"]

    # Split into training and evaluation sets
    eval_stats = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    stat_df = stat_df.reset_index()

    # Construct the regression target as a weighted combination of selected stats
    tmap = TARGET_STATS_WEIGHTS[position_group]

    for col in tmap.keys():
        if col not in stat_df.columns:
            stat_df[col] = 0

    stat_df["target"] = 0
    for col, wgt in tmap.items():
        stat_df["target"] = stat_df["target"] + wgt * stat_df[col]

    # Assemble the feature matrix
    drop_cols = ["match_id", "is_home", "position_group", "weight", "target",
                 "season", "home", "away", "round"]
    X = stat_df.drop(columns=[c for c in drop_cols if c in stat_df.columns])

    y = stat_df["target"]
    w = stat_df["weight"]

    X = X.fillna(0)

    # Remove constant columns
    X = X.loc[:, X.nunique() > 1]

    # Exclude any stats used directly in the target definition
    X = X.drop(columns=[c for c in tmap.keys() if c in X.columns], errors="ignore")

    # Add intercept term
    X["const"] = 1

    # Clean and stabilise weights
    w = w.replace([numpy.inf, -numpy.inf], numpy.nan).fillna(0.0)
    w = w.clip(lower=0.0, upper=numpy.nanquantile(w, 0.99))

    # Apply weighted least squares via square root scaling
    w = w ** (1 / 2)
    y_w = y * w
    X_w = X.mul(w, axis=0)

    sol = pandas.Series(
        numpy.linalg.lstsq(X_w.to_numpy(), y_w.to_numpy(), rcond=None)[0],
        index=X.columns
    )

    # Evaluate prediction error on the remaining fold
    mse = 0.0
    predictions = []

    eval_stats["target"] = 0
    for col, wgt in tmap.items():
        eval_stats["target"] = eval_stats["target"] + wgt * eval_stats[col]

    test_data = (
        eval_stats
        .drop(columns=[c for c in ["weight"] if c in eval_stats.columns])
        .fillna(0)
        .copy()
    )

    feature_cols = [c for c in X.columns if c != "const"]
    for col in feature_cols:
        if col not in test_data.columns:
            test_data[col] = 0

    test_X = test_data.reindex(columns=feature_cols, fill_value=0).copy()
    test_X["const"] = 1
    test_X = test_X.reindex(columns=X.columns)

    for idx, row in test_data.iterrows():
        res = row["target"]
        vec = test_X.loc[idx]
        pred = float((sol * vec).sum())

        mse += (pred - res) ** 2
        predictions.append((pred, idx))

    mse = mse / len(test_data)

    # Compute average feature contributions for the top quartile of predictions
    predictions.sort(key=lambda x: x[0])
    num_preds = max(int(len(predictions) * 0.25), 1)

    top_ids = [p[1] for p in predictions[-num_preds:]]
    shap_rows = test_data.loc[test_data.index.intersection(top_ids)].copy()

    shap_X = shap_rows.reindex(columns=feature_cols, fill_value=0).copy()
    shap_X["const"] = 1
    shap_X = shap_X.reindex(columns=X.columns)

    expected_vals = X.mean()

    total_shap_vals = sol.copy()
    total_shap_vals.loc[total_shap_vals.index != "const"] = 0

    for _, match in shap_X.iterrows():
        total_shap_vals.loc[total_shap_vals.index != "const"] += (
            sol.loc[sol.index != "const"]
            * (match.loc[match.index != "const"]
               - expected_vals.loc[expected_vals.index != "const"])
        )

    total_shap_vals = total_shap_vals / num_preds

    print("MSE: " + str(mse))
    return total_shap_vals

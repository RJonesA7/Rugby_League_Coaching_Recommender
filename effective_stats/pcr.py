import pandas
import numpy
import psycopg2
from sklearn.decomposition import PCA

conn = psycopg2.connect(
    dbname="nrl_data",
    host="/dcs/23/u5503037/CS344/pgsock",
    port=5432
)


def principal_component_regression(weights, Z_df, pc_cols):

    # --------------------------------------------------
    # Attach weights and validation flags
    # --------------------------------------------------

    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    df = Z_df.copy()
    df = df.set_index(["match_id", "is_home"])

    df["weight"] = weights["z_sum"]
    df["validation"] = weights["validation"]

    eval_df = df[df["validation"]].drop(columns=["validation"])
    train_df = df[~df["validation"]].drop(columns=["validation"])

    train_df = train_df.reset_index()
    eval_df = eval_df.reset_index()

    # --------------------------------------------------
    # Build regression problem
    # --------------------------------------------------

    Z = train_df[pc_cols]
    y = train_df["final_margin"]
    w = train_df["weight"]

    Z_reg = Z.copy()
    Z_reg["const"] = 1.0

    sqrt_w = numpy.sqrt(w.to_numpy())

    y_w = y.to_numpy() * sqrt_w
    Z_w = Z_reg.mul(sqrt_w, axis=0)

    beta = numpy.linalg.solve(Z_w.T @ Z_w, Z_w.T @ y_w)
    beta = pandas.Series(beta, index=Z_reg.columns)

    # --------------------------------------------------
    # Validation prediction
    # --------------------------------------------------

    Z_test = eval_df[pc_cols]

    preds = (Z_test * beta.drop("const")).sum(axis=1) + beta["const"]

    predictions = list(zip(preds.to_list(), eval_df["match_id"].to_list()))
    predictions.sort(key=lambda x: x[0])

    # --------------------------------------------------
    # Select top 25% predictions
    # --------------------------------------------------

    num_preds = int(len(predictions) * 0.25)
    top_ids = set(mid for _, mid in predictions[-num_preds:])

    shap_rows = eval_df[eval_df["match_id"].isin(top_ids)]

    # --------------------------------------------------
    # SHAP values in PC space
    # φ_j = β_j (z_j − E[z_j])
    # --------------------------------------------------

    mu_z = Z.mean(axis=0)

    shap_store = {pc: [] for pc in pc_cols}

    for _, row in shap_rows.iterrows():

        for pc in pc_cols:

            shap_val = beta[pc] * (row[pc] - mu_z[pc])
            shap_store[pc].append(float(shap_val))

    # --------------------------------------------------
    # Aggregate boxplot stats
    # --------------------------------------------------

    output = {}

    for pc, values in shap_store.items():

        values = numpy.array(values)

        if len(values) == 0:
            output[pc] = [0.0, [0,0,0,0,0]]
            continue

        output[pc] = [
            float(values.mean()),
            [
                float(values.min()),
                float(numpy.percentile(values,25)),
                float(values.mean()),
                float(numpy.percentile(values,75)),
                float(values.max())
            ]
        ]

    return output


def principal_component_regression_old(weights):

    # -----------------------------
    # Extract matches
    # -----------------------------

    keys = list(weights.index)
    keys = [list(k) for k in keys]

    conditions = "".join(
        f"(team_stats_z.match_id = {k[0]} and team_stats_z.is_home = {k[1]}) or "
        for k in keys
    )[:-4]

    stats_select_query = f"""
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z
    join team_stats_z opp_stats_z
      on team_stats_z.match_id = opp_stats_z.match_id
     and team_stats_z.is_home != opp_stats_z.is_home
    where {conditions}
    """

    stat_df = pandas.read_sql_query(stats_select_query, conn)
    stat_df = stat_df.set_index(["match_id", "is_home"])

    # -----------------------------
    # Attach weights
    # -----------------------------

    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    stat_df["weight"] = weights["z_sum"]
    stat_df["validation"] = weights["validation"]

    eval_stats = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    stat_df = stat_df.reset_index()

    # -----------------------------
    # Build regression problem
    # -----------------------------

    X = stat_df.drop(columns=['team', 'is_home', 'final_margin', 'weight', 'match_id'])
    y = stat_df['final_margin']
    w = stat_df['weight']

    X = X.fillna(0)
    X = X.loc[:, X.nunique() > 1]

    X = X.drop(columns=[
        'score',
        'half_time',
        'tries',
        'conversions',
        'conversions_missed'
    ], errors="ignore")

    feature_cols = X.columns

    # -----------------------------
    # PCA
    # -----------------------------

    pca = PCA(n_components=len(feature_cols))
    Z = pca.fit_transform(X)

    Z = pandas.DataFrame(
        Z,
        columns=[f"PC{i+1}" for i in range(Z.shape[1])]
    )

    # -----------------------------
    # Weighted multilinear regression
    # -----------------------------

    Z["const"] = 1

    w = w ** 0.5
    y = y * w
    Z = Z.mul(w, axis=0)

    beta_pc = numpy.linalg.solve(Z.T @ Z, Z.T @ y)

    beta_pc = pandas.Series(beta_pc, index=Z.columns)

    # -----------------------------
    # Reconstruct coefficients
    # -----------------------------

    loadings = pca.components_.T
    beta_original = loadings @ beta_pc.drop("const").values

    """
    # PCA loadings
    loadings = pca.components_.T

    # PC coefficients
    pc_coefs = beta_pc.drop("const").values

    # Influence calculation
    stat_influence = numpy.abs(loadings) @ numpy.abs(pc_coefs)

    stat_influence = pandas.Series(
        stat_influence,
        index=feature_cols
    )

    print("\nStat influence scores:")
    print(stat_influence.sort_values(ascending=False).round(4))

    """

    sol = pandas.Series(beta_original, index=feature_cols)
    sol["const"] = beta_pc["const"]

    print("\nReconstructed coefficients:")
    print(sol.round(4).to_string())

    # -----------------------------
    # Validation testing
    # -----------------------------

    eval_stats = eval_stats.reset_index()

    test_data = eval_stats.drop(columns=['team', 'is_home', 'weight'])
    test_data = test_data.fillna(0)

    predictions = []
    correct = 0
    incorrect = 0
    mse = 0

    for match in test_data.iterrows():

        match = match[1]
        prediction_id = match['match_id']
        res = match['final_margin']

        match = match.drop(labels=['final_margin', 'match_id'])
        match['const'] = 1

        match = match.reindex(sol.index, fill_value=0)

        prediction_final_margin = (sol * match).sum()

        mse += (prediction_final_margin - res) ** 2

        if (prediction_final_margin > 0 and res > 0) or (prediction_final_margin < 0 and res < 0):
            correct += 1
        else:
            incorrect += 1

        predictions.append((prediction_final_margin, prediction_id))

    mse = mse / (correct + incorrect)

    # -----------------------------
    # Select top 25% predictions
    # -----------------------------

    predictions = sorted(predictions, key=lambda x: x[0])
    num_preds = int(len(predictions) * 0.25)
    predictions = predictions[-num_preds:]
    predictions = [p[1] for p in predictions]

    shap_rows = test_data[test_data['match_id'].isin(predictions)]

    # -----------------------------
    # SHAP calculations
    # -----------------------------

    expected_vals = X.mean()

    shap_store = {col: [] for col in sol.index if col != 'const'}

    for match in shap_rows.iterrows():

        match = match[1]
        match = match.drop(columns=['final_margin', 'match_id'])

        for index in sol.index:

            if index != 'const':
                shap_val = sol[index] * (match[index] - expected_vals[index])
                shap_store[index].append(shap_val)

    output = {}

    for feature, values in shap_store.items():

        values = numpy.array(values)

        output[feature] = [
            float(values.mean()),
            [
                float(values.min()),
                float(numpy.percentile(values, 25)),
                float(values.mean()),
                float(numpy.percentile(values, 75)),
                float(values.max())
            ]
        ]

    return output


    """

    # --------------------------------------------------
    # Compute PCA-space contributions
    # --------------------------------------------------

    # Convert training X into PCA space
    Z_train = pca.transform(X)

    # Expected PC values
    mu_z = Z_train.mean(axis=0)

    # PCA loadings
    loadings = pca.components_.T   # shape (stats, PCs)

    # Precompute allocation shares for each PC
    abs_loadings = numpy.abs(loadings)

    shares = abs_loadings / abs_loadings.sum(axis=0, keepdims=True)

    # --------------------------------------------------
    # Select rows to explain (same logic you already use)
    # --------------------------------------------------

    eval_stats = eval_stats.reset_index()
    test_data = eval_stats.drop(columns=['team', 'is_home', 'weight'])
    test_data = test_data.fillna(0)

    X_test = test_data.drop(columns=['final_margin', 'match_id'])
    X_test = X_test.reindex(columns=feature_cols, fill_value=0)

    # Transform to PCA space
    Z_test = pca.transform(X_test)

    # Predict margins
    y_pred = (Z_test @ beta_pc.drop("const").values) + beta_pc["const"]

    predictions = list(zip(y_pred, test_data["match_id"].to_list()))
    predictions.sort(key=lambda x: x[0])

    num_preds = int(len(predictions) * 0.25)
    top_match_ids = set(mid for _, mid in predictions[-num_preds:])

    shap_rows = test_data[test_data["match_id"].isin(top_match_ids)]

    # --------------------------------------------------
    # Allocate PC contributions to stats
    # --------------------------------------------------

    shap_store = {col: [] for col in feature_cols}

    for row in shap_rows.iterrows():

        row = row[1]

        x = row.drop(labels=['final_margin', 'match_id'])
        x = x.reindex(feature_cols, fill_value=0).to_numpy()

        # PCA transform
        z = pca.transform([x])[0]

        # PC contributions
        pc_contrib = beta_pc.drop("const").values * (z - mu_z)

        # Allocate to stats
        stat_contrib = shares @ pc_contrib

        for i, stat in enumerate(feature_cols):
            shap_store[stat].append(stat_contrib[i])

    # --------------------------------------------------
    # Aggregate exactly as before
    # --------------------------------------------------

    output = {}

    for feature, values in shap_store.items():

        values = numpy.array(values)

        output[feature] = [
            float(values.mean()),
            [
                float(values.min()),
                float(numpy.percentile(values,25)),
                float(values.mean()),
                float(numpy.percentile(values,75)),
                float(values.max())
            ]
        ]

    return output
    """



def pcr_shap_reconstructed(
    weights,
    k=None,
    use_variance_weighting=True,
    loading_power=2
):
    """
    Principal Component Regression with:
      - OLS on principal components
      - SHAP values computed on PC space
      - proportional reconstruction to original stats

    Parameters
    ----------
    weights : pd.Series
        Composite-indexed series by (match_id, is_home), with:
          - values ignored
          - columns expected after copy:
                z_sum
                validation
    k : int or None
        Number of PCs to use for reconstruction.
        If None, use all PCs.
    use_variance_weighting : bool
        If True, multiply reconstructed PC SHAP contributions by explained_variance_ratio.
        This suppresses noisy late PCs.
    loading_power : int or float
        Reconstruction share uses abs(loadings) ** loading_power.
        2 is usually better than 1 because it sharpens attribution.
    """

    # -----------------------------
    # Extract match rows
    # -----------------------------
    keys = list(weights.index)
    keys = [list(k_) for k_ in keys]

    conditions = "".join(
        f"(team_stats_z.match_id = {k_[0]} and team_stats_z.is_home = {k_[1]}) or "
        for k_ in keys
    )[:-4]

    stats_select_query = f"""
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z
    join team_stats_z opp_stats_z
      on team_stats_z.match_id = opp_stats_z.match_id
     and team_stats_z.is_home != opp_stats_z.is_home
    where {conditions}
    """

    stat_df = pandas.read_sql_query(stats_select_query, conn)
    stat_df = stat_df.set_index(["match_id", "is_home"])

    # -----------------------------
    # Attach weights / validation
    # -----------------------------
    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    stat_df["weight"] = weights["z_sum"]
    stat_df["validation"] = weights["validation"]

    eval_stats = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    stat_df = stat_df.reset_index()

    # -----------------------------
    # Build training design
    # -----------------------------
    X = stat_df.drop(columns=["team", "is_home", "final_margin", "weight", "match_id"])
    y = stat_df["final_margin"]
    w = stat_df["weight"]

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

    # -----------------------------
    # PCA on original training X
    # -----------------------------
    pca = PCA(n_components=len(feature_cols))
    Z = pca.fit_transform(X)  # shape: (n, p)

    pc_cols = [f"PC{i+1}" for i in range(Z.shape[1])]
    Z = pandas.DataFrame(Z, columns=pc_cols, index=X.index)

    # -----------------------------
    # Weighted OLS on PCs
    # -----------------------------
    Z_reg = Z.copy()
    Z_reg["const"] = 1.0

    sqrt_w = numpy.sqrt(w.to_numpy())
    y_w = y.to_numpy() * sqrt_w
    Z_w = Z_reg.mul(sqrt_w, axis=0)

    sol = numpy.linalg.solve(Z_w.T @ Z_w, Z_w.T @ y_w)
    sol = pandas.Series(sol, index=Z_reg.columns)

    # -----------------------------
    # Validation prediction
    # -----------------------------
    eval_stats = eval_stats.reset_index()

    test_data = eval_stats.drop(columns=["team", "is_home", "weight"])
    test_data = test_data.fillna(0)

    X_test = test_data.drop(columns=["final_margin", "match_id"])
    X_test = X_test.reindex(columns=feature_cols, fill_value=0)

    Z_test = pca.transform(X_test)
    Z_test_df = pandas.DataFrame(Z_test, columns=pc_cols)

    y_pred = (Z_test_df * sol.drop("const")).sum(axis=1) + sol["const"]

    predictions = list(zip(y_pred.to_list(), test_data["match_id"].to_list()))
    predictions.sort(key=lambda x: x[0])

    num_preds = int(len(predictions) * 0.25)
    top_match_ids = set(mid for _, mid in predictions[-num_preds:])

    shap_rows = test_data[test_data["match_id"].isin(top_match_ids)].copy()

    # -----------------------------
    # PC-level SHAP values
    # For linear regression on orthogonal PCs:
    #   phi_j = beta_j * (z_j - E[z_j])
    # -----------------------------
    mu_z = Z.mean(axis=0).to_numpy()
    beta_pc = sol.drop("const").to_numpy()

    loadings = pca.components_.T  # shape: (n_stats, n_pcs)
    explained = pca.explained_variance_ratio_

    if k is None:
        k = loadings.shape[1]
    k = min(k, loadings.shape[1])

    loadings_k = loadings[:, :k]
    beta_pc_k = beta_pc[:k]
    mu_z_k = mu_z[:k]
    explained_k = explained[:k]

    # -----------------------------
    # Proportional reconstruction weights
    # Share stat i gets from PC j:
    #   abs(loading_ij)^power / sum_i abs(loading_ij)^power
    # -----------------------------
    loading_weights = numpy.abs(loadings_k) ** loading_power
    col_sums = loading_weights.sum(axis=0, keepdims=True)
    shares = numpy.divide(
        loading_weights,
        col_sums,
        out=numpy.zeros_like(loading_weights),
        where=(col_sums != 0)
    )

    if use_variance_weighting:
        pc_multipliers = explained_k
    else:
        pc_multipliers = numpy.ones_like(explained_k)

    # -----------------------------
    # Reconstruct PC SHAP to stats
    # -----------------------------
    shap_store = {col: [] for col in feature_cols}

    for _, row in shap_rows.iterrows():
        x_row = row.drop(labels=["final_margin", "match_id"])
        x_row = x_row.reindex(feature_cols, fill_value=0).to_numpy().reshape(1, -1)

        z_row = pca.transform(x_row)[0][:k]

        # PC SHAP for this row
        phi_pc = beta_pc_k * (z_row - mu_z_k)

        # Optional explained variance weighting
        phi_pc = phi_pc * pc_multipliers

        # Reconstruct to original stats
        # stat_contrib_i = sum_j share_ij * phi_pc_j
        stat_contrib = shares @ phi_pc

        for i, stat in enumerate(feature_cols):
            shap_store[stat].append(float(stat_contrib[i]))

    # -----------------------------
    # Aggregate output exactly as before
    # -----------------------------
    output = {}

    for feature, values in shap_store.items():
        values = numpy.array(values)

        if len(values) == 0:
            output[feature] = [0.0, [0.0, 0.0, 0.0, 0.0, 0.0]]
            continue

        output[feature] = [
            float(values.mean()),
            [
                float(values.min()),
                float(numpy.percentile(values, 25)),
                float(values.mean()),
                float(numpy.percentile(values, 75)),
                float(values.max())
            ]
        ]

    return output


def principal_component_regression_65(weights, variance_threshold=0.65):
    # -----------------------------
    # Extract matches
    # -----------------------------
    keys = list(weights.index)
    keys = [list(k) for k in keys]

    conditions = "".join(
        f"(team_stats_z.match_id = {k[0]} and team_stats_z.is_home = {k[1]}) or "
        for k in keys
    )[:-4]

    stats_select_query = f"""
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z
    join team_stats_z opp_stats_z
      on team_stats_z.match_id = opp_stats_z.match_id
     and team_stats_z.is_home != opp_stats_z.is_home
    where {conditions}
    """

    stat_df = pandas.read_sql_query(stats_select_query, conn)
    stat_df = stat_df.set_index(["match_id", "is_home"])

    # -----------------------------
    # Attach weights
    # -----------------------------
    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    stat_df["weight"] = weights["z_sum"]
    stat_df["validation"] = weights["validation"]

    eval_stats = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    stat_df = stat_df.reset_index()

    # -----------------------------
    # Build regression problem
    # -----------------------------
    X = stat_df.drop(columns=['team', 'is_home', 'final_margin', 'weight', 'match_id'])
    y = stat_df['final_margin']
    w = stat_df['weight']

    X = X.fillna(0)
    X = X.loc[:, X.nunique() > 1]

    X = X.drop(columns=[
        'score',
        'half_time',
        'tries',
        'conversions',
        'conversions_missed'
    ], errors="ignore")

    feature_cols = list(X.columns)

    # -----------------------------
    # PCA
    # -----------------------------
    pca_full = PCA(n_components=len(feature_cols))
    Z_full = pca_full.fit_transform(X)

    explained = pca_full.explained_variance_ratio_
    cumulative = numpy.cumsum(explained)

    k = numpy.searchsorted(cumulative, variance_threshold) + 1
    print(f"\nUsing {k} PCs to explain {cumulative[k-1]:.4f} of variance")

    Z = Z_full[:, :k]
    pc_cols = [f"PC{i+1}" for i in range(k)]
    Z = pandas.DataFrame(Z, columns=pc_cols, index=X.index)

    # -----------------------------
    # Weighted multilinear regression in PC space
    # -----------------------------
    Z["const"] = 1.0

    sqrt_w = numpy.sqrt(w.to_numpy())
    y_w = y.to_numpy() * sqrt_w
    Z_w = Z.mul(sqrt_w, axis=0)

    beta_pc = numpy.linalg.solve(Z_w.T @ Z_w, Z_w.T @ y_w)
    beta_pc = pandas.Series(beta_pc, index=Z.columns)

    # -----------------------------
    # Reconstruct coefficients in original feature space
    # -----------------------------
    loadings_k = pca_full.components_[:k].T   # shape: (n_features, k)
    beta_original = loadings_k @ beta_pc.drop("const").values

    sol = pandas.Series(beta_original, index=feature_cols)
    sol["const"] = beta_pc["const"]

    print("\nReconstructed coefficients:")
    print(sol.round(4).to_string())

    # -----------------------------
    # Validation testing
    # -----------------------------
    eval_stats = eval_stats.reset_index()

    test_data = eval_stats.drop(columns=['team', 'is_home', 'weight'])
    test_data = test_data.fillna(0)

    predictions = []
    correct = 0
    incorrect = 0
    mse = 0.0

    for _, match in test_data.iterrows():
        prediction_id = match['match_id']
        res = match['final_margin']

        x_row = match.drop(labels=['final_margin', 'match_id']).copy()
        x_row['const'] = 1.0
        x_row = x_row.reindex(sol.index, fill_value=0)

        prediction_final_margin = float((sol * x_row).sum())
        predictions.append((prediction_final_margin, prediction_id))

        mse += (prediction_final_margin - res) ** 2

        if (prediction_final_margin > 0 and res > 0) or (prediction_final_margin < 0 and res < 0):
            correct += 1
        else:
            incorrect += 1

    if (correct + incorrect) > 0:
        mse /= (correct + incorrect)

    print(f"\nValidation MSE: {mse:.4f}")
    print(f"Correct: {correct}, Incorrect: {incorrect}, Accuracy: {correct / (correct + incorrect):.4f}")

    # -----------------------------
    # Select top 25% predictions
    # -----------------------------
    predictions = sorted(predictions, key=lambda x: x[0])
    num_preds = max(1, int(len(predictions) * 0.25))
    predictions = predictions[-num_preds:]
    prediction_ids = [p[1] for p in predictions]

    shap_rows = test_data[test_data['match_id'].isin(prediction_ids)]

    # -----------------------------
    # "Linear SHAP" on reconstructed original-space model
    # -----------------------------
    expected_vals = X.mean()
    shap_store = {col: [] for col in feature_cols}

    for _, match in shap_rows.iterrows():
        x_row = match.drop(labels=['final_margin', 'match_id'])

        for col in feature_cols:
            shap_val = sol[col] * (x_row[col] - expected_vals[col])
            shap_store[col].append(float(shap_val))

    output = {}

    for feature, values in shap_store.items():
        values = numpy.array(values)

        if len(values) == 0:
            output[feature] = [0.0, [0.0, 0.0, 0.0, 0.0, 0.0]]
            continue

        output[feature] = [
            float(values.mean()),
            [
                float(values.min()),
                float(numpy.percentile(values, 25)),
                float(values.mean()),
                float(numpy.percentile(values, 75)),
                float(values.max())
            ]
        ]

    return output
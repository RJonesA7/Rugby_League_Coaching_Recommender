import pandas, numpy, math
import psycopg2
import matplotlib.pyplot as plt

from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_squared_error, r2_score

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)


"""
Function which takes in a pandas Series of matches with a composite index of match_id and home and values
(as returned by a function in similar_teams, representing similar teams in the database that we want to consider)
And trains a ridge regressor using sklearn to predict matches
"""
def ridge_regression(weights):
    #Get the match_ids we will use to extract teams from the db
    keys = weights.index

    #Convert to lists so we can access and update them
    keys = list(keys)
    for i in range(len(keys)):
        keys[i] = list(keys[i])

    #Get the database teams for analysis, which are those represented by the match_id and is_home now in keys
    conditions = ["(team_stats_z.match_id = " + str(key[0]) + " and team_stats_z.is_home = " + str(key[1]) + ") or " for key in keys]
    conditions = "".join(conditions)
    conditions = conditions[:-4]

    #Select all stats and the final points difference to the opposition (we need to know how well the team did for analysis)
    stats_select_query = """
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z join team_stats_z opp_stats_z on team_stats_z.match_id = opp_stats_z.match_id and team_stats_z.is_home != opp_stats_z.is_home
    where {conds}
    """.format(conds = conditions)
    stat_df = pandas.read_sql_query(stats_select_query, conn)
    stat_df = stat_df.set_index(['match_id', "is_home"])

    #Get the weights, to then be combined with the stat_df to inform modelling
    weights['z_sum'] = 1/weights['z_sum']
    stat_df['weight'] = weights['z_sum']
    stat_df['validation'] = weights['validation']

    #Reset the index now we have combined
    stat_df = stat_df.reset_index()

    #Split into training data and SHAP calculation data
    eval_df = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    #Create the standard regression problem, with training data X = past stats and labels y = final margins
    X = stat_df.drop(columns = ['team', 'is_home', 'final_margin', 'weight', 'match_id'])

    y = stat_df['final_margin']
    w = stat_df['weight']

    #Set NaNs to 0 to avoid errors being caused without sacrificing information present in other rows for that column
    X = X.fillna(0)

    #Drop columns from the data with zero variance, field goals, penalties etc. will often have this and cause errors
    X = X.loc[:, X.nunique() > 1]

    #Additionally, drop the stats that can't be targeted and are completely dependent on other stats, like score.
    X = X.drop(columns=['score', 'half_time', 'tries', 'conversions', 'conversions_missed'])
    #Also drop stats which dominate signal as they are overly correlated with victory
    X = X.drop(columns=['line_breaks', 'all_run_metres'])

    # Ridge regression with CV over alpha
    # Log-spaced alphas is standard; widen/narrow depending on stability.
    alphas = numpy.logspace(-4, 4, 41)

    # RidgeCV uses internal CV (default 5-fold) and will choose alpha.
    # fit_intercept=True is standard; your X has no explicit constant column here.
    ridge = RidgeCV(alphas=alphas, cv=5, fit_intercept=True)

    ridge.fit(X, y, sample_weight=w)

    # Evaluate on validation split
    y_test = eval_df['final_margin']
    X_test = eval_df.drop(columns = ['team', 'is_home', 'final_margin', 'weight', 'match_id'])

    # Mirror same drops; then align columns to train exactly
    X_test = X_test.drop(columns=['score', 'half_time', 'tries', 'conversions', 'conversions_missed'])
    X_test = X_test.drop(columns=['line_breaks', 'all_run_metres'])
    X_test = X_test.fillna(0)
    X_test = X_test.reindex(columns=X.columns, fill_value=0)

    y_pred = ridge.predict(X_test)

    print("Best alpha:", ridge.alpha_)
    mse = mean_squared_error(y_test, y_pred)
    print("MSE:", mse)
    print("R^2:", r2_score(y_test, y_pred))

    y_pred_cls = (y_pred > 0)
    y_true_cls = (y_test > 0)
    accuracy = (y_pred_cls == y_true_cls).mean()
    print(f"Sign accuracy: {mse:.3%}")

    #return accuracy

    #Calculate SHAP values exactly as previously:

    # 1) Extract sol
    sol = pandas.Series(ridge.coef_, index=X.columns)
    sol["const"] = float(ridge.intercept_)

    # 2) Predict on eval_df and select top 25% most positive predictions
    test_data = eval_df.drop(columns=['team', 'is_home', 'weight'])
    test_data = test_data.fillna(0)

    X_test = test_data.drop(columns=['final_margin', 'match_id'])
    X_test = X_test.reindex(columns=X.columns, fill_value=0)

    y_pred = ridge.predict(X_test)

    predictions = list(zip(y_pred, test_data["match_id"].to_list()))

    predictions.sort(key=lambda x: x[0])
    num_preds = int(len(predictions) * 0.25)
    top_match_ids = set([mid for _, mid in predictions[-num_preds:]])

    # 3) Rows to explain
    shap_rows = test_data[test_data["match_id"].isin(top_match_ids)]

    # Pasted block from dependent_SHAP_multilinear_regression.py explaining with SHAP variables that do not assume independence
    # Estimate mu, Sigma for the feature distribution (excluding 'const').
    #
    # IMPORTANT: X was multiplied by sqrt(w) for weighted regression fitting.
    # For the SHAP conditional distribution, we want the feature distribution in the original scale.
    # We "unweight" by dividing by sqrt(w) to approximate the original feature samples.
    feature_cols = [c for c in sol.index if c != "const"]
    M = len(feature_cols)

    # Recover approximate unweighted feature matrix for distribution estimation
    X_dist = X[feature_cols].div(w, axis=0)
    X_dist = X_dist.replace([numpy.inf, -numpy.inf], numpy.nan).fillna(0)

    mu = X_dist.mean(axis=0).to_numpy() # (M,)
    Sigma = X_dist.cov().to_numpy() # (M, M)

    # Numerical guard: small diagonal jitter for stability
    Sigma = Sigma + numpy.eye(M) * 1e-8

    # Dependent Kernel SHAP via Gaussian conditional expectation
    #
    # For linear f(x) = b0 + beta^T x:
    # v(S) = E[f(X) | X_S = x_S*]
    #      = b0 + beta_S^T x_S* + beta_C^T E[X_C | X_S = x_S*]
    #
    # If X ~ N(mu, Sigma), then:
    # E[X_C | X_S = x_S] = mu_C + Sigma_CS Sigma_SS^{-1} (x_S - mu_S)
    #
    # This is exactly the Aas et al. Gaussian conditional, but we only need the conditional mean because f is linear.

    # Controls (can be changed):
    nsamples = 200 # number of sampled coalitions per explained row (excluding empty/full)
    c = 1e6 # big weight for empty/full constraints

    # Precompute arrays for speed
    beta = numpy.array([float(sol[f]) for f in feature_cols], dtype=float)  # (M,)
    b0 = float(sol["const"]) if "const" in sol.index else 0.0

    # Kernel SHAP size sampling distribution: p(s) proportional to 1/(s*(M-s)), s=1..M-1
    if M >= 2:
        sizes = numpy.arange(1, M)  # 1..M-1
        size_probs = 1.0 / (sizes * (M - sizes))
        size_probs = size_probs / size_probs.sum()

    # Combination helper without new imports (math.comb)
    def nCk(n, k):
        # Safe for moderate n; returns int
        return math.comb(n, k)

    def coalition_value_v_analytic(x_star_vec, S_mask):
        """
        x_star_vec: (M,) values at the explained instance for feature_cols
        S_mask: (M,) boolean, True if feature included in coalition
        returns v(S) analytically.
        """
        s = int(S_mask.sum())

        # Empty coalition: unconditional expectation E[f(X)] = b0 + beta^T mu
        if s == 0:
            return float(b0 + beta @ mu)

        # Full coalition: f(x*)
        if s == M:
            return float(b0 + beta @ x_star_vec)

        idx_S = numpy.where(S_mask)[0]
        idx_C = numpy.where(~S_mask)[0]

        mu_S = mu[idx_S]
        mu_C = mu[idx_C]

        Sigma_SS = Sigma[numpy.ix_(idx_S, idx_S)]
        Sigma_CS = Sigma[numpy.ix_(idx_C, idx_S)]

        x_S = x_star_vec[idx_S]

        # Conditional mean of C given S:
        # mu_C|S = mu_C + Sigma_CS Sigma_SS^{-1} (x_S - mu_S)
        # Use solve for numerical stability.
        try:
            delta = numpy.linalg.solve(Sigma_SS, (x_S - mu_S))
        except numpy.linalg.LinAlgError:
            Sigma_SS_j = Sigma_SS + numpy.eye(Sigma_SS.shape[0]) * 1e-6
            delta = numpy.linalg.solve(Sigma_SS_j, (x_S - mu_S))

        mu_C_given = mu_C + Sigma_CS @ delta

        # Now v(S) = b0 + beta_S^T x_S + beta_C^T mu_C|S
        beta_S = beta[idx_S]
        beta_C = beta[idx_C]
        return float(b0 + beta_S @ x_S + beta_C @ mu_C_given)


    # Build the coalition matrix / solve for phi / store SHAP values

    shap_store = {col: [] for col in sol.index if col != 'const'}
    
    # WLS form (Kernel SHAP):
    #   v ≈ Z phi, with Shapley-kernel weights on rows
    # where Z has an intercept column and coalition indicators.
    
    # We compute phi per explained row and store phi_j (excluding intercept) into shap_store.
    if M == 0:
        # No features to attribute
        pass
    elif M == 1:
        # With a single feature, the Shapley value is just f(x*) - E[f(X)] attributed to that feature
        only_f = feature_cols[0]
        for match in shap_rows.iterrows():
            match = match[1]
            x_star_series = match.drop(labels=['final_margin', 'match_id'], errors='ignore')
            x_star_series = x_star_series.reindex(sol.index, fill_value=0)
            x_star_val = float(x_star_series[only_f])
            phi1 = (b0 + beta[0] * x_star_val) - (b0 + beta[0] * mu[0])
            shap_store[only_f].append(float(phi1))
    else:
        for match in shap_rows.iterrows():
            match = match[1]
            x_star_series = match.drop(labels=['final_margin', 'match_id'], errors='ignore')
            x_star_series = x_star_series.reindex(sol.index, fill_value=0)
            x_star_vec = numpy.array([float(x_star_series[f]) for f in feature_cols], dtype=float)

            # Sample coalitions (excluding empty/full)
            coalition_sizes = numpy.random.choice(sizes, size=nsamples, p=size_probs)
            coalitions = []
            for s in coalition_sizes:
                idx = numpy.random.choice(M, size=int(s), replace=False)
                mask = numpy.zeros(M, dtype=bool)
                mask[idx] = True
                coalitions.append(mask)

            # Prepend empty and append full
            coalitions = [numpy.zeros(M, dtype=bool)] + coalitions + [numpy.ones(M, dtype=bool)]
            L = len(coalitions)

            # Build Z, v, and weights
            Z = numpy.zeros((L, M + 1), dtype=float)
            v_vec = numpy.zeros(L, dtype=float)
            w_vec = numpy.zeros(L, dtype=float)

            for i, mask in enumerate(coalitions):
                Z[i, 0] = 1.0
                Z[i, 1:] = mask.astype(float)

                # Analytic coalition value
                v_vec[i] = coalition_value_v_analytic(x_star_vec, mask)

                s = int(mask.sum())
                if s == 0 or s == M:
                    w_vec[i] = c
                else:
                    # Shapley kernel: (M-1) / (C(M,s) * s * (M-s))
                    w_vec[i] = (M - 1) / (float(nCk(M, s)) * s * (M - s))

            # Weighted least squares via sqrt-weights
            sqrt_w = numpy.sqrt(w_vec)
            Z_w = Z * sqrt_w[:, None]
            v_w = v_vec * sqrt_w

            # Solve for phi (stable least squares)
            phi, *_ = numpy.linalg.lstsq(Z_w, v_w, rcond=None)

            # Store SHAP values (exclude intercept phi[0])
            for j, f in enumerate(feature_cols):
                shap_store[f].append(float(phi[j + 1]))

    """
    #Legacy SHAP Calculations:

    expected_vals = X.mean()
    shap_store = {col: [] for col in sol.index if col != 'const'}

    #Calculate SHAP values
    for match in shap_rows.iterrows():
        match = match[1]
        match = match.drop(columns=['final_margin', 'match_id'])
        
        for index in sol.index:
            if index != 'const':
                shap_val = sol[index] * (match[index] - expected_vals[index])
                shap_store[index].append(shap_val)
    """

    output = {}

    for feature, values in shap_store.items():
        values = numpy.array(values)

        output[feature] = [
            float(values.mean()), # The original output of an average SHAP value, maintaining for now alongside quartile plots
            [
                float(values.min()), # Data for a quartile plot
                float(numpy.percentile(values,25)),
                float(values.mean()),
                float(numpy.percentile(values,75)),
                float(values.max())
            ]
        ]

    return output
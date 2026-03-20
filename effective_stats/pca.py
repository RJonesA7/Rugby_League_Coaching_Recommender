import pandas as pd
import numpy as np
import psycopg2
from sklearn.decomposition import PCA
from statsmodels.stats.outliers_influence import variance_inflation_factor


conn = psycopg2.connect(
    dbname="nrl_data",
    host="/dcs/23/u5503037/CS344/pgsock",
    port=5432
)

def pca(weights):
    keys = [list(k) for k in list(weights.index)]

    conditions = "".join(
        f"(team_stats_z.match_id = {key[0]} and team_stats_z.is_home = {key[1]}) or "
        for key in keys
    )[:-4]

    query = f"""
    select team_stats_z.*, (team_stats_z.score - opp_stats_z.score) as final_margin
    from team_stats_z
    join team_stats_z opp_stats_z
      on team_stats_z.match_id = opp_stats_z.match_id
     and team_stats_z.is_home != opp_stats_z.is_home
    where {conditions}
    """

    df = pd.read_sql_query(query, conn).set_index(["match_id", "is_home"])

    weights = weights.copy()
    weights["z_sum"] = 1 / weights["z_sum"]

    df["weight"] = weights["z_sum"]
    df["validation"] = weights["validation"]

    df = df[~df["validation"]].drop(columns=["validation"]).reset_index()

    X = df.drop(columns=["team", "is_home", "final_margin", "weight", "match_id"])
    X = X.fillna(0)
    X = X.loc[:, X.nunique() > 1]
    X = X.drop(columns=["score", "half_time", "tries", "conversions", "conversions_missed"], errors="ignore")

    #Additionally analyse multicollinearity via VIF

    threshold = 5.0

    X_vif = X.copy()

    while True:
        vif = pd.DataFrame()
        vif["feature"] = X_vif.columns
        vif["VIF"] = [variance_inflation_factor(X_vif.values, i) for i in range(X_vif.shape[1])]

        max_vif = vif["VIF"].max()

        if max_vif <= threshold:
            break

        drop_feature = vif.sort_values("VIF", ascending=False).iloc[0]
        print(f"Dropping '{drop_feature['feature']}' with VIF = {drop_feature['VIF']:.3f}")

        X_vif = X_vif.drop(columns=[drop_feature["feature"]])

    # Final surviving features
    vif = pd.DataFrame()
    vif["feature"] = X_vif.columns
    vif["VIF"] = [variance_inflation_factor(X_vif.values, i) for i in range(X_vif.shape[1])]

    print("\nRemaining features and VIF:")
    print(vif.sort_values("VIF", ascending=False))

    pca = PCA()
    pca.fit(X)

    contribution_table = pd.DataFrame(
        pca.components_.T,
        index=X.columns,
        columns=[f"PC{i+1}" for i in range(pca.n_components_)]
    )

    explained_variance = pd.Series(
        pca.explained_variance_ratio_,
        index=contribution_table.columns,
        name="explained_variance_ratio"
    )

    print("\nExplained variance ratio:")
    print(explained_variance.round(4).to_string())

    print("\nStatistic contribution to each principal component:")
    print(contribution_table.round(4).to_string())

    #Export contributions to csv:
    contribution_table.to_csv("pca_stat_loadings.csv")

    return contribution_table, explained_variance
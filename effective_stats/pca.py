import pandas as pd
import numpy as np
import psycopg2
from sklearn.decomposition import PCA

conn = psycopg2.connect(
    dbname="nrl_data",
    host="/dcs/23/u5503037/CS344/pgsock",
    port=5432
)

def pca_stat_contributions(weights):
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

    # Standardise so PCA is not dominated by larger-scale stats
    X = (X - X.mean()) / X.std(ddof=0)
    X = X.replace([np.inf, -np.inf], 0).fillna(0)

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

    return contribution_table, explained_variance
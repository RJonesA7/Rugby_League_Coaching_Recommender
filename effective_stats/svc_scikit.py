import pandas, numpy
import psycopg2
import matplotlib.pyplot as plt
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error, r2_score

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)


"""
Function which takes in a pandas Series of matches with a composite index of match_id and home and values
(as returned by a function in similar_teams, representing similar teams in the database that we want to consider)
And trains a support vector classifier using sklearn to predict matches
"""
def svc_scikit(weights):
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

    #Create the standard multilinear regression problem, with training data X = past stats and labels y = final margins
    X = stat_df.drop(columns = ['team', 'is_home', 'final_margin', 'weight', 'match_id'])
    y = stat_df['final_margin']
    w = stat_df['weight']

    #Set NaNs to 0 to avoid errors being caused without sacrificing information present in other rows for that column
    X = X.fillna(0)

    #Drop columns from the data with zero variance, field goals, penalties etc. will often have this and cause errors
    X = X.loc[:, X.nunique() > 1]
    #Additionally, drop the stats that can't be targeted and are completely dependent on other stats, like score.
    X = X.drop(columns=['score', 'half_time', 'tries', 'conversions', 'conversions_missed'])
    #This line is to be freely updated to try and obtain meaningful results
    #X = X.drop(columns=['missed_tackles', 'tackles_made', 'penalties_conceded', 'receipts', 'penalty_goals', 'line_breaks', 'all_run_metres'])

    svr = SVR(kernel="rbf")

    param_grid = {
        "C": [0.1, 1, 10, 100],
        "epsilon": [0.01, 0.1, 0.5, 1.0],
        "gamma": ["scale", "auto", 0.01, 0.1, 1, 10],
    }

    search = GridSearchCV(
        svr,
        param_grid=param_grid,
        cv=5,
        scoring="neg_mean_squared_error",
        n_jobs=-1
    )

    search.fit(X, y, sample_weight=w)

    y_test = eval_df['final_margin']
    X_test = eval_df.drop(columns = ['team', 'is_home', 'final_margin', 'weight', 'match_id'])
    X_test = X_test.drop(columns=['score', 'half_time', 'tries', 'conversions', 'conversions_missed'])
    #Drop columns from the data with zero variance, field goals, penalties etc. will often have this and cause errors
    X = X.loc[:, X.nunique() > 1]

    #Set NaNs to 0 to avoid errors being caused without sacrificing information present in other rows for that column
    X_test = X_test.fillna(0)

    best_model = search.best_estimator_
    y_pred = best_model.predict(X_test)

    print("Best params:", search.best_params_)
    mse = mean_squared_error(y_test, y_pred)
    print("MSE:", mse)
    print("R^2:", r2_score(y_test, y_pred))

    y_pred_cls = (y_pred > 0)
    y_true_cls = (y_test > 0)

    accuracy = (y_pred_cls == y_true_cls).mean()
    print(f"Sign accuracy: {accuracy:.3%}")

    return accuracy


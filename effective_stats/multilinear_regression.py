import pandas, numpy
import psycopg2
import matplotlib.pyplot as plt
from scipy.stats import linregress

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

"""
Function which takes in a pandas Series of matches with a composite index of match_id and home and values
(as returned by a function in similar_teams, representing similar teams in the database that we want to consider)
And outputs an array of tuples (stat, effectiveness) which gives 
target stats to beat the initially input opposition 
and how effective targeting that stat will be for beating the opposition
"""
def multilinear_regression(weights):
    
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

    #Split into training data and SHAP calculation data
    eval_stats = stat_df[stat_df["validation"]].drop(columns=["validation"])
    stat_df = stat_df[~stat_df["validation"]].drop(columns=["validation"])

    #Reset the index now we have combined
    stat_df = stat_df.reset_index()

    #Create the standard multilinear regression problem, with training data X = past stats and labels y = final margins
    X = stat_df.drop(columns = ['team', 'is_home', 'final_margin', 'weight', 'match_id'])
    y = stat_df['final_margin']
    w = stat_df['weight']

    #Set NaNs to 0 to avoid errors being caused without sacrificing information present in other rows for that column
    X = X.fillna(0)

    #Drop columns from the data with zero variance, field goals, penalties etc. will often have this and cause errors
    X = X.loc[:, X.nunique() > 1]
    #Additionally, drop the stats that can't be targeted and are completely dependent on other stats, like score.
    X = X.drop(columns=['score', 'half_time', 'tries', 'conversions', 'conversions_missed', 'line_breaks', 'all_run_metres'])

    #Add a constant column to X to allow for bias
    X['const'] = 1

    #Square root the weightings then multiply through X and y to weight towards most similar matches
    w = w ** (1/2)
    y = y * w
    X = X.mul(w, axis = 0)

    #Find solution to (X^TX)^-1X^Ty, and preserve column names for the index so we can see which stat is which.
    sol = numpy.linalg.solve(X.T @ X, X.T @ y)

    sol = pandas.Series(sol, index=X.columns)

    predictions = []
    #For testing: Run some predictions of the final margin, and print them alongside some actual final margins
    test_data = stat_df.drop(columns = ['team', 'is_home', 'weight'])
    test_data = test_data.fillna(0)
    for match in test_data.iterrows():
        match = match[1]
        prediction_id = match['match_id']
        match = match.drop(columns=['final_margin', 'match_id'])
        prediction = sol * match
        prediction_final_margin = prediction.sum()
        predictions.append((prediction_final_margin, prediction_id))

    #Take just the top 25% most positive predictions, as we are interested in what makes the model predicts wins
    predictions = sorted(predictions, key=lambda x: x[0])
    num_preds = int(len(predictions) * 0.25)
    predictions = predictions[-1 * num_preds:]
    predictions = [p[1] for p in predictions]

    #Get these rows of test data for calculating SHAP values
    shap_rows = test_data[test_data['match_id'].isin(predictions)]

    #In order to calculate the shap values, we need the mean value of each column in the training data X for the expected values
    expected_vals = X.mean()

    #Create a store series to total the shap values across relevant matches
    total_shap_vals = sol.copy()
    for index in total_shap_vals.index:
        if index != 'const':
            total_shap_vals[index] = 0

    #Calculate SHAP values for all matches according to the formula (see iteration 2 report for further details)
    shap_vals = sol.copy()
    for match in shap_rows.iterrows():
        match = match[1]
        match = match.drop(columns=['final_margin', 'match_id'])
        
        for index in shap_vals.index:
            #Check this isn't the bias constant
            if index != 'const':
                shap_vals[index] = sol[index] * (match[index] - expected_vals[index])
        
        total_shap_vals = total_shap_vals + shap_vals
    
    total_shap_vals = total_shap_vals/num_preds
    
    return total_shap_vals


"""
Function which uses the above to calculate SHAP values for each statistic, and therefore give the effectiveness for each statistic
"""


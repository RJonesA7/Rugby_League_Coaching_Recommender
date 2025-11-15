import pandas, numpy
import psycopg2
from similar_teams.similar_teams_z_sum import similar_teams_z_sum
import matplotlib.pyplot as plt
from scipy.stats import linregress

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

"""
Function which takes in a pandas Series of matches with a composite index of match_id and home and values
(as returned by a function in similar_teams)
And outputs an array of tuples (stat, effectiveness) which gives 
target stats to beat the initially input opposition 
and how effective targeting that stat will be for beating the opposition
"""
def first_model_regression(opposition_sides):
    #Get the match_ids we will use to extract teams from the db
    keys = opposition_sides.index

    #We want the teams playing against these teams, so convert to lists so we can access and update them
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
    
    #Create a df to store the results
    results_df = pandas.DataFrame({
        'stat': pandas.Series(dtype="string"),
        'r': pandas.Series(dtype="float"),
        'p': pandas.Series(dtype="float"),
        'grad': pandas.Series(dtype="float")
    })

    #Find the relationship between each statistic and the final_margin
    for stat in stat_df.columns:
        if stat not in ['match_id', 'team', 'is_home', 'final_margin'] and not (stat_df[stat].isna().all()):
            ycol = stat_df["final_margin"]
            xcol = stat_df[stat]
            for i in range(len(xcol)):
                if xcol[i] is None:
                    del xcol[i]
                    del ycol[i]

            #Plot best fit line
            mask = ~numpy.isnan(xcol.to_numpy()) & ~numpy.isnan(ycol.to_numpy())
            res = linregress(xcol.to_numpy()[mask], ycol.to_numpy()[mask])
            
            #Code to plot commented out, used in testing and exploratory analysis
            """
            plt.figure(figsize=(6,4))
            plt.scatter(xcol, ycol, s=12)
            xx = numpy.linspace(xcol.min(), xcol.max(), 100)
            plt.plot(xx, res.slope*xx + res.intercept)
            plt.xlabel(stat); plt.ylabel("final_score"); plt.title("final_score vs " + stat + ", pearson correlation: " + str(round(res.rvalue, 5)) + ", prob slope = 0: " + str(round(res.pvalue, 10)))
            plt.tight_layout()
            plt.show()
            """

            #Store the relevant values in the results_df
            results_df.loc[len(results_df)] = {'stat': stat, 'r': res.rvalue, 'p': res.pvalue, 'grad': res.slope}
    
    results_df = results_df.sort_values('p', ascending=True)

    return results_df

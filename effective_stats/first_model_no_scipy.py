import pandas, numpy
import psycopg2
from similar_teams.similar_teams_z_sum import similar_teams_z_sum
import matplotlib.pyplot as plt
from scipy.stats import linregress

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

#Data representing the effectiveness of stats against all teams
base_data = [
    {"stat": "score", "r": 0.833942, "weighted_l2_diff": 18.041244, "grad": 1.412911},
    {"stat": "half_time", "r": 0.634903, "weighted_l2_diff": 35.360810, "grad": 1.025586},
    {"stat": "time_in_possession", "r": 0.512705, "weighted_l2_diff": 43.668517, "grad": 0.914105},
    {"stat": "all_runs", "r": 0.512213, "weighted_l2_diff": 43.698392, "grad": 0.915722},
    {"stat": "all_run_metres", "r": 0.661928, "weighted_l2_diff": 33.284603, "grad": 1.155496},
    {"stat": "post_contact_metres", "r": 0.427643, "weighted_l2_diff": 48.407057, "grad": 0.743043},
    {"stat": "line_breaks", "r": 0.633014, "weighted_l2_diff": 35.502700, "grad": 1.084315},
    {"stat": "tackle_breaks", "r": 0.475692, "weighted_l2_diff": 45.835713, "grad": 0.783863},
    {"stat": "average_set_distance", "r": 0.386221, "weighted_l2_diff": 50.404191, "grad": 0.685722},
    {"stat": "kick_return_metres", "r": 0.483321, "weighted_l2_diff": 45.402334, "grad": 0.809328},
    {"stat": "offloads", "r": 0.136551, "weighted_l2_diff": 58.136334, "grad": 0.227645},
    {"stat": "receipts", "r": 0.442746, "weighted_l2_diff": 47.628287, "grad": 0.771247},
    {"stat": "total_passes", "r": 0.344110, "weighted_l2_diff": 52.226123, "grad": 0.587699},
    {"stat": "dummy_passes", "r": 0.038294, "weighted_l2_diff": 59.154075, "grad": 0.063043},
    {"stat": "kicks", "r": 0.200720, "weighted_l2_diff": 56.854218, "grad": 0.346747},
    {"stat": "kicking_metres", "r": 0.064344, "weighted_l2_diff": 58.995682, "grad": 0.109847},
    {"stat": "forced_drop_outs", "r": 0.088483, "weighted_l2_diff": 58.777139, "grad": 0.149446},
    {"stat": "bombs", "r": 0.194363, "weighted_l2_diff": 57.003008, "grad": 0.315512},
    {"stat": "grubbers", "r": 0.138522, "weighted_l2_diff": 58.104209, "grad": 0.229939},
    {"stat": "forty_twenty", "r": 0.201832, "weighted_l2_diff": 6.918729, "grad": 0.321483},
    {"stat": "tackles_made", "r": -0.359053, "weighted_l2_diff": 51.603678, "grad": -0.624802},
    {"stat": "missed_tackles", "r": -0.476255, "weighted_l2_diff": 45.804008, "grad": -0.781918},
    {"stat": "intercepts", "r": 0.110882, "weighted_l2_diff": 58.512596, "grad": 0.185758},
    {"stat": "ineffective_tackles", "r": -0.085274, "weighted_l2_diff": 58.810170, "grad": -0.139961},
    {"stat": "errors", "r": -0.131971, "weighted_l2_diff": 58.209193, "grad": -0.225524},
    {"stat": "penalties_conceded", "r": -0.031654, "weighted_l2_diff": 59.181591, "grad": -0.053839},
    {"stat": "inside_ten_metres", "r": 0.010425, "weighted_l2_diff": 59.234511, "grad": 0.018231},
    {"stat": "ruck_infringements", "r": -0.052350, "weighted_l2_diff": 59.078600, "grad": -0.089773},
    {"stat": "interchanges_used", "r": -0.010644, "weighted_l2_diff": 59.234237, "grad": -0.016227},
    {"stat": "completion_rate", "r": 0.289972, "weighted_l2_diff": 54.259761, "grad": 0.498012},
    {"stat": "average_play_ball_speed", "r": 0.094222, "weighted_l2_diff": 58.715019, "grad": 0.161061},
    {"stat": "kick_defusal", "r": 0.112042, "weighted_l2_diff": 58.497270, "grad": 0.191256},
    {"stat": "effective_tackle", "r": 0.233460, "weighted_l2_diff": 56.012109, "grad": 0.407789},
    {"stat": "tries", "r": 0.805710, "weighted_l2_diff": 20.783573, "grad": 1.389221},
    {"stat": "conversions", "r": 0.761680, "weighted_l2_diff": 24.871967, "grad": 1.255655},
    {"stat": "conversions_missed", "r": 0.210564, "weighted_l2_diff": 56.614382, "grad": 0.359861},
    {"stat": "penalty_goals", "r": 0.255238, "weighted_l2_diff": 55.381624, "grad": 0.450145},
    {"stat": "penalty_goals_missed", "r": -0.042437, "weighted_l2_diff": 59.134262, "grad": -0.082704},
    {"stat": "sin_bins", "r": 0.002678, "weighted_l2_diff": 59.240525, "grad": 0.004172},
    {"stat": "on_reports", "r": -0.158681, "weighted_l2_diff": 57.749289, "grad": -0.270266},
    {"stat": "one_point_field_goals", "r": 0.035420, "weighted_l2_diff": 59.166626, "grad": 0.055111},
    {"stat": "one_point_field_goals_missed", "r": 0.020767, "weighted_l2_diff": 58.284468, "grad": 0.033324},
    {"stat": "two_point_field_goals", "r": 0.034452, "weighted_l2_diff": 59.170633, "grad": 0.057306},
    {"stat": "two_point_field_goals_missed", "r": 0.046898, "weighted_l2_diff": 58.494508, "grad": 0.077461},
]

"""
Function which takes in a pandas Series of matches with a composite index of match_id and home and values
(as returned by a function in similar_teams, representing similar teams in the database that we want to consider)
And outputs an array of tuples (stat, effectiveness) which gives 
target stats to beat the initially input opposition 
and how effective targeting that stat will be for beating the opposition
"""
def first_model_no_scipy(weights):
    #Set the base data which we will compare to
    base_data_df = pandas.DataFrame(base_data)
    base_data_df = base_data_df.set_index('stat')
    
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
    weights = 1/weights
    stat_df['weight'] = weights

    #Reset the index now we have combined
    stat_df = stat_df.reset_index()

    #Create a df to store the results
    results_df = pandas.DataFrame({
        'stat': pandas.Series(dtype="string"),
        'r': pandas.Series(dtype="float"),
        'weighted_l2_diff': pandas.Series(dtype="float"),
        'grad': pandas.Series(dtype="float")
    })

    #Initialise the numerator and denominator, to be repeatedly used for calculating fractions
    num = 0
    den = 0

    #Find the relationship between each statistic and the final_margin
    for stat in stat_df.columns:
        #Ignore non-statistic columns in the df
        if stat not in ['team', 'is_home', 'final_margin', 'weight', 'match_id'] and not (stat_df[stat].isna().all()):
            ycol = stat_df["final_margin"].to_numpy()
            xcol = stat_df[stat].to_numpy()
            weights = stat_df['weight'].to_numpy()

            #Ignore matches which don't have a value for this stat, using numpy filtering
            filter_list = []
            for i in range(len(stat_df)):
                if xcol[i] is None or numpy.isnan(xcol[i]):
                    filter_list.append(False)
                else:
                    filter_list.append(True)
            ycol = ycol[filter_list]
            xcol = xcol[filter_list]
            weights = weights[filter_list]

            #Calculate the weighted means for use in calculating weights
            x_wm = 0
            y_wm = 0

            for i in range(len(xcol)):
                x_wm = x_wm + xcol[i] * weights[i]
                y_wm = y_wm + ycol[i] * weights[i]

            x_wm = x_wm / weights.sum()
            y_wm = y_wm / weights.sum()

            #Caclulate grad and y-int to minimise least squares - see report for calculation
            num = 0
            den = 0
            for i in range(len(xcol)):
                num = num + weights[i] * (xcol[i] - x_wm) * (ycol[i] - y_wm)
                den = den + weights[i] * ((xcol[i] - x_wm) ** 2)
            grad = num/den
            y_int = y_wm - grad * x_wm

            #Calculate total weighted L2 norm difference to line, as minimised by the above calculations
            diff = 0
            for i in range(len(xcol)):
                diff = diff + weights[i] * ((y_int + grad * xcol[i] - ycol[i]))**2

            #Calculate weighted Pearson correlation coefficient to know whether this line of best fit is reliable
            num = 0
            den_x = 0
            den_y = 0
            for i in range(len(xcol)):
                num = num + weights[i] * (xcol[i] - x_wm) * (ycol[i] - y_wm)
                den_x = den_x + weights[i] * ((xcol[i] - x_wm) ** 2)
                den_y = den_y + weights[i] * ((ycol[i] - y_wm) ** 2)
            den = (den_x * den_y) ** (1/2)
            r = num/den

            """
            #Code to plot, used in testing and exploratory analysis
            plt.figure(figsize=(12,8))
            plt.scatter(xcol, ycol, s=12)
            xx = numpy.linspace(xcol.min(), xcol.max(), 100)
            plt.plot(xx, grad*xx + y_int)
            plt.xlabel(stat); plt.ylabel("final_score"); plt.title("final_score vs "  + stat + ", weighted L2 diff: " + str(round(diff, 3)) + ", pearson correlation: " + str(round(r, 3)) +", gradient: " + str(round(grad, 2)))
            plt.tight_layout()
            plt.show()
            """

            #Store the relevant values in the results_df
            results_df.loc[len(results_df)] = {'stat': stat, 'r': r, 'weighted_l2_diff': diff, 'grad': grad}
    
    results_df = results_df.sort_values('r', ascending=False)
    results_df = results_df.set_index('stat')

    #Subtract standard correlations and gradients so that the recommendations are what works better than normal rather than just what works
    #results_df = results_df - base_data_df

    return results_df
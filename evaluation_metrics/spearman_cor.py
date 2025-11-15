import psycopg2, pandas, numpy

""" Function to take in recommended stats for a given opponent and return a number of hits and misses, 
corresponding to whether or not the team which beat the given opposition had high z-scores in the recommended stats.
This is a base comparison metric to be improved on in future iterations. It is intended to be run with a given opposition
that won the match, to see whether winning teams did as the model recommended.
"""

def spearman_cor(opposition, recommendations):
    conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)
    #Extract opposition data to find stats
    match_id = opposition['match_id']
    is_home = not opposition['is_home']
    stats_query = """
    select team_stats_z.*
    from team_stats_z
    where match_id = {match_id} and is_home = {is_home}
    """.format(match_id = str(match_id), is_home = str(is_home))
    
    stat_df = pandas.read_sql_query(stats_query, conn)
    #Drop the non-statistical metadata then transpose to get a series as desired
    stat_df = stat_df[[c for c in stat_df.columns if c not in ['team', 'match_id', 'is_home']]]
    stat_df = stat_df.transpose()
    
    #Delete rows from both dfs where there is no recommendation or no stat
    stats = stat_df.index
    for stat in stats:
        if stat_df.at[stat, 0] is None or recommendations.at[stat, 'r'] is None:
            stat_df = stat_df.drop(index = [stat])
            recommendations = recommendations.drop(index = [stat])

    stat_df = stat_df.sort_values(0, ascending=False)
    recommendations = recommendations.sort_values('r', ascending=False)

    #We can't calculate Spearman with the sum formula since there will be exact ranking matches, so compute cov and std instead
    #Assign numerical rankings to the recommendations for comparison with rankings of the achieved stats
    stats = stat_df.index
    i = 0
    stats_rankings = []
    recs_rankings = []
    for stat in stats:
        recs_rankings.append(recommendations.index.get_loc(stat) + 1)
        stats_rankings.append(i+1)
        i = i + 1

    cov = numpy.cov(stats_rankings, recs_rankings)
    stats_stdev = numpy.std(stats_rankings)
    recs_stdev = numpy.std(recs_rankings)

    spearman = cov[0,1]/(stats_stdev * recs_stdev)

    return spearman





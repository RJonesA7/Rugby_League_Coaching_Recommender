import pandas, numpy
import psycopg2
from sqlalchemy import create_engine, inspect

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)


engine = create_engine(
    "postgresql+psycopg2:///nrl_data",  
    connect_args={
        "host": "/dcs/23/u5503037/CS344/pgsock",
        "port": 5432
    }
)

stats_select_query = """
select team_stats.*, match_data.season
from team_stats join match_data on team_stats.match_id = match_data.match_id 
where match_data.season = %(season)s
"""

re_insert_query = """

"""

for season in range(2001, 2026):
    stat_df = pandas.read_sql_query(stats_select_query, conn, params={"season": season})
    #Drop season as we no longer need it
    stat_df = stat_df.drop(columns=['season'])

    #drop the stats we don't want to calculate z-scores for
    normalised_stat_df = stat_df.drop(columns=['match_id', 'is_home', 'team'])

    #Turn entire df into column-wise z-scores,
    #If the stdev is 0, set the whole column to 0 and don't do the calculation 
    for col in normalised_stat_df.columns:
        if normalised_stat_df.loc[:, [col]].std().all() == 0:
            normalised_stat_df.loc[:, [col]] = 0
        else:
            normalised_stat_df.loc[:, [col]] = (normalised_stat_df.loc[:, [col]] - normalised_stat_df.loc[:, [col]].mean()) / normalised_stat_df.loc[:, [col]].std()


    #Recombine with relevant info from original df
    normalised_stat_df = pandas.concat([normalised_stat_df, stat_df.loc[:, ['match_id', 'is_home', 'team']]], axis=1)

    #Insert the edited df into the database
    normalised_stat_df.to_sql("team_stats_z", if_exists = 'append', index = False, method = 'multi', con=engine)
    print("inserted z-scores for " + str(season)), 




conn.close()
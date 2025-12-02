#Code below is for empirical testing, as used in iteration 1

#Find empirical metric performance by training on 2001-2023, testing on 2024-25

#Find data representing relevant sides
losing_teams_query = """
select team_stats_z.*
from team_stats_z join team_stats_z opp_stats_z on team_stats_z.match_id = opp_stats_z.match_id and team_stats_z.is_home != opp_stats_z.is_home
where (team_stats_z.score - opp_stats_z.score) < 0 and (select season from match_data where match_data.match_id = team_stats_z.match_id) > 2023
"""
losing_teams = pandas.read_sql_query(losing_teams_query, conn)

n = 0
spearman_tot = 0
for team in losing_teams.iterrows():
	team = team[1]
	team = team.to_dict()
	keys = list(team.keys())
	represantion = {}
	for key in keys:
		if team[key] is None:
			del team[key]
		elif key not in ["match_id", "is_home", "team", "one_point_field_goals", "two_point_field_goals", "one_point_field_goals_missed", "two_point_field_goals_missed"]:
			represantion[key] = team[key]
		
	recommendations = first_model_no_scipy(similar_teams_z_sum(represantion, 100))
	res = spearman_cor({"match_id": team["match_id"], "is_home": team["is_home"]}, recommendations)
	spearman_tot = spearman_tot + res
	print(res)
	n = n + 1

print("Overall average Spearman correlation coefficient for stats recommended by the model and stats of winning teams: " + str(spearman_tot/n))
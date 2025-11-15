from similar_teams.similar_teams_z_sum import similar_teams_z_sum
from effective_stats.first_model_regression import first_model_regression
from effective_stats.first_model_no_scipy import first_model_no_scipy
from evaluation_metrics.spearman_cor import spearman_cor

import pandas, numpy
import psycopg2

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

opposition_representation = {
  "score": 0,
  "half_time": 0,
  "time_in_possession": 0,
  "all_runs": 0,
  "all_run_metres": 0,
  "post_contact_metres": 0,
  "line_breaks": 0,
  "tackle_breaks": 0,
  "average_set_distance": 0,
  "kick_return_metres": 0,
  "offloads": 0,
  "receipts": 0,
  "total_passes": 0,
  "dummy_passes": 0,
  "kicks": 0,
  "kicking_metres": 0,
  "forced_drop_outs": 0,
  "bombs": 0,
  "grubbers": 0,
  #"forty_twenty": 0,
  "tackles_made": 0,
  "missed_tackles": 0,
  "intercepts": 0,
  "ineffective_tackles": 0,
  "errors": 0,
  "penalties_conceded": 0,
  "ruck_infringements": 0,
  "inside_ten_metres": 0,
  "interchanges_used": 0,
  "completion_rate": 0,
  "average_play_ball_speed": 0,
  "kick_defusal": 0,
  "effective_tackle": 0,
  "tries": 0,
  "conversions": 0,
  "conversions_missed": 0,
  "penalty_goals": 0,
  "penalty_goals_missed": 0,
  "sin_bins": 0,
  "on_reports": 0,
  #"one_point_field_goals": 0,
  #"one_point_field_goals_missed": 0,
  #"two_point_field_goals": 0,
  #"two_point_field_goals_missed": 0,
}

"""
opposition_sides = similar_teams_z_sum(opposition_representation, 200)
print(first_model_no_scipy(opposition_sides))
"""

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



from similar_teams.similar_teams_z_sum import similar_teams_z_sum
from effective_stats.first_model_regression import first_model_regression

opposition_representation = {
    "tries": -0.5,
	"all_run_metres": -0.5,
	"all_runs": -0.5,
	"time_in_possession": 0,
	"line_breaks": -1,
	"missed_tackles": -1,
	"post_contact_metres": 1,
	"tackle_breaks": -0.5,
	"effective_tackle": 0.5,
	"total_passes": -1,
	"completion_rate": 0.5,
	"kicking_metres": 1,
	"tackles_made": 1,
	"offloads": -0.5,
    "forced_drop_outs": 0.5,
}

opposition_sides = similar_teams_z_sum(opposition_representation, 200)
print(first_model_regression(opposition_sides))
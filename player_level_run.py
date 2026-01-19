from similar_teams.similar_pos_groups_filtered import similar_pos_groups_filtered
from effective_stats.player_level_multi_regression import player_level_multi_regression
from effective_stats.static_data.matchup_graph import MATCHUP_WEIGHTS

import pandas, numpy
import psycopg2
base_stats = {
    "mins_played": 0,
    #"stint_one": 0,
    #"stint_two": 0,

    "points": 0,
    "tries": 0,
    "conversions": 0,
    "conversion_attempts": 0,
    "penalty_goals": 0,
    "goal_conversion_rate": 0,
    "one_point_field_goals": 0,
    "two_point_field_goals": 0,
    "fantasy_points": 0,

    "all_runs": 0,
    "all_run_metres": 0,
    "hit_ups": 0,
    "post_contact_metres": 0,
    "kick_return_metres": 0,
    "line_engaged_runs": 0,

    "line_breaks": 0,
    "line_break_assists": 0,
    "try_assists": 0,
    "tackle_breaks": 0,

    "play_the_ball": 0,
    "average_play_the_ball_speed": 0,
    "receipts": 0,
    "passes": 0,
    "dummy_passes": 0,
    "offloads": 0,
    "passes_to_run_ratio": 0,
    "dummy_half_runs": 0,
    "dummy_half_run_metres": 0,

    "tackles_made": 0,
    "missed_tackles": 0,
    "ineffective_tackles": 0,
    "tackle_efficiency": 0,
    "intercepts": 0,
    "one_on_one_steal": 0,
    "one_on_one_lost": 0,

    "errors": 0,
    "handling_errors": 0,
    "penalties": 0,
    "ruck_infringements": 0,
    "inside_10_metres": 0,
    "on_report": 0,
    "sin_bins": 0,
    "send_offs": 0,

    "kicks": 0,
    "kicking_metres": 0,
    "forced_drop_outs": 0,
    "bomb_kicks": 0,
    "grubbers": 0,
    "forty_twenty": 0,
    "twenty_forty": 0,
    "cross_field_kicks": 0,
    "kicked_dead": 0,
    "kicks_defused": 0,
}


conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)



opposition_middles = similar_pos_groups_filtered(base_stats, "middles", 4000)

positions = [
    "middles",
    "hooker",
    "second_rows",
    "halves",
    "centres",
    "wingers",
    "fullback",
]

results_by_position = {our_pos: {} for our_pos in positions}

for opp_pos in positions:
    opposition_players = similar_pos_groups_filtered(base_stats, opp_pos, 1000)
    print(f"\nOpposition group: {opp_pos}  (n={len(opposition_players)})")

    # Split once per opposition group (same folds reused for all our_pos)
    chunks = numpy.array_split(opposition_players.to_frame(), 5)

    for our_pos in positions:
        tot_res = None

        for i in range(5):
            curr_chunk = chunks[i].copy()
            other_chunks = pandas.concat([chunks[j] for j in range(5) if j != i])

            curr_chunk["validation"] = True
            other_chunks["validation"] = False

            curr_input = pandas.concat([curr_chunk, other_chunks])

            res = player_level_multi_regression(curr_input, our_pos)
            tot_res = res if tot_res is None else tot_res + res

        results_by_position[our_pos][opp_pos] = tot_res


final_recs = {}

for our_pos, opp_dict in results_by_position.items():
    total = None

    for opp_pos, rec in opp_dict.items():
        w = MATCHUP_WEIGHTS[our_pos][opp_pos] / 100.0
        contrib = rec * w

        total = contrib if total is None else total.add(contrib, fill_value=0)

    final_recs[our_pos] = total

print(final_recs)

    
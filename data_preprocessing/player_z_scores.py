import pandas
import psycopg2
from sqlalchemy import create_engine

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

engine = create_engine(
    "postgresql+psycopg2:///nrl_data",
    connect_args={
        "host": "/dcs/23/u5503037/CS344/pgsock",
        "port": 5432
    }
)

player_select_query = """
SELECT ps.*, md.season
FROM player_stats ps
JOIN match_data md ON ps.match_id = md.match_id
WHERE md.season = %(season)s
"""

# Columns that exist in player_stats_z and should NOT be z-scored
ID_COLS = ["match_id", "name", "number", "position", "season"]

# Exact z-table stat columns (must match your CREATE TABLE player_stats_z)
Z_COLS = [
    "mins_played", "stint_one", "stint_two",
    "points", "tries", "conversions", "conversion_attempts", "penalty_goals",
    "goal_conversion_rate", "one_point_field_goals", "two_point_field_goals", "fantasy_points",
    "all_runs", "all_run_metres", "hit_ups", "post_contact_metres", "kick_return_metres", "line_engaged_runs",
    "line_breaks", "line_break_assists", "try_assists", "tackle_breaks",
    "play_the_ball", "average_play_the_ball_speed", "receipts", "passes", "dummy_passes", "offloads",
    "passes_to_run_ratio", "dummy_half_runs", "dummy_half_run_metres",
    "tackles_made", "missed_tackles", "ineffective_tackles", "tackle_efficiency", "intercepts",
    "one_on_one_steal", "one_on_one_lost",
    "errors", "handling_errors", "penalties", "ruck_infringements", "inside_10_metres",
    "on_report", "sin_bins", "send_offs",
    "kicks", "kicking_metres", "forced_drop_outs", "bomb_kicks", "grubbers", "forty_twenty", "twenty_forty",
    "cross_field_kicks", "kicked_dead", "kicks_defused",
]

for season in range(2001, 2026):
    df = pandas.read_sql_query(player_select_query, conn, params={"season": season})

    if df.empty:
        print(f"no rows for {season}")
        continue

    # Keep only columns that exist (safe if some seasons lack some cols)
    present_z_cols = [c for c in Z_COLS if c in df.columns]

    # Base output has identifier columns ONLY that exist in player_stats_z
    id_df = df.loc[:, [c for c in ["match_id", "name", "number", "position"] if c in df.columns]].copy()

    # Numeric data to z-score
    z_df = df.loc[:, present_z_cols].copy()

    # Column-wise z-score with std==0 guard (NaN-safe)
    for col in z_df.columns:
        col_std = z_df[col].std()
        if pandas.isna(col_std) or col_std == 0:
            z_df[col] = 0
        else:
            z_df[col] = (z_df[col] - z_df[col].mean()) / col_std

    out_df = pandas.concat([id_df, z_df], axis=1)

    # IMPORTANT: out_df columns should be a subset of player_stats_z columns.
    out_df.to_sql(
        "player_stats_z",
        if_exists="append",
        index=False,
        method="multi",
        con=engine
    )

    print("inserted player z-scores for " + str(season))

conn.close()

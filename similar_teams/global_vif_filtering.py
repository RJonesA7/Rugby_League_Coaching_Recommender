import pandas
import numpy as np
import psycopg2
from statsmodels.stats.outliers_influence import variance_inflation_factor

conn = psycopg2.connect(
    dbname="nrl_data",
    host="/dcs/23/u5503037/CS344/pgsock",
    port=5432
)

"""
Iteratively VIF-filter team statistics from team_stats_z until only the
target number of features remain.

Outputs:
- remaining stats
- elimination order
- VIF table at each iteration
"""

VALID_STATS = [
    "score", "half_time", "time_in_possession",
    "all_runs", "all_run_metres", "post_contact_metres",
    "line_breaks", "tackle_breaks", "average_set_distance",
    "kick_return_metres", "offloads", "receipts",
    "total_passes", "dummy_passes", "kicks", "kicking_metres",
    "forced_drop_outs", "bombs", "grubbers", "forty_twenty",
    "tackles_made", "missed_tackles", "intercepts", "ineffective_tackles",
    "errors", "penalties_conceded", "ruck_infringements", "inside_ten_metres",
    "interchanges_used", "completion_rate", "average_play_ball_speed",
    "kick_defusal", "effective_tackle", "tries", "conversions",
    "conversions_missed", "penalty_goals", "penalty_goals_missed",
    "sin_bins", "on_reports"
]


def fetch_team_stats_z(candidate_stats, conn):
    """
    Pull candidate stats from team_stats_z, restricting to complete rows
    for the chosen candidate columns.
    """
    for stat in candidate_stats:
        if stat not in VALID_STATS:
            raise ValueError(f"invalid statistic {stat}")

    stats_select_query = """
        SELECT {columns}
        FROM team_stats_z
        WHERE {not_nulls}
    """.format(
        columns=", ".join(candidate_stats),
        not_nulls=" IS NOT NULL AND ".join(candidate_stats) + " IS NOT NULL"
    )

    df = pandas.read_sql_query(stats_select_query, conn)
    return df


def compute_vif_table(df):
    """
    Compute VIF for every column in df.
    Assumes df contains only numeric predictor columns.
    """
    vif_data = []

    # statsmodels VIF can misbehave with NaNs / infs
    X = df.replace([np.inf, -np.inf], np.nan).dropna(axis=0)

    # Also drop constant columns if any slipped through
    nunique = X.nunique(dropna=False)
    constant_cols = nunique[nunique <= 1].index.tolist()
    if constant_cols:
        X = X.drop(columns=constant_cols)

    if X.shape[1] < 2:
        raise ValueError("Need at least two non-constant columns to compute VIF.")

    for i, col in enumerate(X.columns):
        vif_val = variance_inflation_factor(X.values, i)
        vif_data.append({
            "stat": col,
            "vif": float(vif_val)
        })

    vif_df = pandas.DataFrame(vif_data).sort_values(by="vif", ascending=False).reset_index(drop=True)
    return vif_df, constant_cols, X.shape[0]


def iterative_vif_filter(candidate_stats, conn, target_num_stats=15, verbose=True):
    """
    Iteratively remove the stat with the highest VIF until target_num_stats remain.

    Returns a dict containing:
    - remaining_stats
    - removal_order
    - final_vif_table
    - iteration_log
    """
    if len(candidate_stats) <= target_num_stats:
        raise ValueError("candidate_stats must contain more stats than target_num_stats.")

    remaining_stats = candidate_stats.copy()
    removal_order = []
    iteration_log = []

    iteration = 1

    while len(remaining_stats) > target_num_stats:
        df = fetch_team_stats_z(remaining_stats, conn)
        vif_df, constant_cols, n_rows_used = compute_vif_table(df)

        # If constant columns exist, remove them first
        if constant_cols:
            for const_col in constant_cols:
                if const_col in remaining_stats:
                    remaining_stats.remove(const_col)
                    removal_order.append({
                        "iteration": iteration,
                        "removed_stat": const_col,
                        "reason": "constant column",
                        "vif_at_removal": np.nan,
                        "rows_used": n_rows_used,
                        "num_stats_remaining_after_removal": len(remaining_stats)
                    })
                    if verbose:
                        print(
                            f"Iteration {iteration}: removed {const_col} "
                            f"(reason: constant column). {len(remaining_stats)} stats remain."
                        )
                    iteration += 1

            if len(remaining_stats) <= target_num_stats:
                break

            continue

        worst_stat = vif_df.iloc[0]["stat"]
        worst_vif = vif_df.iloc[0]["vif"]

        iteration_log.append({
            "iteration": iteration,
            "rows_used": n_rows_used,
            "vif_table": vif_df.copy(),
            "removed_stat": worst_stat,
            "removed_vif": worst_vif,
            "num_stats_before_removal": len(remaining_stats)
        })

        remaining_stats.remove(worst_stat)
        removal_order.append({
            "iteration": iteration,
            "removed_stat": worst_stat,
            "reason": "highest VIF",
            "vif_at_removal": worst_vif,
            "rows_used": n_rows_used,
            "num_stats_remaining_after_removal": len(remaining_stats)
        })

        if verbose:
            print(
                f"Iteration {iteration}: removed {worst_stat} "
                f"(VIF = {worst_vif:.4f}). {len(remaining_stats)} stats remain. "
                f"Rows used = {n_rows_used}"
            )

        iteration += 1

    final_df = fetch_team_stats_z(remaining_stats, conn)
    final_vif_df, _, final_rows_used = compute_vif_table(final_df)

    if verbose:
        print("\nFinal remaining stats:")
        for stat in remaining_stats:
            print(f" - {stat}")

        print("\nFinal VIF table:")
        print(final_vif_df.to_string(index=False))

        print("\nRemoval order:")
        print(pandas.DataFrame(removal_order).to_string(index=False))

        print(f"\nFinal rows used: {final_rows_used}")

    return {
        "remaining_stats": remaining_stats,
        "removal_order": removal_order,
        "final_vif_table": final_vif_df,
        "iteration_log": iteration_log,
        "final_rows_used": final_rows_used
    }


if __name__ == "__main__":
    # Choose candidate stats to begin with.
    # Exclude direct outcome variables and obvious scoreboard variables here,
    # because otherwise VIF selection may keep stats that are strong reflections of scoring
    # rather than escriptors.
    candidate_stats = [
        "time_in_possession",
        "all_runs",
        "all_run_metres",
        "post_contact_metres",
        "line_breaks",
        "tackle_breaks",
        "average_set_distance",
        "kick_return_metres",
        "offloads",
        "receipts",
        "total_passes",
        "dummy_passes",
        "kicks",
        "kicking_metres",
        "forced_drop_outs",
        "bombs",
        "grubbers",
        "forty_twenty",
        "tackles_made",
        "missed_tackles",
        "intercepts",
        "ineffective_tackles",
        "errors",
        "penalties_conceded",
        "ruck_infringements",
        "inside_ten_metres",
        "interchanges_used",
        "completion_rate",
        "average_play_ball_speed",
        "kick_defusal",
        "effective_tackle",
        "sin_bins",
        "on_reports"
    ]

    results = iterative_vif_filter(
        candidate_stats=candidate_stats,
        conn=conn,
        target_num_stats=15,
        verbose=True
    )

    removal_order_df = pandas.DataFrame(results["removal_order"])
    final_vif_df = results["final_vif_table"]

    # Save outputs for inspection
    removal_order_df.to_csv("vif_removal_order.csv", index=False)
    final_vif_df.to_csv("final_15_stat_vifs.csv", index=False)
from flask import Flask, render_template, request

from webapp_run_functions import multilinear_regression_run, player_level_multi_regression_run


app = Flask(__name__, template_folder="html", static_folder="webapp_static")


TEAM_STATS = [
    "all_run_metres", "post_contact_metres", "line_breaks", "offloads",
    "total_passes", "kicking_metres", "penalties_conceded", "effective_tackle",
    "completion_rate", "average_play_ball_speed"
]

POSITION_STATS = {
    "middles": [
        "mins_played",
        "hit_ups",
        "all_runs",
        "all_run_metres",
        "post_contact_metres",
        "line_engaged_runs",
        "average_play_the_ball_speed",
        "tackles_made",
        "tackle_efficiency",
        "passes",
    ],
    "hooker": [
        "mins_played",
        "receipts",
        "passes",
        "passes_to_run_ratio",
        "dummy_half_runs",
        "dummy_half_run_metres",
        "average_play_the_ball_speed",
        "tackles_made",
        "tackle_efficiency",
    ],
    "fullback": [
        "mins_played",
        "kick_return_metres",
        "all_run_metres",
        "line_breaks",
        "line_break_assists",
        "tackle_breaks",
        "kicking_metres",
        "errors",
        "tackle_efficiency",
    ],
    "second_rows": [
        "mins_played",
        "line_engaged_runs",
        "all_run_metres",
        "post_contact_metres",
        "offloads",
        "tackle_breaks",
        "line_breaks",
        "tackles_made",
        "tackle_efficiency",
        "errors",
    ],
    "halves": [
        "mins_played",
        "passes",
        "passes_to_run_ratio",
        "try_assists",
        "line_break_assists",
        "kicks",
        "kicking_metres",
        "forced_drop_outs",
        "errors",
    ],
    "centres": [
        "mins_played",
        "all_run_metres",
        "post_contact_metres",
        "line_breaks",
        "try_assists",
        "tackle_breaks",
        "offloads",
        "intercepts",
        "missed_tackles",
        "errors",
    ],
    "wingers": [
        "mins_played",
        "kick_return_metres",
        "all_run_metres",
        "tackle_breaks",
        "line_breaks",
        "tries",
        "kicks_defused",
        "handling_errors",
        "missed_tackles",
        "errors",
    ],
}

POSITION_ORDER = [
    "middles",
    "hooker",
    "second_rows",
    "halves",
    "centres",
    "wingers",
    "fullback",
]


def to_title(text: str) -> str:
    return text.replace("_", " ").strip().title()


def format_obj(obj):
    if isinstance(obj, dict):
        return {to_title(k): format_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [format_obj(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(format_obj(x) for x in obj)
    return obj


TEAM_STATS_FMT = [to_title(s) for s in TEAM_STATS]
POSITION_ORDER_FMT = [to_title(p) for p in POSITION_ORDER]
POSITION_STATS_FMT = {to_title(p): [to_title(s) for s in stats] for p, stats in POSITION_STATS.items()}


def parse_float(value: str, default: float = 0.0) -> float:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return max(-3.0, min(3.0, x))


def default_player_values() -> dict:
    return {
        pos: {stat: 0.0 for stat in POSITION_STATS[pos]}
        for pos in POSITION_ORDER
    }


@app.get("/player")
def player():
    return render_template(
        "player.html",
        position_order=POSITION_ORDER_FMT,
        position_stats=POSITION_STATS_FMT,
        values=format_obj(default_player_values()),
        output=None,
    )


@app.post("/player")
def player_post():
    values = {}
    for pos in POSITION_ORDER:
        values[pos] = {}
        for stat in POSITION_STATS[pos]:
            field = f"{pos}__{stat}"
            values[pos][stat] = parse_float(request.form.get(field, "0"))

    grouped_final_recs, box_plot_grouped_final_recs = player_level_multi_regression_run(values)

    # Convert grouped_final_recs[pos] (Series) -> plain dict (ordered)
    output = {}
    for pos in POSITION_ORDER:
        s = grouped_final_recs.get(pos, None)
        if s is None:
            output[pos] = {}
        else:
            # keep ordering from Series (already sorted in your runner)
            output[pos] = {k: float(v) for k, v in s.items()}

    # box_plot_grouped_final_recs is already pos -> ordered dict of group -> [avg, stats]
    # Ensure float-serialisable
    box_data = {}
    for pos in POSITION_ORDER:
        d = box_plot_grouped_final_recs.get(pos, {})
        box_data[pos] = {
            g: [float(v[0]), [float(x) for x in v[1]]]
            for g, v in d.items()
        }

    return render_template(
        "player.html",
        position_order=POSITION_ORDER_FMT,
        position_stats=POSITION_STATS_FMT,
        values=format_obj(values),
        output=format_obj(output), # numeric list display
        box_data=format_obj(box_data), # keep raw numeric for JS
    )


@app.get("/team")
def team():
    defaults = {s: 0.0 for s in TEAM_STATS}
    return render_template("team.html", stats=TEAM_STATS_FMT, values=format_obj(defaults), output=None)


@app.post("/team")
def team_post():
    values = {s: parse_float(request.form.get(s, "0")) for s in TEAM_STATS}

    output = multilinear_regression_run(values)

    base_output = output[0]
    base_output.pop("const", None)

    box_output = output[1]
    box_output.pop("const", None)

    # Order box plot stats by absolute mean SHAP (index 2 in [min,q1,mean,q3,max])
    box_output = dict(
        sorted(
            box_output.items(),
            key=lambda item: item[1][2],
            reverse=True
        )
    )

    # Also order the numerical output to match
    base_output = dict(
        sorted(
            base_output.items(),
            key=lambda item: item[1],
            reverse=True
        )
    )

    return render_template(
        "team.html",
        stats=TEAM_STATS_FMT,
        values=format_obj(values),
        output=format_obj(base_output),
        box_data=box_output
    )

@app.get("/")
def index():
    return render_template("index.html")


@app.get("/about")
def about():
    return render_template("about.html")


if __name__ == "__main__":
    app.run(debug=True)

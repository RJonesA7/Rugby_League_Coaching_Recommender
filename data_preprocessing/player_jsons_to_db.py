import json, psycopg2
from psycopg2 import sql

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)

#A much more structured setup for transferring data across, lessons learned from match data

def mmss_to_seconds(mmss):
    if mmss is None or mmss == "-" or mmss == "":
        return None
    try:
        mm, ss = mmss.split(":")
        return int(mm) * 60 + int(ss)
    except:
        return None

def to_int(x):
    if x is None or x == "-" or x == "":
        return None
    try:
        return int(str(x).replace(",", ""))
    except:
        return None

def to_float(x):
    if x is None or x == "-" or x == "":
        return None
    try:
        return float(str(x).replace(",", ""))
    except:
        return None

def to_percent(x):
    # "93.1%" -> 93.1
    if x is None or x == "-" or x == "":
        return None
    s = str(x).strip()
    if s.endswith("%"):
        s = s[:-1]
    return to_float(s)

def to_seconds_float(x):
    # "3.31s" -> 3.31
    if x is None or x == "-" or x == "":
        return None
    s = str(x).strip()
    if s.endswith("s"):
        s = s[:-1]
    return to_float(s)

def parse_match_key(match_key: str):
    """
    E.g. "2025-1-Sea-Eagles-v-Cowboys"
    season + round are the first two '-' tokens; remainder split on '-v-'
    """
    season = match_key.split("-", 2)[0]
    rnd = match_key.split("-", 2)[1]
    rest = match_key.split("-", 2)[2]
    try:
        home_slug, away_slug = rest.split("-v-")
    except:
        home_slug, away_slug = None, None

    home = home_slug.replace("-", " ") if home_slug else None
    away = away_slug.replace("-", " ") if away_slug else None

    return to_int(season), to_int(rnd), home, away

# Map JSON keys -> (db_column, converter) to force compatibility with database
KEYMAP = {
    # identifiers
    "Name": ("name", lambda v: None if v in [None, "-", ""] else str(v)),
    "Number": ("number", to_int),
    "Position": ("position", lambda v: None if v in [None, "-", ""] else str(v)),

    # time
    "Mins Played": ("mins_played", mmss_to_seconds),
    "Stint One": ("stint_one", mmss_to_seconds),
    "Stint Two": ("stint_two", mmss_to_seconds),

    # scoring
    "Points": ("points", to_int),
    "Tries": ("tries", to_int),
    "Conversions": ("conversions", to_int),
    "Conversion Attempts": ("conversion_attempts", to_int),
    "Penalty Goals": ("penalty_goals", to_int),
    "Goal Conversion Rate": ("goal_conversion_rate", to_percent),
    "1 Point Field Goals": ("one_point_field_goals", to_int),
    "2 Point Field Goals": ("two_point_field_goals", to_int),
    "Total Points": ("fantasy_points", to_int),

    # running / metres
    "All Runs": ("all_runs", to_int),
    "All Run Metres": ("all_run_metres", to_int),
    "Hit Ups": ("hit_ups", to_int),
    "Post Contact Metres": ("post_contact_metres", to_int),
    "Kick Return Metres": ("kick_return_metres", to_int),
    "Line Engaged Runs": ("line_engaged_runs", to_int),

    # attacking creation
    "Line Breaks": ("line_breaks", to_int),
    "Line Break Assists": ("line_break_assists", to_int),
    "Try Assists": ("try_assists", to_int),
    "Tackle Breaks": ("tackle_breaks", to_int),

    # ruck / handling / distribution
    "Play The Ball": ("play_the_ball", to_int),
    "Average Play The Ball Speed": ("average_play_the_ball_speed", to_seconds_float),
    "Receipts": ("receipts", to_int),
    "Passes": ("passes", to_int),
    "Dummy Passes": ("dummy_passes", to_int),
    "Offloads": ("offloads", to_int),
    "Passes To Run Ratio": ("passes_to_run_ratio", to_float),
    "Dummy Half Runs": ("dummy_half_runs", to_int),
    "Dummy Half Run Metres": ("dummy_half_run_metres", to_int),

    # defence
    "Tackles Made": ("tackles_made", to_int),
    "Missed Tackles": ("missed_tackles", to_int),
    "Ineffective Tackles": ("ineffective_tackles", to_int),
    "Tackle Efficiency": ("tackle_efficiency", to_percent),
    "Intercepts": ("intercepts", to_int),
    "One On One Steal": ("one_on_one_steal", to_int),
    "One On One Lost": ("one_on_one_lost", to_int),

    # discipline / errors
    "Errors": ("errors", to_int),
    "Handling Errors": ("handling_errors", to_int),
    "Penalties": ("penalties", to_int),
    "Ruck Infringements": ("ruck_infringements", to_int),
    "Inside 10 Metres": ("inside_10_metres", to_int),
    "On Report": ("on_report", to_int),
    "Sin Bins": ("sin_bins", to_int),
    "Send Offs": ("send_offs", to_int),

    # kicking
    "Kicks": ("kicks", to_int),
    "Kicking Metres": ("kicking_metres", to_int),
    "Forced Drop Outs": ("forced_drop_outs", to_int),
    "Bomb Kicks": ("bomb_kicks", to_int),
    "Grubbers": ("grubbers", to_int),
    "40/20": ("forty_twenty", to_int),
    "20/40": ("twenty_forty", to_int),
    "Cross Field Kicks": ("cross_field_kicks", to_int),
    "Kicked Dead": ("kicked_dead", to_int),
    "Kicks Defused": ("kicks_defused", to_int),
}

PLAYER_STATS_COLS = [
    # match identifiers
    "season", "round", "home", "away", "match_key", "match_id",
    # player identifiers
    "name", "number", "position",
    # time
    "mins_played", "stint_one", "stint_two",
    # scoring
    "points", "tries", "conversions", "conversion_attempts", "penalty_goals",
    "goal_conversion_rate", "one_point_field_goals", "two_point_field_goals", "fantasy_points",
    # running / metres
    "all_runs", "all_run_metres", "hit_ups", "post_contact_metres", "kick_return_metres", "line_engaged_runs",
    # attacking
    "line_breaks", "line_break_assists", "try_assists", "tackle_breaks",
    # ruck/handling
    "play_the_ball", "average_play_the_ball_speed", "receipts", "passes", "dummy_passes", "offloads",
    "passes_to_run_ratio", "dummy_half_runs", "dummy_half_run_metres",
    # defence
    "tackles_made", "missed_tackles", "ineffective_tackles", "tackle_efficiency", "intercepts",
    "one_on_one_steal", "one_on_one_lost",
    # discipline/errors
    "errors", "handling_errors", "penalties", "ruck_infringements", "inside_10_metres",
    "on_report", "sin_bins", "send_offs",
    # kicking
    "kicks", "kicking_metres", "forced_drop_outs", "bomb_kicks", "grubbers", "forty_twenty", "twenty_forty",
    "cross_field_kicks", "kicked_dead", "kicks_defused",
]

def build_row(player_dict: dict, match_key: str):
    season, rnd, home, away = parse_match_key(match_key)

    row = {c: None for c in PLAYER_STATS_COLS}
    row["season"] = season
    row["round"] = rnd
    row["home"] = home
    row["away"] = away
    row["match_key"] = match_key
    row["match_id"] = None  # filled in player_z_scores to join to matches(match_id)

    for json_key, (db_col, conv) in KEYMAP.items():
        if json_key in player_dict:
            row[db_col] = conv(player_dict[json_key])

    return row

def insert_row(row: dict):
    cols = [c for c in PLAYER_STATS_COLS]  # fixed ordering
    vals = [row[c] for c in cols]

    query = sql.SQL("INSERT INTO player_stats ({fields}) VALUES ({placeholders})").format(
        fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )

    with conn.cursor() as cursor:
        cursor.execute(query, vals)

# main ingestion loop

IGNORE_MATCH_KEYS = set()  # add any rogue ones if needed

for year in range(2001, 2026):
    year = str(year)
    path_player = f"/dcs/23/u5503037/CS344/data_jsons/NRL/{year}/NRL_player_statistics_{year}.json"

    with open(path_player, "r", encoding="utf-8") as f:
        player_data = json.load(f)

    rounds_blob = player_data["PlayerStats"][0][year]

    print(len(rounds_blob))

    for i in range(len(rounds_blob)):
        roundnum = str(i)
        if roundnum not in rounds_blob[i]:
            continue

        matches_list = rounds_blob[i][roundnum]  # list of {match_key: [players...]} dicts
        for match_obj in matches_list:
            for match_key, players in match_obj.items():
                if match_key in IGNORE_MATCH_KEYS or not players:
                    continue

                # de-dupe within a match (to clean data in case there were issues in scraping)
                seen = set()

                for p in players:
                    sig = (match_key, p.get("Name"), p.get("Number"), p.get("Position"), p.get("Mins Played"))
                    if sig in seen:
                        continue
                    seen.add(sig)

                    row = build_row(p, match_key)
                    insert_row(row)

    conn.commit()
    print(f"Committed player_stats for season {year}")

conn.close()

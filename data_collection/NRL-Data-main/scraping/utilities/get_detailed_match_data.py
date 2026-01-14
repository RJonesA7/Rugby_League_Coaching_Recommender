"""
Optimized Web Scraper for Finding NRL Team Statistics
"""

from bs4 import BeautifulSoup
from utilities.set_up_driver import set_up_driver
import sys

sys.path.append("..")
import ENVIRONMENT_VARIABLES as EV

# Default statistics with missing values set to -1
BARS_DATA = {
    'time_in_possession': -1, 'all_runs': -1, 'all_run_metres': -1, 'post_contact_metres': -1,
    'line_breaks': -1, 'tackle_breaks': -1, 'average_set_distance': -1, 'kick_return_metres': -1,
    'offloads': -1, 'receipts': -1, 'total_passes': -1, 'dummy_passes': -1, 'kicks': -1, 'kicking_metres': -1,
    'forced_drop_outs': -1, 'bombs': -1, 'grubbers': -1, '40/20': -1, 'tackles_made': -1, 'missed_tackles': -1,
    'intercepts': -1, 'ineffective_tackles': -1, 'errors': -1, 'penalties_conceded': -1, 'ruck_infringements': -1,
    'inside_10_metres': -1, 'on_reports': -1, 'sin_bins': -1, 'interchanges_used': -1
}

DONUT_DATA = {
    'completion_rate': -1, 'average_play_ball_speed': -1,
    'kick_defusal': -1, 'effective_tackle': -1
}

DONUT_DATA_2 = {
    'tries': -1, 'conversions': -1, 'penalty_goals': -1, 'sin_bins': -1,
    '1_point_field_goals': -1, '2_point_field_goals': -1, 'half_time': -1
}

DONUT_DATA_2_DEFAULTS = {
    'tries': "0", 'conversions': "0", 'penalty_goals': "0/0", 'sin_bins': "0",
    '1_point_field_goals': "0/0", '2_point_field_goals': "0/0", 'half_time': "0"
}

DONUT_DATA_2_WORDS = [
    'TRIES', 'CONVERSIONS', 'PENALTY GOALS', 'SIN BINS',
    '1 POINT FIELD GOALS', '2 POINT FIELD GOALS', 'HALF TIME'
]

DONUT_DATA_2_ORDER = [
    'tries', 'conversions', 'penalty_goals', 'sin_bins',
    '1_point_field_goals', '2_point_field_goals', 'half_time'
]

def get_detailed_nrl_data(round: int, year: int, home_team: str, away_team: str, driver=None, nrl_website=EV.NRL_WEBSITE):
    home_team, away_team = [x.replace(" ", "-") for x in [home_team, away_team]]

    url = f"{nrl_website}{year}/round-{round}/{home_team}-v-{away_team}/"
    print(f"Fetching data: {url}")

    # Webscrape the NRL website
    if driver is None:
        driver = set_up_driver()  # Only create a new driver if one isn't provided
    
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Initialize match data structures
    home_donut, away_donut = DONUT_DATA.copy(), DONUT_DATA.copy()
    home_game_stats, away_game_stats = DONUT_DATA_2.copy(), DONUT_DATA_2.copy()
    
    # **Extract Team Possession**
    try:
        home_possession = soup.find('p', class_='match-centre-card-donut__value--home').text.strip()
        away_possession = soup.find('p', class_='match-centre-card-donut__value--away').text.strip()
    except AttributeError:
        home_possession, away_possession = None, None
        print("Error: Missing possession data.")

    # **Extract Bar Statistics (Team Stats)**
    def extract_bars(stat_list, bars_dict, order):
        for item, bar_name in zip(stat_list, order):
            if bar_name in bars_dict.keys():
                bars_dict[bar_name] = item.get_text(strip=True)

    try:
        order = soup.find_all(class_="stats-bar-chart__title")
        for i in range(len(order)):
            order[i] = order[i].get_text().lower().replace(' ', '_').replace('_%','')
            if order[i] == 'used':
                order[i] = "interchanges_used"
        #Get rid of stats we aren't looking for
        final_order = [stat for stat in order if stat in BARS_DATA]
        order = final_order
        
        #Take only the stats which are present on the match centre:
        home_bars = {}
        away_bars = {}
        for stat in order:
            home_bars = home_bars | {stat: -1}
            away_bars = away_bars | {stat: -1}

        extract_bars(soup.find_all('dd', class_="stats-bar-chart__label--home"), home_bars, order)
        extract_bars(soup.find_all('dd', class_="stats-bar-chart__label--away"), away_bars, order)
    except Exception as e:
        print("Error: Issue extracting bar statistics." + str(e))

    # **Extract Donut Statistics**
    try:
        order = soup.find_all(class_="stats-bar-chart__title")
        for i in range(len(order)):
            order[i] = order[i].get_text().lower().replace(' ', '_').replace('_%','').replace("_the", '')
        #Get rid of stats we aren't looking for
        final_order = [stat for stat in order if stat in DONUT_DATA]
        order = final_order
        
        #Take only the stats which are present on the match centre:
        home_donut= {}
        away_donut = {}
        for stat in order:
            home_donut = home_donut | {stat: -1}
            away_donut = away_donut | {stat: -1}
        elements = soup.find_all("p", class_="donut-chart-stat__value")
        numbers = [el.get_text(strip=True) for el in elements]
        home_donut.update(dict(zip(home_donut.keys(), numbers[::2])))
        away_donut.update(dict(zip(away_donut.keys(), numbers[1::2])))
    except Exception:
        print("Error: Issue extracting donut statistics.")

    # **Extract Try Scorers & Times**
    def extract_try_scorers(team_class):
        try:
            tries = soup.find("ul", class_=team_class).find_all("li")
            names, times = zip(*[(t.get_text(strip=True).rsplit(" ", 1)) for t in tries])
            return list(names), list(times)
        except (AttributeError, ValueError):
            return [], []

    home_try_names, home_try_minutes = extract_try_scorers("match-centre-summary-group__list--home")
    away_try_names, away_try_minutes = extract_try_scorers("match-centre-summary-group__list--away")

    # **Determine First Try Scorer**
    def determine_first_scorer():
        if not home_try_minutes and not away_try_minutes:
            return None, None, None
        elif not away_try_minutes or (home_try_minutes and int(home_try_minutes[0].partition("'")[0]) < int(away_try_minutes[0].partition("'")[0])):
            return home_try_names[0], home_try_minutes[0], home_team
        else:
            return away_try_names[0], away_try_minutes[0], away_team

    overall_first_try_scorer, overall_first_try_minute, overall_first_scorer_team = determine_first_scorer()

    # **Check Missing Data for DONUT_DATA_2**
    span_elements = {span.text.strip().upper() for span in soup.find_all('span', class_='match-centre-summary-group__name')}
    for word in DONUT_DATA_2_WORDS:
        if word not in span_elements:
            home_game_stats.pop(word.lower().replace(" ", "_"))
            away_game_stats.pop(word.lower().replace(" ", "_"))

    # **Extract Match Summary Data**
    try:
        stats = [el.span.get_text(strip=True) for el in soup.find_all("span", class_="match-centre-summary-group__value")]
        home_game_stats.update(dict(zip(home_game_stats.keys(), stats[::2])))
        away_game_stats.update(dict(zip(away_game_stats.keys(), stats[1::2])))
        # Update with defaults for all stats where there wasn't a value
        home_game_stats = home_game_stats | {k: v for k, v in DONUT_DATA_2_DEFAULTS.items() if k not in home_game_stats}
        away_game_stats = away_game_stats | {k: v for k, v in DONUT_DATA_2_DEFAULTS.items() if k not in away_game_stats}
        # Reorder so defaults go where they should
        home_game_stats = {k: home_game_stats[k] for k in DONUT_DATA_2_ORDER}
        away_game_stats = {k: away_game_stats[k] for k in DONUT_DATA_2_ORDER}
    except Exception:
        print("Error: Issue extracting match summary statistics.")
        
    # **Extract Referee Data**
    try:
        refs = soup.find_all("a", class_="card-team-mate")
        ref_names = [r.find("h3", class_="card-team-mate__name").get_text(strip=True) for r in refs]
        ref_positions = [r.find("p", class_="card-team-mate__position").get_text(strip=True) for r in refs]
        main_ref_name = ref_names[0] if ref_names else None
    except Exception:
        ref_names, ref_positions, main_ref_name = [], [], None
        print("Error: Issue extracting referee data.")

    # **Extract Ground & Weather Conditions**
    ground_condition, weather_condition = None, None
    try:
        conditions = {p.get_text(strip=True).split(":")[0].strip(): p.span.get_text(strip=True) for p in soup.find_all("p", class_="match-weather__text")}
        ground_condition = conditions.get("Ground Conditions", None)
        weather_condition = conditions.get("Weather", None)
    except Exception:
        print("Error: Issue extracting weather/ground conditions.")

    # **Prepare Final Data Structure**
    match_data = {
        'overall_first_try_scorer': overall_first_try_scorer,
        'overall_first_try_minute': overall_first_try_minute,
        'overall_first_try_round': overall_first_scorer_team,
        'ref_names': ref_names, 'ref_positions': ref_positions, 'main_ref': main_ref_name,
        'ground_condition': ground_condition, 'weather_condition': weather_condition
    }

    return {'match': match_data, 'home': {**home_bars, **home_donut, **home_game_stats}, 'away': {**away_bars, **away_donut, **away_game_stats}}

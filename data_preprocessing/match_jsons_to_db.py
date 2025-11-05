import json, psycopg2
from datetime import datetime
from psycopg2 import sql

conn = psycopg2.connect(dbname="nrl_data", host="/dcs/23/u5503037/CS344/pgsock", port=5432)
cursor = conn.cursor()

match_insert_query = """ 
INSERT INTO 
matches(home_text, away, home_score, away_score) 
VALUES ({round}, '{home}', '{away}', {home_score}, {away_score})
"""

#Some matches have rogue data, due to something throwing off the scraper. There aren't many so they can be safely ignored
ignore_matches = [
    "Sea Eagles v Rabbitohs 2017",
]

#Initial testing of JSON retreival
path_match = "/dcs/23/u5503037/CS344/data_jsons/NRL/2025/NRL_data_2025.json"
#print(path)
file = open(path_match, "r", encoding="utf-8")
data = json.load(file)
#print(data['NRL'][0]['2025'][0]['1'][0])

path_detailed = "/dcs/23/u5503037/CS344/data_jsons/NRL/2025/NRL_detailed_match_data_2025.json"
#print(path_detailed)
file_detailed = open(path_detailed, "r", encoding="utf-8")
detailed_data = json.load(file_detailed)
#print(detailed_data['NRL'][0]['1'][0])

#path_player = "/dcs/23/u5503037/CS344/data_jsons/NRL/2025/NRL_player_statistics_2025.json"
#print(path_player)
#file_player = open(path_player, "r", encoding="utf-8")
#player_data = json.load(file_player)
#print(player_data['PlayerStats'][0]['2025'][0]['0'][0]["2025-1-Raiders-v-Warriors"][0])

#Function to find the same match from detailed data as is being considered in match data, given the match data and the detailed data for the same round
def combine_matches(match_data, detailed_data_round):
    #Create the identifier for the match which will be the name of the match in the detailed data
    match = match_data['Home'] + " v " + match_data["Away"]
    #Iterate over the detailed data until we find this match
    found = False
    i = 0
    while found == False:
        try:
            match_detailed_data = detailed_data_round[i][match]
            found = True
        except:
            pass
        i = i + 1
        if i > 15:
            print("Failed to find data for match: " + match,  match_data['Round'], match_data['Date'])
    
    final_match_json =  {'Overall_Data': match_data} | match_detailed_data

    return final_match_json

rai_war = combine_matches(data['NRL'][0]['2025'][0]['1'][0], detailed_data['NRL'][0]['1'])

#Function to put full match data into the database
def insert_match(full_match_data):
    #Has to be done to match the columns in the schema
    #Go through each of the four JSONs in the overarching JSON individually
    #Use a switch case for the insertion datatype and the extraction datatype (have to turn strings into the relevant datatype)
    #Most stats are ints, but a fair few will require specific handling

    #If the match is in the specified ignore matches, don't insert
    if full_match_data['Overall_Data']['Home'] + ' v ' + full_match_data['Overall_Data']['Away'] + ' ' + str(datetime.fromisoformat(full_match_data['Overall_Data']['Date'].replace("Z", "+00:00")).year) in ignore_matches:
        print('Rogue match found, not being inserted')
        return False

    #Turn the dict into a dict which matches the format of the database table
    keys = list(full_match_data['home'].keys())
    for key in keys:
        #recognise 'home' in key + other sanitisation
        full_match_data['home'][key.lower().replace("40/20", "forty_twenty").replace('10', 'ten').replace('1', 'one').replace('2', 'two').replace(' ', '_') + "_h"] = full_match_data['home'].pop(key)
        key = key.lower().replace("40/20", "forty_twenty").replace('10', 'ten').replace('1', 'one').replace('2', 'two').replace(' ', '_') + "_h"

        
        #Sanitise and convert data type. Dependent on exactly what the data is, if elif else statements used here since there aren't too many cases

        #If null, set to None and skip
        if full_match_data['home'][key] == -1:
            full_match_data['home'][key] = None
            #For some categories, additional nulls have to be added.
            if key in ["penalty_goals_h", "conversions_h", "one_point_field_goals_h", "two_point_field_goals_h"]:
                full_match_data['home'][key[:-2] + "_missed_h"] = None
        elif key in ['average_set_distance_h', 'average_play_ball_speed_h', 'effective_tackle_h', 'completion_rate_h', 'kick_defusal_h']:
            full_match_data['home'][key] = float(full_match_data['home'][key].replace('s', '').replace('%', ''))
        elif key == 'time_in_possession_h':
            time_str = full_match_data['home'][key]
            time_str = time_str.split(':')
            time_sec = int(time_str[0]) * 60 + int(time_str[1])
            full_match_data['home'][key] = time_sec
        elif key in ["penalty_goals_h", "conversions_h", "one_point_field_goals_h", "two_point_field_goals_h"]:
            #try except because in some old data, field goal missed attempts were not recorded
            try:
                both = full_match_data['home'][key]
                if both == '0':
                    both = "0/0"
                both = both.split('/')
                full_match_data['home'][key] = int(both[0])
                full_match_data['home'][key[:-2] + "_missed_h"] = int(both[1]) - int(both[0])
            except:
                full_match_data['home'][key[:-2] + "_missed_h"] = None
        elif key == "sin_bins_h":
            sin_bins = full_match_data['home'][key]
            if '/' in sin_bins:
                sin_bins = sin_bins.split('/')[1]
            full_match_data['home'][key] = sin_bins
        else:
            full_match_data['home'][key] = int(full_match_data['home'][key].replace(',', '').replace('%', ''))

        

    keys = list(full_match_data['away'].keys())
    for key in keys:
        #recognise 'away' in key    
        full_match_data['away'][key.lower().replace("40/20", "forty_twenty").replace('10', 'ten').replace('1', 'one').replace('2', 'two').replace(' ', '_') + "_a"] = full_match_data['away'].pop(key)
        key = key.lower().replace("40/20", "forty_twenty").replace('10', 'ten').replace('1', 'one').replace('2', 'two').replace(' ', '_') + "_a"
        
        #Convert data type. Dependent on exactly what the data is. If elif else statements used here since there aren't too many cases

        #If null, set to None and skip
        if full_match_data['away'][key] == -1:
            full_match_data['away'][key] = None
            #For some categories, additional nulls have to be added.
            if key in ["penalty_goals_a", "conversions_a", "one_point_field_goals_a", "two_point_field_goals_a"]:
                full_match_data['away'][key[:-2] + "_missed_a"] = None
        elif key in ['average_set_distance_a', 'average_play_ball_speed_a', 'effective_tackle_a', 'completion_rate_a', 'kick_defusal_a']:
            full_match_data['away'][key] = float(full_match_data['away'][key].replace('s', '').replace('%', ''))
        elif key == 'time_in_possession_a':
            time_str = full_match_data['away'][key]
            time_str = time_str.split(':')
            time_sec = int(time_str[0]) * 60 + int(time_str[1])
            full_match_data['away'][key] = time_sec
        elif key in ["penalty_goals_a", "conversions_a", "one_point_field_goals_a", "two_point_field_goals_a"]:
            #try except because in some old data, field goal missed attempts were not recorded
            try:
                both = full_match_data['away'][key]
                if both == '0':
                    both = "0/0"
                both = both.split('/')
                full_match_data['away'][key] = int(both[0])
                full_match_data['away'][key[:-2] + "_missed_a"] = int(both[1]) - int(both[0])
            except:
                full_match_data['away'][key[:-2] + "_missed_a"] = None
        elif key == "sin_bins_a":
            sin_bins = full_match_data['away'][key]
            if '/' in sin_bins:
                sin_bins = sin_bins.split('/')[1]
            full_match_data['away'][key] = int(sin_bins)
        else:
            full_match_data['away'][key] = int(full_match_data['away'][key].replace(',', '').replace('%', ''))


    keys = list(full_match_data['Overall_Data'].keys())
    for key in keys:
        if key == 'Date':
            dt = datetime.fromisoformat(full_match_data['Overall_Data'][key].replace("Z", "+00:00"))
            season = str(dt.year)
            full_match_data['Overall_Data']['match_date_utc'] = full_match_data['Overall_Data'].pop(key)
        elif key == 'Round':
            #Finals games should not be given a round, they are separate
            if 'Final' in full_match_data['Overall_Data'][key]:
                full_match_data['Overall_Data'][key] = None
            else:
                full_match_data['Overall_Data'][key] = full_match_data['Overall_Data'][key].split(' ')[-1]
            full_match_data['Overall_Data'][key.lower()] = full_match_data['Overall_Data'].pop(key)
        else:
            full_match_data['Overall_Data'][key.lower()] = full_match_data['Overall_Data'].pop(key)

    if full_match_data['match']['overall_first_try_minute'] != None:
        full_match_data['match']['overall_first_try_minute'] = int(full_match_data['match']['overall_first_try_minute'].replace("'", ""))

    insertion_dict = full_match_data['home'] | full_match_data['away'] | full_match_data['Overall_Data'] | full_match_data['match'] | {'season':season}
    
    #Insert
    cols = list(insertion_dict.keys())
    vals = list(insertion_dict.values())

    query = sql.SQL("INSERT INTO matches ({fields}) VALUES ({placeholders})").format(
        fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in cols),   
    )   

    with conn.cursor() as cursor:
        cursor.execute(query, vals)
        conn.commit()
        print("committed " + insertion_dict['home'] + ' v ' + insertion_dict['away'] + ' ' + str(season))


#Insertion loop - goes through all years, and each array for that year
for year in range(2001, 2026):
    year = str(year)
    #Fetch the match data and detailed match data JSON collections for the relevant year
    path_match = "/dcs/23/u5503037/CS344/data_jsons/NRL/"+ year +"/NRL_data_"+year+".json"
    file = open(path_match, "r", encoding="utf-8")
    data = json.load(file)
    path_detailed = "/dcs/23/u5503037/CS344/data_jsons/NRL/"+ year +"/NRL_detailed_match_data_"+ year +".json"
    file_detailed = open(path_detailed, "r", encoding="utf-8")
    detailed_data = json.load(file_detailed)

    #Move to relevant part of data
    data = data['NRL'][0][year]
    detailed_data = detailed_data['NRL']
    
    #Iterate over rounds and matches, combine over data and detailed_data and insert into database
    for i in range(len(data)):
        roundnum = str(i + 1)
        for match in data[i][roundnum]:
            insert_match(combine_matches(match, detailed_data[i][roundnum]))

conn.close()
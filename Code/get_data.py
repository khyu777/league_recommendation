from riotwatcher import LolWatcher, ApiError
import pandas as pd
import numpy as np
import datetime as dt
import pytz
import time

# Get all matches function
def get_all_matches(region, id):
    global count
    match = watcher.match.matchlist_by_account(region, id, queue = 420)
    beginIndex = 100
    endIndex = 200
    while endIndex <= 500:
        print(count, id, beginIndex)
        try:
            match_addl = watcher.match.matchlist_by_account(region, id, queue = 420, begin_index = beginIndex)
            match['matches'] = match['matches'] + match_addl['matches']
            beginIndex += 100
            endIndex += 100
            time.sleep(1)
        except:
            return False
    count += 1
    return(match)

# accountId print function
def acct_id_print(my_region, id):
    global count
    print(count)
    acct_id = watcher.summoner.by_id(my_region, id)['accountId']
    count += 1
    time.sleep(1)
    return(acct_id)

#global vars
api_key = 'RGAPI-62ceaab2-d743-4840-9235-be861c88513c'
watcher = LolWatcher(api_key)
my_region = 'na1'

print('Pulling Master players list...')
master = watcher.league.masters_by_queue(my_region, 'RANKED_SOLO_5x5')
print('Finished pulling Master players')
master_df = pd.DataFrame(master)
master_df = master_df.head(100)

master_df = master_df.join(pd.json_normalize(master_df.entries))
master_df.drop(columns = ['entries'], inplace = True)

print('Total # of players: {}'.format(master_df.shape[0]))

print('Getting accountId...')
count = 1
master_df.loc[:,'acct_id'] = [acct_id_print(my_region, x) for x in master_df.summonerId]
print('Finished getting accountId')

print('Getting matches...')
count = 1
master_df.loc[:, 'match'] = [get_all_matches(my_region, x) for x in master_df.acct_id]
master_df = master_df.join(pd.json_normalize(master_df.match))

lens = [len(item) for item in master_df['matches']]
master_df = pd.DataFrame(
    {'accountId': np.repeat(master_df['acct_id'].values, lens),
    'summonerName': np.repeat(master_df['summonerName'].values, lens),
    'wins': np.repeat(master_df['wins'].values, lens),
    'losses': np.repeat(master_df['losses'].values, lens),
    'leaguePoints': np.repeat(master_df['leaguePoints'].values, lens),
    'totalGames': np.repeat(master_df['totalGames'].values, lens),
    'matches': np.concatenate(master_df['matches'].values)}
)
master_df = master_df.join(pd.json_normalize(master_df.matches))
master_df.drop(columns = ['matches'], inplace = True)

champion_key = pd.read_json('Raw Data/en_US/champion.json')[['type', 'data']]
champion_key = pd.json_normalize(champion_key.data)[['name', 'key']]
champion_key['key'] = pd.to_numeric(champion_key['key'])

master_df = pd.merge(master_df, champion_key, how = 'left', left_on = 'champion', right_on = 'key')
print('Added champion names')

master_df.to_csv('Raw Data/master_solo_queue.csv', index = False)

# Get Match Information
print('Getting match information')
match_info = pd.DataFrame([watcher.match.by_id(my_region, x) for x in master_df.head()['gameId']])[['gameId', 'teams', 'participants', 'participantIdentities']]

lens = [len(item) for item in match_info['teams']]
match_info_teams = pd.DataFrame(
    {'gameId': np.repeat(match_info['gameId'].values, lens),
    'teams': np.concatenate(match_info['teams'].values)}
)
match_info_teams = match_info_teams.join(pd.json_normalize(match_info_teams.teams))
match_info_teams = match_info_teams.explode('bans').reset_index(drop = True)
match_info_teams = match_info_teams.join(pd.json_normalize(match_info_teams.bans))
match_info_teams.drop(columns = ['teams', 'bans'], inplace = True)
print('Finished getting match information')

lens = [len(item) for item in match_info['participants']]
match_info_pts = pd.DataFrame(
    {'gameId': np.repeat(match_info['gameId'].values, lens),
    'participants': np.concatenate(match_info['participants'].values),
    'participantIdentities': np.concatenate(match_info['participantIdentities'].values)}
)
pts = match_info_pts.join(pd.json_normalize(match_info_pts.participants))
pts.drop(columns = ['participants', 'participantIdentities'], inplace = True)
pt_ids = match_info_pts.join(pd.json_normalize(match_info_pts.participantIdentities))
pt_ids.drop(columns = ['participants', 'participantIdentities'], inplace = True)
print('Finished getting participants')

pt_match_all = pts.merge(pt_ids, on = ['participantId', 'gameId'])

match_info_teams.to_csv('Raw Data/match_team_info.csv', index = False)
pt_match_all.to_csv('Raw Data/match_pt_info.csv', index = False)

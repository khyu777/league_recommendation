from riotwatcher import LolWatcher, ApiError
import pandas as pd
import numpy as np
import datetime as dt
import pytz
import time
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("RIOT_API_KEY")

# accountId print function
def acct_id_print(my_region, id):
    global count
    print(count)
    acct_id = watcher.summoner.by_id(my_region, id)['accountId']
    count += 1
    time.sleep(10/12)
    return(acct_id)

#global vars
watcher = LolWatcher(api_key)
my_region = 'na1'

print('Pulling Master players list...')
master = watcher.league.masters_by_queue(my_region, 'RANKED_SOLO_5x5')
print('Finished pulling Master players')
master_df = pd.DataFrame(master)
#master_df = master_df.head(100)

master_df = master_df.join(pd.json_normalize(master_df.entries))
master_df.drop(columns = ['entries'], inplace = True)

print('Total # of players: {}'.format(master_df.shape[0]))

print('Getting accountId...')
count = 1
master_df.loc[:,'acct_id'] = [acct_id_print(my_region, x) for x in master_df.summonerId]
print('Finished getting accountId')

master_df.to_csv('Raw Data/queue_list.csv', index=False)
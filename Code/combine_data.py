import pandas as pd
import os

def append_df(filetype, num, filenum):
    df_list = []
    while num <= filenum:
        filename = 'Raw Data/' + filetype + '_' + str(num) + '.csv'
        file = pd.read_csv(filename)
        df_list.append(file)
        num += 1
    outfile = 'Raw Data/' + filetype + '.csv'
    pd.concat(df_list).to_csv(outfile, index=False)

num = 1
filenum = 5

append_df('master_solo_queue', num, filenum) 
append_df('match_team_info', num, filenum)
append_df('match_pt_info', num, filenum)
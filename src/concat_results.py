import pandas as pd
import numpy as np
import os
from utils.elo import Elo
import git

# this script concatenates gameweek-level stats
SEASONS = [
    "2018-19",
    "2019-20",
    "2020-21",
    "2021-22",
]
SOURCE = "data/Fantasy-Premier-League/data"
OUTPUT_DIR = "data/processed"
GIT_DIR = "data/Fantasy-Premier-League"



def load_player_data(seasons):
    gameweek_df = pd.DataFrame()
    for season in seasons:
        for gw in range(1, 39):
            try:
                gw_path = f"{SOURCE}/{season}/gws/gw{gw}.csv"
                if season == '2018-19':
                    encoding = 'ISO-8859-1'# slightly annoying :( 
                else:
                    encoding = 'utf-8'
                g_df = pd.read_csv(gw_path, encoding=encoding)
                g_df["gw"] = gw
                g_df["season"] = season
                gameweek_df = gameweek_df.append(g_df, sort=False)
            except FileNotFoundError:  # the current season will be missing some gws
                assert season == "2021-22"
                pass
    gameweek_df['date'] = pd.to_datetime(gameweek_df.kickoff_time).dt.date
    print(f'concat {g_df.date.max()}') 
    return gameweek_df.reset_index(drop=True)

def load_understat_data():
    combined_df = pd.DataFrame()
    dlq = []
    path = f'{SOURCE}/2021-22/understat'# contains historic seasons
    for player in os.listdir(path):
        if '.csv' in player:
            try:
                df = pd.read_csv(f'{path}/{player}',encoding='utf-8',parse_dates=['date'])
                df['player'] = player
                combined_df = combined_df.append(df)
            except ValueError:
                dlq.append(player)
    print(f'understat {combined_df.date.max()}')
    assert len(dlq) == 1 # only one incorrect file
    return combined_df 

def main():
    git.cmd.Git(GIT_DIR).pull()
    gameweek_df = load_player_data(SEASONS)
    gameweek_df.to_csv(f"{OUTPUT_DIR}/combined_gameweeks.csv", index=False)
    
    understat_df = load_understat_data()
    understat_df.to_csv(f"{OUTPUT_DIR}/understat_raw.csv", index=False)



if __name__ == "__main__":
    main()

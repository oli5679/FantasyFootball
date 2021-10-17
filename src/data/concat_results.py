import pandas as pd
import numpy as np

# this script concatenates gameweek-level stats

SEASONS = ['2018-19' ,'2019-20','2020-21', '2021-22',]

SOURCE = "data/Fantasy-Premier-League/data"

OUTPUT = "data/processed/results_concat.csv"

def load_data(seasons):
    gameweek_df = pd.DataFrame()
    for season in seasons:
        for gw in range(1, 39):
            try:
                gw_path = f"{SOURCE}/{season}/gws/gw{gw}.csv"
                g_df = pd.read_csv(gw_path, encoding="ISO-8859-1")
                g_df["gw"] = gw
                g_df["season"] = season
                gameweek_df = gameweek_df.append(g_df, sort=False)
            except FileNotFoundError: # the current season will be missing some gws
                pass
    return gameweek_df.reset_index(drop=True)

if __name__ == '__main__':
    gameweek_df = load_data(SEASONS)
    gameweek_df.to_csv(OUTPUT)
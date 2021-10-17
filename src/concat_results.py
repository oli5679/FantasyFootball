import pandas as pd
import numpy as np
import pickle

from utils.elo import Elo

# this script concatenates gameweek-level stats
SEASONS = [
    "2018-19",
    "2019-20",
    "2020-21",
    "2021-22",
]
SOURCE = "data/Fantasy-Premier-League/data"
OUTPUT_DIR = "data/processed"


def load_player_data(seasons):
    gameweek_df = pd.DataFrame()
    for season in seasons:
        for gw in range(1, 39):
            try:
                gw_path = f"{SOURCE}/{season}/gws/gw{gw}.csv"
                g_df = pd.read_csv(gw_path, encoding="ISO-8859-1")
                g_df["gw"] = gw
                g_df["season"] = season
                gameweek_df = gameweek_df.append(g_df, sort=False)
            except FileNotFoundError:  # the current season will be missing some gws
                assert season == "2021-22"
                pass
    return gameweek_df.reset_index(drop=True)


def main():
    gameweek_df = load_player_data(SEASONS)
    gameweek_df.to_csv(f"{OUTPUT_DIR}/combined_gameweeks.csv", index=False)


if __name__ == "__main__":
    main()

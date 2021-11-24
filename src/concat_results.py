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

NAME_SWAPS = [
    ["Cristiano_Aveiro", "Cristiano_Ronaldo"],
    ["Tino_Livramento", "Valentino_Livramento"],
    ["Heung-Min_Son", "Son_Heung-Min"],
    ["Emile_Rowe", "Emile_Smith-Rowe"],
    ["Raphael_Belloli", "Raphinha"],
    ["Benjamin_Chilwell", "Ben_Chilwell",],
    ["Virgil_Dijk", "Virgil_van_Dijk",],
    ["Richarlison_Andrade", "Richarlison"],
    ["David_Martin", "David_Raya"],
    ["Ederson_Moraes", "Ederson"],
    ["Alisson_Becker", "Alisson"],
    ["Allan_Loureiro", "Allan"],
    ["David_Gea", "David_de_Gea"],
]  # there is probably a better way, e.g. string distances


def clean_name(name_series, swaps):
    split = (
        name_series.str.replace("\_\d+", "", regex=True)
        .str.replace("_", " ", regex=False)
        .str.replace("ï|í", "i", regex=True)
        .str.replace("á", "a", regex=False)
        .str.split()
    )
    combined = split.str[0] + "_" + split.str[-1]
    for from_, to in swaps:
        combined = combined.replace(from_, to)
    return combined


def concat_player_data(seasons):
    gameweek_df = pd.DataFrame()
    for season in seasons:
        for gw in range(1, 39):
            try:
                gw_path = f"{SOURCE}/{season}/gws/gw{gw}.csv"
                if season == "2018-19":
                    encoding = "ISO-8859-1"  # slightly annoying :(
                else:
                    encoding = "utf-8"
                g_df = pd.read_csv(gw_path, encoding=encoding)
                g_df["gw"] = gw
                g_df["season"] = season
                gameweek_df = gameweek_df.append(g_df, sort=False)
            except FileNotFoundError:  # the current season will be missing some gws
                assert season == "2021-22"
                pass
    gameweek_df["date"] = pd.to_datetime(gameweek_df.kickoff_time).dt.date
    print(f"concat {gameweek_df.date.max()}")
    gameweek_df["clean_name"] = clean_name(gameweek_df.name, NAME_SWAPS)
    return gameweek_df.reset_index(drop=True)


def concat_understat_data():
    combined_df = pd.DataFrame()
    dlq = []
    path = f"{SOURCE}/2021-22/understat"  #  contains historic seasons
    for player in os.listdir(path):
        if ".csv" in player:
            try:
                df = pd.read_csv(f"{path}/{player}", encoding="utf-8", parse_dates=["date"])
                df["player"] = player
                combined_df = combined_df.append(df)
            except ValueError:
                dlq.append(player)
    print(f"understat {combined_df.date.max()}")
    assert len(dlq) == 1  # only one incorrect file
    return combined_df


def create_ratings(gameweek_df):
    teams = pd.read_csv(f"{SOURCE}/master_team_list.csv").rename(
        columns={"team": "opponent_team", "team_name": "opponent_name"}
    )
    gameweek_df["team"] = gameweek_df.groupby("clean_name")["team"].bfill()  # backfill the teams based on the players
    # in cases where multiple opponents, e.g. because name backfill is wrong, go with the most common opponent
    gameweek_dedup = (
        gameweek_df.groupby(["team", "gw", "season", "opponent_team"], as_index=False)
        .agg({"name": "count", "team_h_score": "first", "team_a_score": "first", "was_home": "first",})
        .sort_values(by=["opponent_team", "season", "gw", "name"])
        .drop_duplicates(subset=["opponent_team", "season", "gw"], keep="last")
    )
    assert len(gameweek_dedup) == len(gameweek_dedup.drop_duplicates(subset=["team", "season", "gw", "opponent_team"]))
    with_opponents = gameweek_df.merge(teams, on=["season", "opponent_team"], how="left")
    fixtures = (
        with_opponents[["team", "opponent_name", "season", "gw", "team_h_score", "team_a_score", "was_home",]]
        .loc[with_opponents.team.notna()]
        .drop_duplicates()
        .copy()
    )
    fixtures["opp_team"] = "opp_" + fixtures.opponent_name  # so we can merge and update
    fixtures["home_outcome"] = (np.clip(fixtures["team_h_score"] - fixtures["team_a_score"], -1, 1) + 1) / 2
    fixtures["outcome"] = np.where(
        fixtures.was_home, fixtures.home_outcome, 1 - fixtures.home_outcome
    )  # there must be a simpler way
    elo = Elo(fixtures, k_factor=20, team_h="team", team_a="opp_team")  # todo tune k
    historic_elo, elo_ratings = elo.process_all_fixtures()
    return historic_elo  # todo also calculate scoring and defending elos


def main():
    git.cmd.Git(GIT_DIR).pull()
    gameweek_df = concat_player_data(SEASONS)
    gameweek_df.to_csv(f"{OUTPUT_DIR}/combined_gameweeks.csv", index=False)
    understat_df = concat_understat_data()
    understat_df.to_csv(f"{OUTPUT_DIR}/understat_raw.csv", index=False)
    team_elos = create_ratings(gameweek_df)
    team_elos[["team", "season", "gw", "elo_e"]].to_csv(f"{OUTPUT_DIR}/team_elos.csv", index=False)


if __name__ == "__main__":
    main()

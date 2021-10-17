from collections import defaultdict


class Elo:
    def __init__(self, results, k_factor=20):
        self.fixtures = results.copy().reset_index(drop=True)
        self.ratings = defaultdict(lambda: 1200)
        self.k_factor = k_factor

    def _record_expectation(self, i, fixture):
        self.fixtures.at[i, "elo_e"] = self.win_prob(
            fixture["team_h"], fixture["team_a"]
        )
        self.fixtures.at[i, "elo"] = self.ratings[fixture["team_h"]]

    def process_all_fixtures(self):
        for i, f in self.fixtures.iterrows():
            self._record_expectation(i, f)
            self._process_fixture(f)
        return self.fixtures, self.ratings

    def _process_fixture(self, fixture):
        win_prob = self.win_prob(fixture["team_h"], fixture["team_a"])
        self.ratings[fixture["team_h"]] += (
            fixture["outcome"] - win_prob
        ) * self.k_factor
        self.ratings[fixture["team_a"]] += (
            win_prob - fixture["outcome"]
        ) * self.k_factor

    def win_prob(self, team_1, team_2):
        rating_1, rating_2 = self.ratings[team_1], self.ratings[team_2]
        q1 = 10.0 ** (rating_1 / 400)
        q2 = 10.0 ** (rating_2 / 400)
        return q1 / (q1 + q2)

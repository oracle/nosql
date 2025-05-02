select acct_id, user_id from Viewers v where exists v.shows.seasons.episodes[substring($element.date, 0, 4) = "2021"]

select acct_id, user_id,seq_transform(v.shows.seasons.episodes.date, substring($, 5, 2)) as months from Viewers v where exists v.shows.seasons.episodes[substring($element.date, 0, 4) = "2020" and substring($element.date, 5, 2) >= "07"]


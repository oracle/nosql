select acct_id, user_id,
       seq_transform(v.info.shows.seasons.episodes.date, substring($, 5, 2)) as months
from Viewers v
where exists v.info.shows.seasons.episodes[substring($element.date, 5, 2) = "03"]

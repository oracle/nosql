select acct_id, user_id, lastName
from viewers v
where v.shows.seasons.episodes.date <any "2021-01-01"

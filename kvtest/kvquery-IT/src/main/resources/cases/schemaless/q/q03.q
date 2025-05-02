select acct_id, user_id, lastName
from viewers v
where v.shows.genres[] =any "comedy"

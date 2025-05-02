select acct_id, user_id, lastName
from viewers v
where country = "USA" and v.shows.genres[] =any "comedy"

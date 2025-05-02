update viewers $v
put $v { "country" : "France" }
where acct_id = 300 and user_id = 1

select acct_id, user_id
from viewers v
where country = "France"

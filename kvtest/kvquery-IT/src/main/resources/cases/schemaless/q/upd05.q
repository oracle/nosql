update viewers $v
put $v { "acct_id" : 3}
where acct_id = 100 and user_id = 2


select acct_id, user_id
from viewers v
where acct_id = 100 and user_id = 2

update viewers $v
put $v { "new" : 3}
where acct_id = 100 and user_id = 2

select acct_id, user_id, v.new
from viewers v
where acct_id = 100 and user_id = 2

update viewers $v
json merge $v with patch { "new" : 3}
where acct_id = 300 and user_id = 1

select *
from viewers v
where acct_id = 300 and user_id = 1

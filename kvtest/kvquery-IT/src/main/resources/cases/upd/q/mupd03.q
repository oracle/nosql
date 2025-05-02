update users
set ttl 2 days 
where sid = 0

select sid, id, remaining_days($u) as days
from users $u
where sid = 0

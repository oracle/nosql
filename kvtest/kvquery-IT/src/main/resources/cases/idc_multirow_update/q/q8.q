update users u
set ttl 2 days
where sid1 = 1 and sid2 = 2

select name, remaining_days($u) as days
from users $u
where sid1 = 1 and sid2 = 2
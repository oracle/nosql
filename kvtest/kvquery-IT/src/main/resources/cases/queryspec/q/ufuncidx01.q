update users set ttl 1 hours where id = 1

select id
from users $u
where expiration_time($u) > timestamp_add(current_time(), "2 hours")

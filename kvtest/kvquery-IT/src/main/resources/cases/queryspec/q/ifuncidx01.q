insert into users values (10, "Tom", "Waits", null, 66, 1000, null, null, null)
set ttl 6 hours

select id
from users $u
where expiration_time($u) > timestamp_add(current_time(), "5 hours")

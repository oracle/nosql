select id
from users $u
where expiration_time($u) > timestamp_add(current_time(), "2 hours")

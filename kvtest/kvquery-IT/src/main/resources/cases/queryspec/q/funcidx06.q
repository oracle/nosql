select /* FORCE_INDEX(users idx_year_month) */ id
from users $u
where expiration_time($u) > timestamp_add(current_time(), "4 hours")

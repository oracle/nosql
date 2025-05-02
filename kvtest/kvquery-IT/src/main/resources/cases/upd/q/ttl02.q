update Foo f
set ttl 6 days
where id = 1

select (expiration_time_millis($f) - current_time_millis()) / (1000 * 3600 * 24)
from Foo $f
where id = 1

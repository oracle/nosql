update Foo f
set ttl 3 hours
where id = 0

select (expiration_time_millis($f) - current_time_millis()) / (1000 * 3600)
from Foo $f
where id = 0

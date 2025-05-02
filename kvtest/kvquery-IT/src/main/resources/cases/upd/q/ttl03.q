update Foo f
set ttl 1 hours,
set ttl using table default
where id = 2

select (expiration_time_millis($f) - current_time_millis()) / (1000 * 3600 * 24)
from Foo $f
where id = 2

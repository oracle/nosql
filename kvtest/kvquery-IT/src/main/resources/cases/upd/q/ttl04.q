update Foo f
set f.info.address.state = "WI",
set ttl 1 hours,
set f.info.address.city = "Madison",
set ttl seq_concat() hours
where id = 3

select (expiration_time_millis($f) - current_time_millis()) / (1000 * 3600 * 24),
       $f.info.address.city
from Foo $f
where id = 3

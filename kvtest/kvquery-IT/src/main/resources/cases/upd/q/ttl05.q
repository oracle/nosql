update Foo f
set f.record.long = 20,
set ttl -1 hours,
set f.record.int = null
where id = 4

select expiration_time_millis($f),
       $f.record
from Foo $f
where id = 4

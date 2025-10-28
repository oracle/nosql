select id,
       year(creation_time($f)) >= 2020,
       creation_time_millis($f) > 1700000000
from boo $f
where $f.address.state = "MA"
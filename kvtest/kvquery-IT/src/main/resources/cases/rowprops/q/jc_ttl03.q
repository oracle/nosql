select id, year(expiration_time($f)) >= 2020
from boo $f
where $f.address.state = "CA"

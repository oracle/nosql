select id, year(expiration_time($f)) >= 2020
from foo $f
where $f.address.state = "CA"

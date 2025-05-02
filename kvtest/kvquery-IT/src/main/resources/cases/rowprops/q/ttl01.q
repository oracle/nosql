select id, remaining_days($f)
from foo $f
where $f.address.state = "CA"

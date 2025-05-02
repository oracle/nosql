select id, remaining_days($f)
from boo $f
where $f.address.state = "CA"

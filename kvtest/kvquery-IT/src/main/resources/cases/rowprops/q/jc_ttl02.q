select id, 2 * remaining_days($f) as days, remaining_hours($f) < 15 as hours
from boo $f
where $f.address.state = "MA" and 2 * remaining_days($f) > 3

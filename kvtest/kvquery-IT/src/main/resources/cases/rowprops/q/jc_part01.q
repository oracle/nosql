select id, partition($f) as part
from boo $f
where $f.address.state = "MA"

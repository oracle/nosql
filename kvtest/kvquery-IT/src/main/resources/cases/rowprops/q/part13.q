select id, partition($f) as part
from foo $f
where $f.address.state = "MA" and partition($f) = 3

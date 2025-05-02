select id, shard($f) as shard
from foo $f
where $f.address.state = "CA" and shard($f) = 2

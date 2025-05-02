select id, shard($f) as shard
from foo $f
where $f.address.state = "MA" and shard($f) = 2 and partition($f) = 10

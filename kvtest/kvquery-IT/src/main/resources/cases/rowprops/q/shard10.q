select id, shard($f) as shard
from foo $f
where $f.address.state = "MA" and shard($f) = 1 and partition($f) = 10

select id, shard($f) as shard
from foo $f
where 1 <= shard($f) and shard($f) <= 3

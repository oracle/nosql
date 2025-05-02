select id, shard($f) as shard
from foo $f
where shard($f) < 2

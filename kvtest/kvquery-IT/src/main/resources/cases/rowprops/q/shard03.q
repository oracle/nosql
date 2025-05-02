select id, partition($f) as part
from foo $f
where shard($f) = 1

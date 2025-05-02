select id, partition($f) as part
from foo $f
where shard($f) = 1 and partition($f) > 1

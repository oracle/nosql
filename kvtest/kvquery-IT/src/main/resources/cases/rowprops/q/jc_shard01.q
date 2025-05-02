select id, shard($f) as shard, partition($f) as part
from boo $f

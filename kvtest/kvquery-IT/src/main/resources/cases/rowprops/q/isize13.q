select shard($f) as shard, 
       sum(index_storage_size($f, "idx_city_phones")) as index_size
from foo $f
group by shard($f)

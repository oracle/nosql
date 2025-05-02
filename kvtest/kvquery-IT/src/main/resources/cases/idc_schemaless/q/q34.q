select shard($f) as shard, sum(index_storage_size($f, "idx_name")) > 0 as bool from jsoncol $f group by shard($f)

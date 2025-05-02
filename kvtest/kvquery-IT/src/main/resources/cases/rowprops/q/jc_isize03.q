select id,
       40 <= index_storage_size($f, "idx_state_city_age") and
       index_storage_size($f, "idx_state_city_age") <= 43 as index_size
from boo $f
where index_storage_size($f, "idx_state_city_age") > 38

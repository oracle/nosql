select id,
       32 <= index_storage_size($f, "idx_state_city_age") and
       index_storage_size($f, "idx_state_city_age") <= 39 as index_size
from foo $f


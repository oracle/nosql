select id, 
       170 <= row_storage_size($f) and row_storage_size($f) <= 185 as row_size,
       index_storage_size($f, "idx_state_city_age") as index_size
from foo $f

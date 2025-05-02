select id, 
       330 <= row_storage_size($f) and row_storage_size($f) <= 450 as row_size,
       index_storage_size($f, "idx_state_city_age") as index_size
from boo $f

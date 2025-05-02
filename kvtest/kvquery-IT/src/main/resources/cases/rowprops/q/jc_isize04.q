select id, firstName,
       index_storage_size($f, "idx_state_city_age") as index_size
from boo $f

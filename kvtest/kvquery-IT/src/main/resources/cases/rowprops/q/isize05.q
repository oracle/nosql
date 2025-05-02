select id,
       index_storage_size($f, "idx_state_city_age") as index_size
from foo $f
where $f.children.Anna.iage < 10


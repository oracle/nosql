select id, index_storage_size($f, "idx_city_phones") as isize
from foo $f
where index_storage_size($f, "idx_city_phones") > 40

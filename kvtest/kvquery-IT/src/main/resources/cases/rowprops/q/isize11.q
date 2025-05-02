select id, 2 * index_storage_size($f, "idx_city_phones") as isize
from foo $f

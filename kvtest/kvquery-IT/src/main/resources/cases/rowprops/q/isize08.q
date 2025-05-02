select id, index_storage_size($f, "idx_city_phones") as isize, firstName
from foo $f
order by firstName

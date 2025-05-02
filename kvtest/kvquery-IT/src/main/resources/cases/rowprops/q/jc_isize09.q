select id, index_storage_size($f, "idx_city_phones") as isize, firstName
from boo $f
order by firstName
limit 3
offset 2

select /*+ force_primary_index(boo) */
  id, index_storage_size($f, "idx_city_phones") as isize
from boo $f

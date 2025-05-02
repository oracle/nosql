select /*+ force_primary_index(foo) */
  id, index_storage_size($f, "idx_city_phones") as isize
from foo $f

select /*+ force_index(bar2 idx_map_keys_year_month) */ id
from bar2 b
where 
      exists b.map.values(substring($value, 0, 4) = "2021")

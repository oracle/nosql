select /*+ force_index(bar2 idx_map_upper_keys) */ id
from bar2 $b, $b.map.keys() $k
where upper($k) = "B" 


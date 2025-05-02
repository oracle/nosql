select $k, $t.values($key=$k) as v
from bar $t, $t.keys() $k

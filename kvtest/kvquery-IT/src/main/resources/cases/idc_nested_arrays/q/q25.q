select id
from nestedTable nt
where nt.maps.values($key > "key1").array[][$element > 5] =any 6

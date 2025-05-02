select id
from nestedTable nt
where nt.maps.values($key > "key1").array[][] =any 3

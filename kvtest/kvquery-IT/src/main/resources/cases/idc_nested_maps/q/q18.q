select id
from nestedTable $nt
where exists $nt.map1.values($key = "key1").map2.values($value =any 35)

select id
from nestedTable $nt
where exists $nt.map1.values(exists $value.map2.values($value =any 43))

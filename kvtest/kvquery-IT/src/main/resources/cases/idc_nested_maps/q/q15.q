select id
from nestedTable $nt
where exists$nt.map1.values().map2.values($value =any 56)

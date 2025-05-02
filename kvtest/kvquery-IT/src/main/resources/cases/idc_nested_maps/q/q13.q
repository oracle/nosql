select id 
from nestedTable $nt
where $nt.map1.values().map2.values() =any 35

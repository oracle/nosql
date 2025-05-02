select id
from nestedTable nt
where exists nt.maps.values().array[][$element in (5, 6)]

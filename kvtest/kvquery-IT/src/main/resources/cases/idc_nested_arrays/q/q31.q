select id
from nestedTable nt
where exists nt.maps.values().array[][3 < $element and $element < 9]

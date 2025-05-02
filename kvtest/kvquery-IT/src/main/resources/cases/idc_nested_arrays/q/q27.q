select id
from nestedTable nt
where exists nt.maps[$element.key1.array[][] =any 4]

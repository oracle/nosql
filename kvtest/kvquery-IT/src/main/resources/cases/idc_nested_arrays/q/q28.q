select id
from nestedTable nt
where exists nt.maps[$element.values().foo =any "sdf"]

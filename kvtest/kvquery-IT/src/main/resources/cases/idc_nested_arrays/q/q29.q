select id
from nestedTable nt
where exists nt.maps[$element.values().foo =any "hhh" and $element.values().bar =any 34]

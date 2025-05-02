select id
from nestedTable nt
where exists nt.addresses[$element.state = "OR" and $element.city < "R"]

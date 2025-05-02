select id
from nestedTable nt
where exists nt.addresses[$element.state = "CA" and exists $element.phones[$element.number =any 52 and $element.kind =any "home"]]

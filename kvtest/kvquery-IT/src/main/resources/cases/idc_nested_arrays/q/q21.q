select id
from nestedTable nt
where exists nt.addresses[exists $element.phones[$element.number =any 52 and $element.kind =any "home"]]

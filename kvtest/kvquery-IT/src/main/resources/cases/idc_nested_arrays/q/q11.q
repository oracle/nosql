select id
from nestedTable nt
where exists nt.addresses[exists $element.phones[$element.number =any 50]]

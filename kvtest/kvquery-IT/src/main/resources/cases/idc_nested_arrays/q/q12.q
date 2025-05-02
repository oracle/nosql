select /*+ FORCE_PRIMARY_INDEX(nestedTable) */ id
from nestedTable nt
where exists nt.addresses[exists $element.phones[$element.number =any 50]]

select id
from nestedTable nt
where nt.addresses.phones[][$element.kind = "work"].number >any 60

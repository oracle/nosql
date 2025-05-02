select id
from nestedTable nt
where nt.addresses.phones[][$element.number >any 60].kind =any "work"

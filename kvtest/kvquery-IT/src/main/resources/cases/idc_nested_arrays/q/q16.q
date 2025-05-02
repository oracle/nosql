select id
from nestedTable nt
where nt.addresses.phones[][$element.number in (31, 50, 70)].kind =any "home"

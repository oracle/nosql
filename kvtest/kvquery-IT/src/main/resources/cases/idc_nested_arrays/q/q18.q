select id
from nestedTable nt
where nt.age in (17, 30) and nt.addresses.phones[][$element.number in (31, 50, 60)].kind =any "home"

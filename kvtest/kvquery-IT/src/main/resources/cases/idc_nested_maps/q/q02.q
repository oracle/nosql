select id
from nestedTable $nt
where $nt.age = 30 and $nt.addresses.values().phones.values().values().areacode =any 520

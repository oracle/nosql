select id
from nestedTable $nt
where exists $nt.addresses.values().phones.values($value.values().areacode =any 408)

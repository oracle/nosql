select id
from nestedTable $nt
where $nt.addresses.values().phones.values().values().areacode =any 520

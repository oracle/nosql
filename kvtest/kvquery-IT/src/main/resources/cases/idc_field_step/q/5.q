# Test Description: Select value of existing key from a map using variable reference.

declare $mapkey string;
select id, $C.children.John.$mapkey
from Complex $C
order by id
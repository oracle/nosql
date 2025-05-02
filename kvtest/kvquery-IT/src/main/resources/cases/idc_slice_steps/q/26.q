# Test Description: Context item is map identifier.

select id, $C.map[0:1]
from Complex $C
order by id
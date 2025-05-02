# Test Description: Select value of existing key from a map.

select id, $C.children.Matt.friends
from Complex $C
order by id

# Test Description: Low expression < 0 (L is set to 0)

select id, $C.address.phones[-1 : 2]
from Complex $C
order by id

# Test Description: Low expression is missing

select id, $C.address.phones[:2]
from Complex $C
order by id
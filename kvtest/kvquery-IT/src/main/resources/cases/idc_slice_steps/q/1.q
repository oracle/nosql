# Test Description: Low expression is missing and H is size($).

select id, $C.address.phones[:size($)]
from Complex $C
order by id
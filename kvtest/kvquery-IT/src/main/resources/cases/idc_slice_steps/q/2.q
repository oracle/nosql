# Test Description: Low expression returns empty result (L is set to 0).

select id, $C.address.phones[$C.address.city.foo : 2]
from Complex $C
order by id

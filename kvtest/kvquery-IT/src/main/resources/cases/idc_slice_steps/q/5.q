# Test Description:: High expression returns empty result (H should be set to size of array -1)

select id, $C.address.phones[1:$C.address.city.foo]
from Complex $C
order by id

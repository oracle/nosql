# Test Description: High expression is missing.

select id, $C.address.phones[1:]
from Complex $C
order by id
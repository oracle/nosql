# Test Description: L > H.

select id, $C.address.phones[2:1]
from Complex $C
order by id
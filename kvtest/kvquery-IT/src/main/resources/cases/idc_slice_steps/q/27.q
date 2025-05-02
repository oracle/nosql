# Test Description: Low expression returns float.

select id, c.address.phones[flt:2]
from complex c
order by id
# Test Description: Low expression returns boolean.

select id, c.address.phones[bool:2]
from complex c
order by id
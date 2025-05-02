# Test Description: Low expression returns string.

select id, c.address.phones[firstName:2]
from complex c
order by id
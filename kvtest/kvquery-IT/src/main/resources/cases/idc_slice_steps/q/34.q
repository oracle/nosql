# Test Description: High expression returns boolean.

select id, c.address.phones[0:bool]
from complex c
order by id
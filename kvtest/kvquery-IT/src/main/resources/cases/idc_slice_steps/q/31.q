# Test Description: High expression returns float.

select id, c.address.phones[0:flt]
from complex c
order by id
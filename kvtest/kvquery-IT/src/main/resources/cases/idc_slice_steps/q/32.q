# Test Description: High expression returns double.

select id, c.address.phones[0:dbl]
from complex c
order by id
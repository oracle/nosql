# Test Description: Low expression returns double.

select id, c.address.phones[dbl:2]
from complex c
order by id
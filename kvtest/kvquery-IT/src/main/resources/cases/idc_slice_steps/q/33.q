# Test Description: High expression returns string.

select id, c.address.phones[0:lastname]
from complex c
order by id
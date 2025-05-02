# Test Description: High expression returns NULL.

select id, c.address.phones[0:$[2].work]
from Complex c
where id = 2
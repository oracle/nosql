# Test Description: Low expression returns NULL.

select id, c.address.phones[$[2].work:1]
from Complex c
where id = 2

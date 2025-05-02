# Test Description: L expression uses $ implicit variable.

select id, c.address.phones[$[0].work:2]
from complex c
order by id
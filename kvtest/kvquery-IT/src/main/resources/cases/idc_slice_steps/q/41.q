# Test Description: H expression uses $ implicit variable.

select id, c.address.phones[0 : $[0].home]
from complex c
order by id
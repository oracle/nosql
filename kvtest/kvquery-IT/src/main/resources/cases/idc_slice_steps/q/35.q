# Test Description: Context item is an atomic identifier in a record.

select id, c.address.city.foo[0:2]
from complex c
order by id
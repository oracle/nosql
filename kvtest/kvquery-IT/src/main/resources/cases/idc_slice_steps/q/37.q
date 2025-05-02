# Test Description: Context item is an atomic identifier in a array.

select id, c.address.phones.work.foo[0:2]
from complex c
order by id
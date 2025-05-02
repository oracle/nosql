# Test Description: Context item is an atomic identifier in a array.

select id, u.address.phones.work.foo
from complex u
order by id
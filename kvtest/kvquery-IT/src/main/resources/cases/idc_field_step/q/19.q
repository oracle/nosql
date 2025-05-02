# Test Description: Context item is an atomic var_ref in a array.

DECLARE $work string;
select id, u.address.phones.$work.foo
from complex u
order by id

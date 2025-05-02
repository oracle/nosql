# Test Description: Context item is an atomic var_ref in a record.

DECLARE $city string;
select id, u.address.$city.foo
from complex u
order by id

# Test Description: Context item is an atomic var_ref in a map.

DECLARE $mapkey string;
select id, u.children.Anna.$mapkey.foo
from complex u
order by id
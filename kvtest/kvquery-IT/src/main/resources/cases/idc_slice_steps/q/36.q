# Test Description: Context item is an atomic identifier in a map.

select id, c.children.foo[0:2]
from complex c
order by id
# Test Description: Context item is an atomic string in a map.

select id, u.children.Anna."age".foo
from complex u
order by id
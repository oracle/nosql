# Test Description: Context item is record identifier.

select id, $C.address[0:1]
from Complex $C
order by id
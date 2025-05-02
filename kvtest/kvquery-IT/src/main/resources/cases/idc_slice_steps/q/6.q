# Test Description: High expression > size of array -1 (H is set to size of array -1).

select id, $C.address.phones[1:size($)+1]
from Complex $C
order by id

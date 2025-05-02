# Array filter step
# Test Description: No predicate expression (all elements should be returned).

select id, $C.address.phones[]
from Complex $C
order by id

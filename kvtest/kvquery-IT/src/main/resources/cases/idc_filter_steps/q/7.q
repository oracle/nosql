# Array filter step
# Test Description: Predicate expression returns negative numeric item.

select id, $C.address.phones[$element.home - 500]
from Complex $C
order by id


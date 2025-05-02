# Array filter step
# Test Description: Predicate expression returns negative numeric item.

select id, $C.address.phones[$element.work + $element.home - 1000]
from Complex $C
order by id
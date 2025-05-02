# Array filter step
# Test Description: Predicate expression returns numeric item >= array size.

select id, $C.address.phones[$element.home + $element.work]
from Complex $C
order by id
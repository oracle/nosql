# Array filter step
# Test Description: Predicate expression returns numeric item >= 0 and < array size (use implicit variables $element and $pos).

select id, $C.address.phones[$element.home - 30]
from Complex $C
order by id

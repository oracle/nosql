# Array filter step
# Test Description: Predicate expression returns boolean item (use implicit variables $element and $pos)

select id, c.address.phones[$element.work > 510 and $pos < 3]
from complex c
order by id
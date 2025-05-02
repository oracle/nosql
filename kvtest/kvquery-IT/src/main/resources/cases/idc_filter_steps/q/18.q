# Array filter step
# Test Description: Context item is an atomic string in a array - with false predicate expression.

select id, c.address.phones."work"[false]
from complex c
order by id
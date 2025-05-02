# Array filter step
# Test Description: Context item is an atomic identifier in a array - with true predicate expression

select id, c.address.phones.work[true]
from complex c
order by id
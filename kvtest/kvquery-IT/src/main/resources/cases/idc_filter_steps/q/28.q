# Map filter step
# Test Description: Context item is an atomic identifier in a record - with true predicate expression

select id, C.address.city.foo.keys(true)
from Complex C
order by id
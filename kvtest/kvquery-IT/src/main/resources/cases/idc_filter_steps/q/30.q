# Map filter step
# Test Description: Context item is an atomic identifier in a record - with no predicate expression

select id, C.address.city.foo.keys()
from Complex C
order by id
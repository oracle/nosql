# Map filter step
# Test Description: Context item is an atomic var_ref in a record - with no predicate expression.

declare $city string;
select id, C.address.$city.foo.keys()
from Complex C
order by id
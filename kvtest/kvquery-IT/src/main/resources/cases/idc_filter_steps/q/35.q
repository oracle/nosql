# Map filter step
# Test Description: Context item is an atomic var_ref in a record - with false predicate expression.

declare $city string;
select id, C.address.$city.foo.keys(false)
from Complex C
order by id
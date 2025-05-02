# Map filter step
# Test Description: Context item is an atomic string in a record - with false predicate expression.

select id, C.address."city".foo.keys(false)
from Complex C
order by id
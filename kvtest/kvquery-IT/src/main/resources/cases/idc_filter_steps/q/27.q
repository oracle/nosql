# Map filter step
# Test Description: Context item is an atomic string in a map - with true predicate expression.

select id, C.children.John."age".foo.keys(true)
from Complex C
order by id
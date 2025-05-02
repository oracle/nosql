# Map filter step
# Test Description: Context item is an atomic identifier in a map - with false predicate expression

select id, C.children.John.age.foo.keys(false)
from Complex C
order by id
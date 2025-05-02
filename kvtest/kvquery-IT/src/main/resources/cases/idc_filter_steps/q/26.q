# Map filter step
# Test Description: Context item is an atomic identifier in a map - with no predicate expression

select id, C.children.John.age.foo.keys()
from Complex C
order by id
# Map filter step
# Test Description: No predicate expression (all elements should be returned)

select id, C.children.keys()
from Complex C
order by id
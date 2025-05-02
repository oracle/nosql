# Map filter step
# Test Description: Predicate expression returns empty sequence.

select id, C.children.values($key = "test") 
from Complex C
order by id
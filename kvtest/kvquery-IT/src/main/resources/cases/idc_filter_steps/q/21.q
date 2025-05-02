# Map filter step
# Test Description: Predicate expression returns boolean item (use implicit variables $key).

select id, C.children.values($key = "John")
from Complex C
order by id
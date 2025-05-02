# Map filter step
# Test Description: Predicate expression returns NULL

select id, C.children.values(bool)
from Complex C
where id = 2

# Array filter step
# Test Description: Predicate expression returns NULL.

select id, C.children.John.friends[C.bool]
from Complex C 
where id = 2

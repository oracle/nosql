# Array filter step
# Test Description: Predicate expression returns empty sequence.

select id, C.arrmap[$element.key3 = "test"] 
from Complex C
order by id
# Array filter step
# Test Description: Predicate expression returns empty sequence.

select id, C.arrmap[$element.test] 
from Complex C
order by id
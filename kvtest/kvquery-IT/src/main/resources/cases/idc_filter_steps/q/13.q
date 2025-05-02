# Array filter step
# Test Description: Predicate expression returns NULL.

select C.arrint[C.bool IS NULL]
from Complex C
where id = 2
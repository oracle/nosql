# Array filter step
# Test Description: Context item is integer.

select id, C.age[$element > 1]
from Complex C
order by id
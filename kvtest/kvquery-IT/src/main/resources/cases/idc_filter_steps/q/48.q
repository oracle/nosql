# Array filter step
# Test Description: Context item is long.

select id, C.lng[$element > 2]
from Complex C
order by id
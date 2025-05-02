# Array filter step
# Test Description: Context item is boolean.

select id, C.bool[$element = true]
from Complex C
order by id
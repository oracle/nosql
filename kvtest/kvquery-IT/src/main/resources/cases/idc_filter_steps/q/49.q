# Array filter step
# Test Description: Context item is float.

select id, C.flt[$element > 0]
from Complex C
order by id
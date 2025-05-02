# Array filter step
# Test Description: Context item is double.

select id, C.dbl[$element > 3]
from Complex C
order by id
# Map filter step
# Test Description: Context item is double.

select id, C.dbl.values()
from Complex C
order by id